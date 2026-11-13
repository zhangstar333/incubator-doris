// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

class DorisKeytabRenewalTest {
    enum FailureMode {
        BEFORE_LOGOUT,
        AFTER_LOGOUT,
        EXPIRED_TGT,
        MISSING_TGT
    }

    @BeforeEach
    void setUp() {
        UserGroupInformation.reset();
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.authentication", "simple");
        conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 0);
        UserGroupInformation.setConfiguration(conf);
    }

    @AfterEach
    void tearDown() {
        UserGroupInformation.reset();
    }

    @ParameterizedTest
    @EnumSource(FailureMode.class)
    void keytabRenewalRecoversOnTheOriginalUgi(FailureMode failureMode) throws Exception {
        KerberosTicket originalTicket = ticket(failureMode == FailureMode.EXPIRED_TGT);
        Subject subject = subject(originalTicket);
        KerberosTicket replacementTicket = ticket(false);
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<UserGroupInformation.AutoRenewalForUserCredsRunnable> renewalTask = new AtomicReference<>();
        UserGroupInformation ugi = new UserGroupInformation(subject) {
            @Override
            public void reloginFromKeytab() throws IOException {
                if (attempts.incrementAndGet() == 1) {
                    if (failureMode == FailureMode.AFTER_LOGOUT || failureMode == FailureMode.MISSING_TGT) {
                        destroyTicket(subject, originalTicket);
                    }
                    if (failureMode == FailureMode.MISSING_TGT) {
                        // A racing failed login can leave no TGT, then throttle this attempt.
                        return;
                    }
                    throw new IOException("Injected transient keytab login failure");
                }
                subject.getPrivateCredentials().remove(originalTicket);
                subject.getPrivateCredentials().add(replacementTicket);
                renewalTask.get().setRunRenewalLoop(false);
            }
        };
        // libhdfs may replace the JVM login user when another filesystem connects.
        // The old DFSClient's UGI must still renew its own Subject independently.
        UserGroupInformation loginUser = UserGroupInformation.createRemoteUser("other");
        UserGroupInformation.setLoginUser(loginUser);
        UserGroupInformation.AutoRenewalForUserCredsRunnable task = ugi.new KeytabRenewalRunnable(originalTicket, 0);
        renewalTask.set(task);
        runRenewal(task);

        Assertions.assertEquals(2, attempts.get());
        Assertions.assertSame(subject, ugi.getSubject());
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        Assertions.assertEquals(1, tickets.size());
        Assertions.assertSame(replacementTicket, tickets.iterator().next());
        Assertions.assertSame(loginUser, UserGroupInformation.getLoginUser());
        Assertions.assertNotEquals(ugi, loginUser);
    }

    @Test
    void ticketCacheRenewalStillStopsAfterLogoutDestroysTheTgt() {
        KerberosTicket originalTicket = ticket(false);
        Subject subject = subject(originalTicket);
        UserGroupInformation ugi = new UserGroupInformation(subject);
        Assertions.assertSame(subject, ugi.getSubject());
        AtomicInteger attempts = new AtomicInteger();
        UserGroupInformation.AutoRenewalForUserCredsRunnable task =
                ugi.new AutoRenewalForUserCredsRunnable(originalTicket, 0) {
                    @Override
                    protected void relogin() throws IOException {
                        attempts.incrementAndGet();
                        destroyTicket(subject, originalTicket);
                        throw new IOException("Injected ticket-cache renewal failure");
                    }
                };
        task.run();
        Assertions.assertEquals(1, attempts.get());
    }

    @Test
    void interruptedKeytabRetryTerminates() throws Exception {
        Configuration conf = new Configuration(false);
        conf.setLong("hadoop.kerberos.min.seconds.before.relogin", 60);
        UserGroupInformation.setConfiguration(conf);
        KerberosTicket originalTicket = ticket(false);
        Subject subject = subject(originalTicket);
        CountDownLatch failed = new CountDownLatch(1);
        UserGroupInformation ugi = new UserGroupInformation(subject) {
            @Override
            public void reloginFromKeytab() throws IOException {
                destroyTicket(subject, originalTicket);
                failed.countDown();
                throw new IOException("Injected unavailable KDC");
            }
        };
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Assertions.assertSame(subject, ugi.getSubject());
            Future<?> renewal = executor.submit(ugi.new KeytabRenewalRunnable(originalTicket, 0));
            Assertions.assertTrue(failed.await(10, TimeUnit.SECONDS));
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            Assertions.assertDoesNotThrow(() -> renewal.get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void runRenewal(UserGroupInformation.AutoRenewalForUserCredsRunnable task) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> renewal = executor.submit(task);
            renewal.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static Subject subject(KerberosTicket ticket) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new User("probe", UserGroupInformation.AuthenticationMethod.KERBEROS, null));
        subject.getPrivateCredentials().add(ticket);
        return subject;
    }

    private static KerberosTicket ticket(boolean expired) {
        long now = System.currentTimeMillis();
        KerberosPrincipal client = new KerberosPrincipal("probe@EXAMPLE.INVALID");
        KerberosPrincipal server = new KerberosPrincipal("krbtgt/EXAMPLE.INVALID@EXAMPLE.INVALID");
        return new KerberosTicket(new byte[] {1}, client, server, new byte[16], 17, new boolean[32],
                new Date(now - 7200000), new Date(now - 7200000),
                new Date(expired ? now - 60000 : now + 3600000), null, null);
    }

    private static void destroyTicket(Subject subject, KerberosTicket ticket) throws IOException {
        subject.getPrivateCredentials().remove(ticket);
        try {
            ticket.destroy();
        } catch (javax.security.auth.DestroyFailedException e) {
            throw new IOException(e);
        }
    }
}
