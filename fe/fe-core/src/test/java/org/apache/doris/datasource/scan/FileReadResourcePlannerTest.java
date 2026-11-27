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


package org.apache.doris.datasource.scan;

import org.apache.doris.thrift.TFileReadResource;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceBlobResource;
import org.apache.doris.thrift.TPaimonFileReadResource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

class FileReadResourcePlannerTest {
    @Test
    void assignsOneDeterministicNamespaceToMixedProvidersAndScans() {
        TFileReadResource lance = new TFileReadResource().setLance(new TLanceBlobResource()
                .setDatasetUri("s3://bucket/a.lance").setVersion(7).setColumnName("image"));
        TFileReadResource record = new TFileReadResource().setPaimon(
                new TPaimonFileReadResource().setValidateRecord(true));
        TFileReadResource range = new TFileReadResource().setPaimon(
                new TPaimonFileReadResource().setValidateRecord(false));
        TFileScanRangeParams first = new TFileScanRangeParams();
        first.putToFileReadResources("image", lance);
        TFileScanRangeParams second = new TFileScanRangeParams();
        second.putToFileReadResources("paimon-record", record);
        second.putToFileReadResources("paimon-range", range);
        Map<Integer, TFileReadResource> resources = FileReadResourcePlanner.planParams(
                Arrays.asList(first, new TFileScanRangeParams(), second), 21);
        Assertions.assertEquals(3, resources.size());
        Assertions.assertEquals(1, lance.getSourceId());
        Assertions.assertEquals(2, range.getSourceId());
        Assertions.assertEquals(3, record.getSourceId());
        Assertions.assertEquals(7, resources.get(1).getLance().getVersion());
        Assertions.assertTrue(resources.get(3).getPaimon().isValidateRecord());
        Assertions.assertEquals(resources, FileReadResourcePlanner.planParams(Arrays.asList(first, second), 21));
    }

    @Test
    void gatesBoundResourcesButLeavesOrdinaryScansOnOldVersions() {
        TFileScanRangeParams scan = new TFileScanRangeParams();
        scan.putToFileReadResources("image", new TFileReadResource());
        Assertions.assertThrows(IllegalStateException.class,
                () -> FileReadResourcePlanner.planParams(Collections.singletonList(scan), 20));
        Assertions.assertTrue(FileReadResourcePlanner.planParams(
                Collections.singletonList(new TFileScanRangeParams()), 19).isEmpty());
    }
}
