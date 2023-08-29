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

package org.apache.doris.analysis;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TPartitionByRange;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PartitionExprUtil {
    private static final Logger LOG = LogManager.getLogger(PartitionExprUtil.class);
    public static final Set<String> SUPPORTED_PARTITION_FORMAT = ImmutableSet.of("hour", "day", "month", "year");
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DATETIME_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final PartitionExprUtil partitionExprUtil = new PartitionExprUtil();

    public class FunctionIntervalInfo {
        public String timeUnit;
        public int interval;
        public FunctionIntervalInfo(String timeUnit, int interval) {
            this.timeUnit = timeUnit;
            this.interval = interval;
        }
    }

    public static FunctionIntervalInfo getFunctionIntervalInfo(ArrayList<Expr> partitionExprs, PartitionType partitionType) throws AnalysisException {
        if (partitionType != PartitionType.RANGE) {
            return null;
        }
        if (partitionExprs.size() != 1) {
            throw new AnalysisException("now only support one expr in range partition");
        }

        Expr e = partitionExprs.get(0);
        if (!(e instanceof FunctionCallExpr)) {
            throw new AnalysisException("now range partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) e;
        String fnName = functionCallExpr.getFnName().getFunction();
        String timeUnit;
        int interval;
        if ("date_trunc".equalsIgnoreCase(fnName)) {
            List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
            if (paramsExprs.size() != 2) {
                throw new AnalysisException("date_trunc params exprs size should be 2.");
            }
            Expr param = paramsExprs.get(1);
            if (!(param instanceof StringLiteral)) {
                throw new AnalysisException("date_trunc param of time unit is not string literal.");
            }
            timeUnit = ((StringLiteral) param).getStringValue().toLowerCase();
            interval = 1;
        } else {
            throw new AnalysisException("now range partition only support date_trunc.");
        }
        return partitionExprUtil.new FunctionIntervalInfo(timeUnit, interval);
    }

    public static LocalDateTime getRangeEnd(LocalDateTime beginTime, FunctionIntervalInfo intervalInfo) {
        String timeUnit = intervalInfo.timeUnit;
        int interval = intervalInfo.interval;
        switch (timeUnit) {
            case "year": 
                return beginTime.plusYears(interval);
            case "month":
                return beginTime.plusMonths(interval);
            case "day":
                return beginTime.plusDays(interval);
            case "hour":
                return beginTime.plusHours(interval);
            case "minute":
                return beginTime.plusMinutes(interval);
            case "second":
                return beginTime.plusSeconds(interval);
            default:
                break;
        }
        return null;
    }

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
            List<TPartitionByRange> partitionValues, ArrayList<Expr> partitionExprs, PartitionType partitionType) throws AnalysisException {
        Map<String, AddPartitionClause> result = Maps.newHashMap();
        FunctionIntervalInfo intervalInfo = getFunctionIntervalInfo(partitionExprs, partitionType);

        for (TPartitionByRange partitionValue : partitionValues) {
            String beginTime = partitionValue.start_key.date_literal.value;
            // maybe need check the range in FE also, like getAddPartitionClause.
            PartitionValue lowerValue = new PartitionValue(beginTime);
            PartitionKeyDesc partitionKeyDesc = null;
            String partitionName = "p";

            if (partitionType == PartitionType.RANGE) {
                LocalDateTime beginDateTime = parseStringWithDefaultHSM(beginTime,
                        DATETIME_FORMATTER);
                String lowerBound = beginDateTime.format(DATETIME_NAME_FORMATTER);
                partitionName += lowerBound;

             LocalDateTime endDateTime = getRangeEnd(beginDateTime, intervalInfo);
             String endTime = endDateTime.format(DATETIME_FORMATTER);

             PartitionValue upperValue = new PartitionValue(endTime);
             partitionKeyDesc = PartitionKeyDesc.createFixed(
                    Collections.singletonList(lowerValue),
                    Collections.singletonList(upperValue));

            } else if (partitionType == PartitionType.LIST) {
                List<List<PartitionValue>> listValues = new ArrayList<>();
                listValues.add(Collections.singletonList(lowerValue));
                partitionKeyDesc = PartitionKeyDesc.createIn(
                    listValues);
                partitionName += lowerValue.getStringValue();
            } else {
                throw new AnalysisException("now only support range and list partition");
            }

            Map<String, String> partitionProperties = Maps.newHashMap();
            // here need check
            Short replicationNum = olapTable.getTableProperty().getReplicaAllocation()
                    .getTotalReplicaNum();
            partitionProperties.put("replication_num", String.valueOf(replicationNum));
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();

            SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);

            AddPartitionClause addPartitionClause = new AddPartitionClause(singleRangePartitionDesc,
                    distributionDesc, partitionProperties, false);
            result.put(partitionName, addPartitionClause);
        }
        return result;
    }

    public static LocalDateTime parseStringWithDefaultHSM(String datetime,
            DateTimeFormatter formatter) {
        TemporalAccessor temporal = formatter.parse(datetime);
        if (temporal.isSupported(ChronoField.HOUR_OF_DAY) &&
                temporal.isSupported(ChronoField.SECOND_OF_MINUTE) &&
                temporal.isSupported(ChronoField.MINUTE_OF_HOUR)) {
            return LocalDateTime.from(temporal);
        } else {
            return LocalDateTime.of(LocalDate.from(temporal), LocalTime.of(0, 0, 0));
        }
    }
}
