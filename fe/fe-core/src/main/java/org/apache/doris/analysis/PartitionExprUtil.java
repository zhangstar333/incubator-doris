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

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
            List<TPartitionByRange> partitionValues, PartitionType partitionType) throws AnalysisException {
        Map<String, AddPartitionClause> result = Maps.newHashMap();
        for (TPartitionByRange partitionValue : partitionValues) {
            String beginTime = partitionValue.start_key.date_literal.value;
            String endTime = partitionValue.end_key.date_literal.value;

            // maybe need check the range in FE also, like getAddPartitionClause.
            PartitionValue lowerValue = new PartitionValue(beginTime);
            PartitionValue upperValue = new PartitionValue(endTime);
            PartitionKeyDesc partitionKeyDesc = null;
            String partitionName = "p";
            if (partitionType == PartitionType.RANGE) {
             partitionKeyDesc = PartitionKeyDesc.createFixed(
                    Collections.singletonList(lowerValue),
                    Collections.singletonList(upperValue));
                LocalDateTime dateTime = parseStringWithDefaultHSM(beginTime,
                        DATETIME_FORMATTER);
                String lowerBound = dateTime.format(DATETIME_NAME_FORMATTER);
                partitionName += lowerBound;
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
