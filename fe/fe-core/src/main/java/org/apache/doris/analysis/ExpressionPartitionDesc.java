// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.analysis.PartitionKeyDesc.PartitionKeyValueType;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ExpressionRangePartitionInfo;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.thrift.TPartitionByRange;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

public class ExpressionPartitionDesc extends PartitionDesc {

    private static final Logger LOG = LogManager.getLogger(ExpressionPartitionDesc.class);
    private Expr expr;
    // private RangePartitionDesc rangePartitionDesc = null;
    public static final Set<String> SUPPORTED_PARTITION_FORMAT = ImmutableSet.of("hour", "day", "month", "year");
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter DATETIME_NAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    
    public ExpressionPartitionDesc(Expr expr, List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        this.expr = expr;
        try {
            this.partitionColNames = new ArrayList<>();
            checkFunctionExpr(expr, partitionColNames);
            RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(partitionColNames,
                    allPartitionDescs);
            this.singlePartitionDescs = rangePartitionDesc.getSinglePartitionDescs();
        } catch (AnalysisException e) {
            throw new AnalysisException("ExpressionPartitionDesc have meet some error " +
                    e.toString());
        }
        this.type = PartitionType.EXPR_RANGE;
    }

    public void checkFunctionExpr(Expr expr, List<String> partitionColNames)
            throws AnalysisException {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            FunctionName fnName = functionCallExpr.getFnName();
            List<Expr> paramsExpr = functionCallExpr.getParams().exprs();
            if (fnName.getFunction().equalsIgnoreCase("date_trunc")) {
                if (paramsExpr.size() != 2) {
                    throw new AnalysisException("The date_trunc function should have 2 parameters");
                }
                Expr slotRef = paramsExpr.get(0);
                Expr argUnit = paramsExpr.get(1);
                if (slotRef instanceof SlotRef) {
                    partitionColNames.add(((SlotRef) slotRef).getColumnName());
                } else {
                    throw new AnalysisException("The firstExpr parameter of date_trunc only supports column in table");
                }

                if (argUnit instanceof StringLiteral) {
                    StringLiteral stringLiteral = (StringLiteral) argUnit;
                    String unit = stringLiteral.getValue();
                    if (!SUPPORTED_PARTITION_FORMAT.contains(unit.toLowerCase())) {
                        throw new AnalysisException("Unsupported date_trunc format " + unit);
                    }
                } else {
                    throw new AnalysisException("Unsupported date_trunc params " +
                            argUnit.toSql());
                }
            } else if (fnName.getFunction().equalsIgnoreCase("date_floor") ||
                    fnName.getFunction()
                            .equalsIgnoreCase("date_ceil")) {
                Expr slotRef = paramsExpr.get(0);
                Expr argUnit = paramsExpr.get(1);
                if (slotRef instanceof SlotRef) {
                    partitionColNames.add(((SlotRef) slotRef).getColumnName());
                } else {
                    throw new AnalysisException(
                            "The first parameter of date_floor/date_ceil only supports column in table");
                }
                if (argUnit instanceof IntLiteral) {
                    // some code check
                } else {
                    throw new AnalysisException(
                            "ExpressionPartitionDesc the partition by expr is function call expr " +
                                    expr.toSql());
                }
            }
        } else {
            throw new AnalysisException(
                    "ExpressionPartitionDesc only support function call expr " +
                            expr.toSql());
        }
    }

    public static SlotRef getSlotRefFromFunctionCallExpr(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef) {
                    return (SlotRef) child;
                }
            }
        }
        return null;
    }

    public static void analyzePartitionExpr(Expr expr, Type targetColType) throws AnalysisException {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
            Function builtinFunction = null;
            FunctionName fnName = functionCallExpr.getFnName();
            if (fnName.getFunction().equalsIgnoreCase("date_trunc")) {
                Type[] argTypes = { targetColType, Type.VARCHAR };
                builtinFunction = functionCallExpr.getBuiltinFunction(fnName.getFunction(),
                        argTypes, Function.CompareMode.IS_IDENTICAL);
            } else if (fnName.getFunction().equalsIgnoreCase("date_floor") ||
                    fnName.getFunction()
                            .equalsIgnoreCase("date_ceil")) {
                Type[] argTypes = { targetColType, Type.INT };
                builtinFunction = functionCallExpr.getBuiltinFunction(fnName.getFunction(),
                        argTypes, Function.CompareMode.IS_IDENTICAL);
            }
            if (builtinFunction == null) {
                throw new AnalysisException(
                        "Function: " + fnName.getFunction() + " maybe not support in partition by expr with type "
                                + targetColType.toString() + " should check it.");
            }

            functionCallExpr.setFn(builtinFunction);
        }
    }

    @Override
    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        super.analyze(columnDefs, otherProperties);
        boolean hasExprAnalyze = false;
        // if (rangePartitionDesc != null) {
        //     rangePartitionDesc.analyze(columnDefs, otherProperties);
        // }
        SlotRef slotRef = getSlotRefFromFunctionCallExpr(expr);

        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                analyzePartitionExpr(this.expr, columnDef.getType());
                slotRef.setType(columnDef.getType());
                hasExprAnalyze = true;
            }
        }
        if (!hasExprAnalyze) {
            throw new AnalysisException("Partition expr without analyzed.");
        }
    }

    @Override
    public void checkPartitionKeyValueType(PartitionKeyDesc partitionKeyDesc)
            throws AnalysisException {
        if (partitionKeyDesc.getPartitionType() != PartitionKeyValueType.FIXED
                && partitionKeyDesc.getPartitionType() != PartitionKeyValueType.LESS_THAN) {
            throw new AnalysisException("You can only use fixed or less than values to create range partitions");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (String column : partitionColNames) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`");
            idx++;
        }
        sb.append(")\n(\n");

        for (int i = 0; i < singlePartitionDescs.size(); i++) {
            if (i != 0) {
                sb.append(",\n");
            }
            sb.append(singlePartitionDescs.get(i).toSql());
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        // if (rangePartitionDesc == null) {
        //     return new ExpressionRangePartitionInfo(Collections.singletonList(expr),
        //             schema, PartitionType.RANGE);
        // }
        PartitionType partitionType = PartitionType.EXPR_RANGE;

        List<Column> partitionColumns = new ArrayList<>();
        for (String colName : getPartitionColNames()) {
            findRangePartitionColumn(schema, partitionColumns, colName);
        }

        ExpressionRangePartitionInfo expressionRangePartitionInfo = new ExpressionRangePartitionInfo(
                Collections.singletonList(expr), partitionColumns, partitionType);

        for (SinglePartitionDesc desc : singlePartitionDescs) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            expressionRangePartitionInfo.handleNewSinglePartitionDesc(desc, partitionId,
                    isTemp);
        }
        return expressionRangePartitionInfo;

    }

    static void findRangePartitionColumn(List<Column> schema, List<Column> partitionColumns, String colName)
            throws DdlException {
        boolean find = false;
        for (Column column : schema) {
            if (column.getName().equalsIgnoreCase(colName)) {
                if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                    throw new DdlException("The partition column could not be aggregated column"
                            + " and unique table's partition column must be key column");
                }

                if (column.getType().isFloatingPointType() ||
                        column.getType().isComplexType()) {
                    throw new DdlException(String.format("Invalid partition column '%s': %s",
                            column.getName(), "invalid data type " + column.getType()));
                }

                try {
                    RangePartitionInfo.checkPartitionColumn(column);
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }

                partitionColumns.add(column);
                find = true;
                break;
            }
        }
        if (!find) {
            throw new DdlException("Partition column[" + colName + "] does not found");
        }
    }

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
            List<TPartitionByRange> partitionValues) throws AnalysisException {
        Map<String, AddPartitionClause> result = Maps.newHashMap();
        for (TPartitionByRange partitionValue : partitionValues) {
            String beginTime = partitionValue.start_key.date_literal.value;
            String endTime = partitionValue.end_key.date_literal.value;

            // maybe need check the range in FE also, like getAddPartitionClause.
            PartitionValue lowerValue = new PartitionValue(beginTime);
            PartitionValue upperValue = new PartitionValue(endTime);
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(
                    Collections.singletonList(lowerValue),
                    Collections.singletonList(upperValue));

            LocalDateTime dateTime = ExpressionPartitionDesc.parseStringWithDefaultHSM(beginTime,
                                DATETIME_FORMATTER);
            String lowerBound = dateTime.format(DATETIME_NAME_FORMATTER);
            String partitionName = "p" + lowerBound;

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
