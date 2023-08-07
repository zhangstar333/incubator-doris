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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExpressionPartitionDesc;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.gson.annotations.SerializedName;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import com.google.gson.reflect.TypeToken;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExpressionRangePartitionInfo extends RangePartitionInfo {
    private static final Logger LOG = LogManager.getLogger(ExpressionRangePartitionInfo.class);
    public static final String AUTOMATIC_SHADOW_PARTITION_NAME = "$shadow_automatic_partition";
    public static final String SHADOW_PARTITION_PREFIX = "$";
    @SerializedName(value = "partitionExprs")
    private List<Expr> partitionExprs;

    public ExpressionRangePartitionInfo() {
        this.type = PartitionType.EXPR_RANGE;
    }

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs, List<Column> columns, PartitionType type) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 1;
        this.type = type;
    }

    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public void setPartitionExprs(List<Expr> partitionExprs) {
        this.partitionExprs = partitionExprs;
    }

    public static PartitionInfo read(DataInput in) throws IOException {
        ExpressionRangePartitionInfo partitionInfo = new ExpressionRangePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        int size = partitionExprs.size();
        out.writeInt(size);
        for (int i = 0; i < size; ++i) {
            Expr e = partitionExprs.get(i);
            Expr.writeTo(e, out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        LOG.info("readFields(DataInput in) 1111111");
        partitionExprs = new ArrayList<>();
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            Expr e = Expr.readIn(in);
            if (e instanceof FunctionCallExpr) {
                SlotRef slotRef = ExpressionPartitionDesc.getSlotRefFromFunctionCallExpr(e);
                for (Column partitionColumn : partitionColumns) {
                    if (slotRef.getColumnName().equalsIgnoreCase(partitionColumn.getName())) {
                        try {
                            ExpressionPartitionDesc.analyzePartitionExpr(e, partitionColumn.getType());
                        } catch (AnalysisException err) {
                            throw new IOException("ExpressionRangePartitionInfo read meet some error: " +
                                    err.toString());
                        }
                        slotRef.setType(partitionColumn.getType());
                    }
                }
            }
            partitionExprs.add(e);
        }
    }

    @Override
    public String toSql(OlapTable table, List<Long> partitionId) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (Column column : partitionColumns) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column.getName()).append("`");
            idx++;
        }
        sb.append(")\n(");

        // sort range
        List<Map.Entry<Long, PartitionItem>> entries = new ArrayList<>(this.idToItem.entrySet());
        Collections.sort(entries, RangeUtils.RANGE_MAP_ENTRY_COMPARATOR);

        idx = 0;
        for (Map.Entry<Long, PartitionItem> entry : entries) {
            Partition partition = table.getPartition(entry.getKey());
            String partitionName = partition.getName();
            Range<PartitionKey> range = entry.getValue().getItems();

            // print all partitions' range is fixed range, even if some of them is
            // created
            // by less than range
            sb.append("PARTITION ").append(partitionName).append(" VALUES [");
            sb.append(range.lowerEndpoint().toSql());
            sb.append(", ").append(range.upperEndpoint().toSql()).append(")");

            Optional.ofNullable(this.idToStoragePolicy.get(entry.getKey())).ifPresent(p -> {
                if (!p.equals("")) {
                    sb.append("PROPERTIES (\"STORAGE POLICY\" = \"");
                    sb.append(p).append("\")");
                }
            });

            if (partitionId != null) {
                partitionId.add(entry.getKey());
                break;
            }

            if (idx != entries.size() - 1) {
                sb.append(",\n");
            }
            idx++;
        }
        sb.append(")");
        return sb.toString();
    }

}
