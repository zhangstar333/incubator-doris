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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.GenericVariantBuilder;
import org.apache.paimon.data.variant.GenericVariantUtil;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Query-level plan for Paimon Variant access-path projection.
 *
 * <p>Paimon returns one Variant value for every requested path. Doris expressions consume one
 * Variant slot, so the extracted values are merged into a partial object. The path tree is built
 * once, while {@link Materializer} uses one builder for all rows in a Doris output batch. This
 * avoids constructing a final Variant and exact-sized value and metadata arrays for every row.
 */
final class PaimonVariantProjection {
    private static final String FIELD_NAME_PREFIX = "__doris_variant_field_";
    private static final int VARIANT_VALUE_INDEX = 0;
    private static final int VARIANT_METADATA_INDEX = 1;
    private static final int VARIANT_FIELD_COUNT = 2;

    private final RowType readType;
    private final PathNode root;
    private final int objectCount;
    private final long projectionValueOverhead;

    private PaimonVariantProjection(
            RowType readType, PathNode root, int objectCount, long projectionValueOverhead) {
        this.readType = readType;
        this.root = root;
        this.objectCount = objectCount;
        this.projectionValueOverhead = projectionValueOverhead;
    }

    /**
     * Creates the metadata-marked RowType understood by Paimon's Variant reader.
     *
     * <p>Returning null means that the complete Variant column must be read instead. This
     * all-or-nothing fallback is important because Doris still evaluates every original
     * element_at expression after the scan.
     */
    static PaimonVariantProjection create(List<List<String>> paths, String timeZone) {
        if (paths == null || paths.isEmpty()) {
            return null;
        }

        List<DataField> fields = new ArrayList<>(paths.size());
        PathNode root = new PathNode();
        for (int fieldIndex = 0; fieldIndex < paths.size(); fieldIndex++) {
            List<String> path = paths.get(fieldIndex);
            if (!supportsObjectPath(path) || !root.add(path, fieldIndex)) {
                // Doris access paths currently do not retain whether a numeric segment came from
                // an array index or an object key. Falling back avoids changing either meaning.
                return null;
            }
            fields.add(new DataField(
                    fieldIndex,
                    FIELD_NAME_PREFIX + fieldIndex,
                    DataTypes.VARIANT(),
                    VariantMetadataUtils.buildVariantMetadata(toPaimonPath(path), false, timeZone)));
        }
        int objectCount = root.assignObjectIndexes(0);
        return new PaimonVariantProjection(
                new RowType(fields), root, objectCount, root.valueOverheadUpperBound());
    }

    RowType readType() {
        return readType;
    }

    Materializer newMaterializer(VectorColumn outputColumn) {
        return new Materializer(outputColumn);
    }

    private static boolean supportsObjectPath(List<String> path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        for (String segment : path) {
            if (segment == null || segment.isEmpty() || segment.indexOf('.') >= 0
                    || segment.indexOf('[') >= 0 || segment.indexOf(';') >= 0
                    || isIntegerSegment(segment)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isIntegerSegment(String segment) {
        int offset = segment.startsWith("-") ? 1 : 0;
        if (offset == segment.length()) {
            return false;
        }
        for (int i = offset; i < segment.length(); i++) {
            if (!Character.isDigit(segment.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static String toPaimonPath(List<String> path) {
        return "$." + String.join(".", path);
    }

    /** Batch-scoped state for merging projected paths into the encoded JNI Variant column. */
    final class Materializer {
        private final VectorColumn outputColumn;
        private final List<ArrayList<GenericVariantBuilder.FieldEntry>> fieldEntries;
        private final GenericVariant[] extractedValues =
                new GenericVariant[readType.getFieldCount()];
        private GenericVariantBuilder builder;
        private int[] valueOffsets = new int[129];
        private boolean[] nullRows = new boolean[128];
        private int rowCount;
        private long metadataInputBytes;

        private Materializer(VectorColumn outputColumn) {
            this.outputColumn = outputColumn;
            this.fieldEntries = new ArrayList<>(objectCount);
            for (int i = 0; i < objectCount; i++) {
                fieldEntries.add(new ArrayList<>());
            }
        }

        void startBatch() {
            builder = new GenericVariantBuilder(false);
            rowCount = 0;
            metadataInputBytes = 0;
        }

        void append(DataGetters record, int fieldIndex) {
            if (builder == null) {
                throw new IllegalStateException("Projected Variant batch has not been started");
            }
            boolean isNull = record.isNullAt(fieldIndex);
            if (!isNull) {
                InternalRow extracted = record.getRow(fieldIndex, readType.getFieldCount());
                RowSizeEstimate estimate = readExtractedValues(extracted);
                if (shouldFlushBefore(estimate)) {
                    flush();
                    startBatch();
                }
                metadataInputBytes += estimate.metadataBytes;
            }

            ensureRowCapacity(rowCount + 1);
            valueOffsets[rowCount] = builder.getWritePos();
            nullRows[rowCount] = isNull;
            if (!isNull) {
                try {
                    appendObject(root);
                } finally {
                    Arrays.fill(extractedValues, null);
                }
            }
            rowCount++;
        }

        /**
         * Finalizes the shared metadata once and appends every encoded row by buffer range.
         */
        void flush() {
            if (builder == null) {
                return;
            }
            if (rowCount == 0) {
                builder = null;
                return;
            }

            valueOffsets[rowCount] = builder.getWritePos();
            Variant batch = builder.result();
            byte[] metadata = batch.metadata();
            byte[] values = batch.value();
            for (int row = 0; row < rowCount; row++) {
                if (nullRows[row]) {
                    outputColumn.appendNull(ColumnType.Type.VARIANT);
                } else {
                    int offset = valueOffsets[row];
                    outputColumn.appendVariant(
                            metadata, values, offset, valueOffsets[row + 1] - offset);
                }
            }
            builder = null;
        }

        int rowCount() {
            return rowCount;
        }

        private RowSizeEstimate readExtractedValues(InternalRow extracted) {
            Arrays.fill(extractedValues, null);
            long valueBytesUpperBound = projectionValueOverhead;
            long metadataBytes = 0;
            for (int fieldIndex = 0; fieldIndex < extractedValues.length; fieldIndex++) {
                if (!hasExtractedVariant(extracted, fieldIndex)) {
                    continue;
                }
                InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
                byte[] value = variant.getBinary(VARIANT_VALUE_INDEX);
                byte[] metadata = variant.getBinary(VARIANT_METADATA_INDEX);
                extractedValues[fieldIndex] = new GenericVariant(value, metadata);
                valueBytesUpperBound += 4L * value.length;
                metadataBytes += metadata.length;
            }
            return new RowSizeEstimate(valueBytesUpperBound, metadataBytes);
        }

        private boolean shouldFlushBefore(RowSizeEstimate estimate) {
            if (rowCount == 0) {
                return false;
            }
            return builder.getWritePos() + estimate.valueBytesUpperBound
                            > GenericVariantUtil.SIZE_LIMIT
                    || metadataInputBytes + estimate.metadataBytes
                            > GenericVariantUtil.SIZE_LIMIT / 4L;
        }

        private void appendObject(PathNode node) {
            int start = builder.getWritePos();
            ArrayList<GenericVariantBuilder.FieldEntry> fields = fieldEntries.get(node.objectIndex);
            fields.clear();
            for (Map.Entry<String, PathNode> entry : node.children.entrySet()) {
                PathNode child = entry.getValue();
                if (!hasValue(child)) {
                    continue;
                }
                String key = entry.getKey();
                int dictionaryId = builder.addKey(key);
                fields.add(new GenericVariantBuilder.FieldEntry(
                        key, dictionaryId, builder.getWritePos() - start));
                if (child.fieldIndex >= 0) {
                    builder.appendVariant(extractedValues[child.fieldIndex]);
                } else {
                    appendObject(child);
                }
            }
            builder.finishWritingObject(start, fields);
        }

        private void ensureRowCapacity(int requiredCapacity) {
            if (requiredCapacity <= nullRows.length) {
                return;
            }
            int newCapacity = Math.max(requiredCapacity, nullRows.length * 2);
            valueOffsets = Arrays.copyOf(valueOffsets, newCapacity + 1);
            nullRows = Arrays.copyOf(nullRows, newCapacity);
        }

        private boolean hasValue(PathNode node) {
            if (node.fieldIndex >= 0) {
                return extractedValues[node.fieldIndex] != null;
            }
            for (PathNode child : node.children.values()) {
                if (hasValue(child)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static boolean hasExtractedVariant(InternalRow extracted, int fieldIndex) {
        // Paimon 1.4.2 does not reliably advance the enclosing Variant row vector for non-null
        // values. Its value and metadata children remain aligned and are the source of truth.
        InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
        boolean valueIsNull = variant.isNullAt(VARIANT_VALUE_INDEX);
        boolean metadataIsNull = variant.isNullAt(VARIANT_METADATA_INDEX);
        if (valueIsNull != metadataIsNull) {
            throw new IllegalStateException(
                    "Paimon projected Variant must contain both value and metadata");
        }
        return !valueIsNull;
    }

    private static final class RowSizeEstimate {
        private final long valueBytesUpperBound;
        private final long metadataBytes;

        private RowSizeEstimate(long valueBytesUpperBound, long metadataBytes) {
            this.valueBytesUpperBound = valueBytesUpperBound;
            this.metadataBytes = metadataBytes;
        }
    }

    private static final class PathNode {
        private final Map<String, PathNode> children = new LinkedHashMap<>();
        private int fieldIndex = -1;
        private int objectIndex;

        private boolean add(List<String> path, int index) {
            PathNode node = this;
            for (String segment : path) {
                if (node.fieldIndex >= 0) {
                    return false;
                }
                node = node.children.computeIfAbsent(segment, ignored -> new PathNode());
            }
            if (!node.children.isEmpty() || node.fieldIndex >= 0) {
                return false;
            }
            node.fieldIndex = index;
            return true;
        }

        private int assignObjectIndexes(int nextIndex) {
            objectIndex = nextIndex++;
            for (PathNode child : children.values()) {
                if (child.fieldIndex < 0) {
                    nextIndex = child.assignObjectIndexes(nextIndex);
                }
            }
            return nextIndex;
        }

        private long valueOverheadUpperBound() {
            // With four-byte ids and offsets, an object header is at most 9 + 8 * fields bytes.
            long size = 9L + 8L * children.size();
            for (PathNode child : children.values()) {
                if (child.fieldIndex < 0) {
                    size += child.valueOverheadUpperBound();
                }
            }
            return size;
        }
    }
}
