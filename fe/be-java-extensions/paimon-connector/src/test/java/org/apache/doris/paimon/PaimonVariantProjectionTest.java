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

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorTable;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class PaimonVariantProjectionTest {
    @BeforeClass
    public static void setUpClass() {
        OffHeap.setTesting();
    }

    @Test
    public void testBuildsMetadataMarkedReadTypeAndPartialVariant() {
        PaimonVariantProjection projection = PaimonVariantProjection.create(
                Arrays.asList(
                        Collections.singletonList("name"),
                        Arrays.asList("profile", "city"),
                        Collections.singletonList("missing")),
                "Asia/Shanghai");

        Assert.assertNotNull(projection);
        Assert.assertEquals("$.name", VariantMetadataUtils.path(
                projection.readType().getFields().get(0).description()));
        Assert.assertEquals("$.profile.city", VariantMetadataUtils.path(
                projection.readType().getFields().get(1).description()));
        Assert.assertFalse(VariantMetadataUtils.failOnError(
                projection.readType().getFields().get(1).description()));

        GenericRow extracted = projectedRecord(
                GenericVariant.fromJson("\"alice\""),
                GenericVariant.fromJson("\"beijing\""),
                null);
        Variant result = materialize(projection, extracted);
        Assert.assertEquals(
                "{\"name\":\"alice\",\"profile\":{\"city\":\"beijing\"}}",
                result.toJson());
    }

    @Test
    public void testPreservesJsonNullButOmitsMissingPath() {
        PaimonVariantProjection projection = PaimonVariantProjection.create(
                Arrays.asList(
                        Collections.singletonList("present"),
                        Collections.singletonList("missing")),
                "UTC");

        Variant result = materialize(
                projection, projectedRecord(GenericVariant.fromJson("null"), null));
        Assert.assertEquals("{\"present\":null}", result.toJson());
    }

    @Test
    public void testIgnoresMisalignedPaimonVariantNullBitmap() {
        PaimonVariantProjection projection = PaimonVariantProjection.create(
                Collections.singletonList(Collections.singletonList("name")), "UTC");

        GenericVariant alice = GenericVariant.fromJson("\"alice\"");
        GenericVariant bob = GenericVariant.fromJson("\"bob\"");
        HeapBytesVector values = new HeapBytesVector(3);
        HeapBytesVector metadata = new HeapBytesVector(3);
        appendVariant(values, metadata, alice);
        appendVariant(values, metadata, bob);

        HeapRowVector variants = new HeapRowVector(3, values, metadata);
        // Paimon 1.4.2 does not advance this row vector for non-null Variants. Appending the
        // missing third value consequently marks row 0 null even though its binary children hold
        // Alice; the binary children themselves still have the correct row alignment.
        variants.appendNull();
        HeapRowVector extractedRows = new HeapRowVector(3, variants);
        extractedRows.appendRow();
        extractedRows.appendRow();
        extractedRows.appendRow();

        Assert.assertEquals(
                "{\"name\":\"alice\"}",
                materialize(projection, GenericRow.of(extractedRows.getRow(0))).toJson());
        Assert.assertEquals(
                "{\"name\":\"bob\"}",
                materialize(projection, GenericRow.of(extractedRows.getRow(1))).toJson());
        Assert.assertEquals(
                "{}", materialize(projection, GenericRow.of(extractedRows.getRow(2))).toJson());
    }

    @Test
    public void testMaterializesRowsWithOneBatchBuilder() {
        PaimonVariantProjection projection = PaimonVariantProjection.create(
                Arrays.asList(
                        Collections.singletonList("name"),
                        Arrays.asList("profile", "city")),
                "UTC");
        GenericVariant first = GenericVariant.fromJson(
                "{\"name\":\"alice\",\"profile\":{\"city\":\"beijing\"}}");
        GenericVariant second = GenericVariant.fromJson(
                "{\"profile\":{\"city\":\"shanghai\"},\"name\":\"bob\"}");
        InternalRow[] rows = new InternalRow[] {
                projectedRecord(
                        first.getFieldByKey("name"),
                        first.getFieldByKey("profile").getFieldByKey("city")),
                projectedRecord(
                        second.getFieldByKey("name"),
                        second.getFieldByKey("profile").getFieldByKey("city"))
        };

        ColumnType variantType = ColumnType.parseType("v", "variant");
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {variantType}, new String[] {"v"}, rows.length);
        try {
            PaimonVariantProjection.Materializer materializer =
                    projection.newMaterializer(table.getColumn(0));
            materializer.startBatch();
            for (InternalRow row : rows) {
                materializer.append(row, 0);
            }
            Assert.assertEquals(2, materializer.rowCount());
            materializer.flush();

            Assert.assertEquals(
                    "{\"name\":\"alice\",\"profile\":{\"city\":\"beijing\"}}",
                    readVariant(table, 0).toJson());
            Assert.assertEquals(
                    "{\"name\":\"bob\",\"profile\":{\"city\":\"shanghai\"}}",
                    readVariant(table, 1).toJson());
        } finally {
            table.close();
        }
    }

    private static void appendVariant(
            HeapBytesVector values, HeapBytesVector metadata, GenericVariant variant) {
        values.appendByteArray(variant.value(), 0, variant.value().length);
        metadata.appendByteArray(variant.metadata(), 0, variant.metadata().length);
    }

    private static GenericRow projectedRecord(Variant... extractedValues) {
        ColumnVector[] fields = new ColumnVector[extractedValues.length];
        for (int i = 0; i < extractedValues.length; i++) {
            HeapBytesVector values = new HeapBytesVector(1);
            HeapBytesVector metadata = new HeapBytesVector(1);
            HeapRowVector variant = new HeapRowVector(1, values, metadata);
            if (extractedValues[i] == null) {
                variant.appendNull();
            } else {
                GenericVariant value = new GenericVariant(
                        extractedValues[i].value(), extractedValues[i].metadata());
                appendVariant(values, metadata, value);
                variant.appendRow();
            }
            fields[i] = variant;
        }
        HeapRowVector extracted = new HeapRowVector(1, fields);
        extracted.appendRow();
        return GenericRow.of(extracted.getRow(0));
    }

    private static Variant materialize(
            PaimonVariantProjection projection, InternalRow projectedRow) {
        ColumnType variantType = ColumnType.parseType("v", "variant");
        VectorTable table = VectorTable.createWritableTable(
                new ColumnType[] {variantType}, new String[] {"v"}, 1);
        try {
            PaimonVariantProjection.Materializer materializer =
                    projection.newMaterializer(table.getColumn(0));
            materializer.startBatch();
            materializer.append(projectedRow, 0);
            materializer.flush();
            return readVariant(table, 0);
        } finally {
            table.close();
        }
    }

    private static Variant readVariant(VectorTable table, int rowIndex) {
        long meta = table.getMetaAddress();
        long nullMap = OffHeap.getLong(null, meta + 8);
        Assert.assertFalse(OffHeap.getBoolean(null, nullMap + rowIndex));

        long metadataOffsets = OffHeap.getLong(null, meta + 24);
        long metadataBytes = OffHeap.getLong(null, meta + 32);
        long metadataIds = OffHeap.getLong(null, meta + 40);
        long valueOffsets = OffHeap.getLong(null, meta + 48);
        long valueBytes = OffHeap.getLong(null, meta + 56);

        int metadataId = OffHeap.getInt(null, metadataIds + (long) rowIndex * Integer.BYTES);
        int metadataStart = OffHeap.getInt(
                null, metadataOffsets + (long) metadataId * Integer.BYTES);
        int metadataEnd = OffHeap.getInt(
                null, metadataOffsets + (long) (metadataId + 1) * Integer.BYTES);
        int valueStart = OffHeap.getInt(
                null, valueOffsets + (long) rowIndex * Integer.BYTES);
        int valueEnd = OffHeap.getInt(
                null, valueOffsets + (long) (rowIndex + 1) * Integer.BYTES);
        return new GenericVariant(
                OffHeap.getByte(null, valueBytes + valueStart, valueEnd - valueStart),
                OffHeap.getByte(null, metadataBytes + metadataStart, metadataEnd - metadataStart));
    }

    @Test
    public void testFallsBackForAmbiguousOrUnsupportedPaths() {
        Assert.assertNull(PaimonVariantProjection.create(
                Collections.singletonList(Collections.singletonList("1")), "UTC"));
        Assert.assertNull(PaimonVariantProjection.create(
                Collections.singletonList(Collections.singletonList("a.b")), "UTC"));
        Assert.assertNull(PaimonVariantProjection.create(
                Collections.singletonList(Collections.singletonList("a;b")), "UTC"));
        Assert.assertNull(PaimonVariantProjection.create(
                Arrays.asList(
                        Collections.singletonList("profile"),
                        Arrays.asList("profile", "city")),
                "UTC"));
    }
}
