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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TPaimonBlobFileDesc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Plans managed BLOB inputs while retaining the SDK's merge and fallback semantics. */
final class PaimonBlobScanPlanner {
    private final FileStoreTable table;
    private final Function<String, String> normalize;
    private final Predicate<DataFileMeta> supportedAnchor;

    PaimonBlobScanPlanner(FileStoreTable table, Function<String, String> normalize,
            Predicate<DataFileMeta> supportedAnchor) {
        this.table = table;
        this.normalize = normalize;
        this.supportedAnchor = supportedAnchor;
    }

    static boolean containsBlob(DataType type) {
        switch (type.getTypeRoot()) {
            case BLOB:
                return true;
            case ARRAY:
                return containsBlob(((ArrayType) type).getElementType());
            case MAP:
                return containsBlob(((MapType) type).getKeyType())
                        || containsBlob(((MapType) type).getValueType());
            case ROW:
                return ((RowType) type).getFields().stream().anyMatch(field -> containsBlob(field.type()));
            default:
                return false;
        }
    }

    static List<Integer> descriptorFieldIds(Table table, Set<String> projectedNames) {
        if (!(table instanceof FileStoreTable) || table instanceof FallbackReadFileStoreTable
                || table.primaryKeys().isEmpty() || ((FileStoreTable) table).coreOptions().queryAuthEnabled()) {
            return Collections.emptyList();
        }
        List<String> managedFields = CoreOptions.blobField(table.options());
        List<Integer> ids = new ArrayList<>();
        for (DataField field : table.rowType().getFields()) {
            if (!projectedNames.contains(field.name().toLowerCase(Locale.ROOT)) || !containsBlob(field.type())) {
                continue;
            }
            if (!managedFields.contains(field.name()) || !supportsBlobDescriptorType(field.type())) {
                return Collections.emptyList();
            }
            ids.add(field.id());
        }
        return ids;
    }

    private static boolean supportsBlobDescriptorType(DataType type) {
        switch (type.getTypeRoot()) {
            case BLOB:
                return true;
            case ARRAY:
                return ((ArrayType) type).getElementType().getTypeRoot() == DataTypeRoot.BLOB;
            case MAP:
                return ((MapType) type).getValueType().getTypeRoot() == DataTypeRoot.BLOB;
            default:
                return false;
        }
    }

    Optional<List<PaimonScanRange>> planAppend(DataSplit split, Set<String> projectedNames,
            Map<String, String> partitionValues, long targetSplitSize, long weightDenominator) {
        if (table instanceof FallbackReadFileStoreTable || !table.coreOptions().dataEvolutionEnabled()
                || table.coreOptions().queryAuthEnabled()
                || table.rowType().getFields().stream().anyMatch(field ->
                        projectedNames.contains(field.name().toLowerCase(Locale.ROOT))
                                && containsBlob(field.type()) && field.type().getTypeRoot() != DataTypeRoot.BLOB)
                || !canUseNativeBlobSplit(split, projectedNames)) {
            return Optional.empty();
        }
        return createNativeBlobSplits(split, partitionValues, targetSplitSize, weightDenominator, projectedNames);
    }

    private boolean canUseNativeBlobSplit(DataSplit dataSplit, Set<String> projectedNames) {
        if (dataSplit instanceof FallbackReadFileStoreTable.FallbackSplit
                || !table.primaryKeys().isEmpty() || dataSplit.isStreaming()) {
            return false;
        }
        // TODO: If Paimon enables deletion vectors for data-evolution tables, propagate each
        // anchor's deletion file to its native splits and preserve original row positions
        // when materializing BLOBs. Paimon 1.4.2 currently rejects this option combination.
        if (dataSplit.deletionFiles().orElse(Collections.emptyList()).stream().anyMatch(Objects::nonNull)) {
            return false;
        }
        if (table.coreOptions().blobDescriptorField().stream()
                .anyMatch(name -> projectedNames.contains(name.toLowerCase(Locale.ROOT)))) {
            return false;
        }
        for (DataFileMeta file : dataSplit.dataFiles()) {
            // Companion files must use the same backend configuration as the anchor.
            if (file.externalPath().isPresent() || file.externalPathDir().isPresent()) {
                return false;
            }
            if (file.firstRowId() == null || file.firstRowId() < 0 || file.rowCount() <= 0
                    || file.firstRowId() > Long.MAX_VALUE - file.rowCount()) {
                throw new DorisConnectorException("Missing or invalid Paimon data-evolution row range for "
                        + file.fileName());
            }
            if (BlobFileFormat.isBlobFile(file.fileName())
                    && (file.writeCols() == null || file.writeCols().size() != 1)) {
                throw new DorisConnectorException("Managed Paimon BLOB files must contain exactly one column");
            }
        }
        return true;
    }

    /**
     * Plans native reads only for projected BLOB columns using Paimon's Default blob storage:
     * payloads live in managed .blob files and are joined to ordinary columns by row ID.
     * Descriptor-only and external-storage descriptor modes fall back to JNI.
     * The blob-as-descriptor read option is independent of the storage mode; native reads
     * support both inline payload and descriptor output for Default blob storage.
     * Returns Optional.empty() when the file layout requires JNI.
     */
    private Optional<List<PaimonScanRange>> createNativeBlobSplits(DataSplit dataSplit,
            Map<String, String> partitionValues,
            long targetSplitSize, long weightDenominator, Set<String> projectedNames) {
        Set<Integer> projectedBlobIds = table.rowType().getFields().stream()
                .filter(field -> field.type().getTypeRoot() == DataTypeRoot.BLOB
                        && projectedNames.contains(field.name().toLowerCase(Locale.ROOT)))
                .map(DataField::id).collect(Collectors.toSet());
        List<PaimonScanRange> result = new ArrayList<>();
        try {
            // Reuse the SDK's overlap grouping and latest-version selection. Flattening the
            // DataSplit would duplicate rows, and selecting every .blob file would read obsolete
            // versions after compaction.
            for (List<DataFileMeta> group : DataEvolutionSplitRead.mergeRangesAndSort(dataSplit.dataFiles())) {
                DataFileMeta anchor = findBlobParquetAnchor(group, projectedBlobIds);
                if (anchor == null || !supportedAnchor.test(anchor)) {
                    return Optional.empty();
                }
                List<DataEvolutionSplitRead.FieldBunch> bunches = DataEvolutionSplitRead.splitFieldBunches(
                        group, file -> table.schemaManager().schema(file.schemaId()).logicalRowType());
                List<TPaimonBlobFileDesc> blobFiles = new ArrayList<>();
                for (DataEvolutionSplitRead.FieldBunch bunch : bunches) {
                    DataFileMeta first = bunch.files().get(0);
                    if (!BlobFileFormat.isBlobFile(first.fileName())) {
                        continue;
                    }
                    DataField field = table.schemaManager().schema(first.schemaId()).logicalRowType()
                            .getField(first.writeCols().get(0));
                    if (!projectedBlobIds.contains(field.id())) {
                        continue;
                    }
                    if (bunch.rowCount() != anchor.rowCount()
                            || first.nonNullFirstRowId() != anchor.nonNullFirstRowId()) {
                        return Optional.empty();
                    }
                    for (DataFileMeta blob : bunch.files()) {
                        blobFiles.add(createBlobFileDescriptor(dataSplit, blob));
                    }
                }
                String path = paimonDataFilePath(dataSplit, anchor);
                for (long[] offset : PaimonScanPlanProvider.computeFileSplitOffsets(
                        anchor.fileSize(), targetSplitSize)) {
                    result.add(new PaimonScanRange.Builder()
                            .path(normalize.apply(path)).originalFilePath(path)
                            .start(offset[0]).length(offset[1]).fileSize(anchor.fileSize())
                            .fileFormat("parquet").schemaId(anchor.schemaId())
                            .partitionValues(partitionValues).bucket(dataSplit.bucket())
                            .selfSplitWeight(offset[1]).targetSplitSize(weightDenominator)
                            .blobFiles(blobFiles, anchor.nonNullFirstRowId(), anchor.rowCount()).build());
                }
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new DorisConnectorException("Failed to plan native Paimon BLOB files: " + e.getMessage(), e);
        }
        return Optional.of(result);
    }

    /**
     * Selects the single ordinary Parquet file that supplies columns and original row positions
     * for joining managed .blob files. Multiple ordinary files require column merging, which
     * this native path does not support. Returns null for layouts that must fall back to JNI.
     */
    private DataFileMeta findBlobParquetAnchor(List<DataFileMeta> group, Set<Integer> projectedBlobIds) {
        DataFileMeta anchor = null;
        for (DataFileMeta file : group) {
            if (BlobFileFormat.isBlobFile(file.fileName())) {
                continue;
            }
            // Multiple ordinary column files require a column-merge reader. Choosing one of them
            // could omit newer ordinary values, even when all their row ranges match.
            if (anchor != null || !"parquet".equals(file.fileFormat())) {
                return null;
            }
            anchor = file;
        }
        if (anchor == null) {
            return null;
        }
        // Descriptor-only and external-storage descriptor modes store descriptors in Parquet,
        // rather than payloads in managed .blob files; projected columns in these modes use JNI.
        for (DataField field : table.schemaManager().schema(anchor.schemaId()).logicalRowType().getFields()) {
            if (field.type().getTypeRoot() == DataTypeRoot.BLOB
                    && projectedBlobIds.contains(field.id())
                    && (anchor.writeCols() == null || anchor.writeCols().contains(field.name()))) {
                return null;
            }
        }
        return anchor;
    }

    private TPaimonBlobFileDesc createBlobFileDescriptor(DataSplit split, DataFileMeta file) {
        DataField field = table.schemaManager().schema(file.schemaId()).logicalRowType()
                .getField(file.writeCols().get(0));
        if (field.type().getTypeRoot() != DataTypeRoot.BLOB) {
            throw new DorisConnectorException("Managed Paimon BLOB file has a non-BLOB field: " + field.name());
        }
        TPaimonBlobFileDesc descriptor = new TPaimonBlobFileDesc();
        descriptor.setPath(normalize.apply(paimonDataFilePath(split, file)));
        descriptor.setFileSize(file.fileSize());
        descriptor.setFirstRowId(file.nonNullFirstRowId());
        descriptor.setRowCount(file.rowCount());
        descriptor.setFieldId(field.id());
        return descriptor;
    }

    private static String paimonDataFilePath(DataSplit split, DataFileMeta file) {
        return file.externalPath().orElse(split.bucketPath() + "/" + file.fileName());
    }

}
