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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalTable.class);

    public IcebergExternalTable(long id, String name, String remoteName, IcebergExternalCatalog catalog,
            IcebergExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.ICEBERG_EXTERNAL_TABLE);
    }

    public String getIcebergCatalogType() {
        return ((IcebergExternalCatalog) catalog).getIcebergCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        return Optional.of(new SchemaCacheValue(IcebergUtils.getSchema(catalog, dbName, name)));
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (getIcebergCatalogType().equals("hms")) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            TIcebergTable icebergTable = new TIcebergTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_TABLE,
                    schema.size(), 0, getName(), dbName);
            tTableDescriptor.setIcebergTable(icebergTable);
            return tTableDescriptor;
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        long rowCount = IcebergUtils.getIcebergRowCount(getCatalog(), getDbName(), getName());
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    public Table getIcebergTable() {
        return IcebergUtils.getIcebergTable(getCatalog(), getDbName(), getName());
    }

    /**
     * Check if the table has at least one identity partition field.
     * Only identity transforms can be used for internal partition pruning.
     * Other transforms (day, month, bucket, truncate, etc.) are handled
     * by Iceberg's own predicate pushdown at execution time.
     */
    private boolean hasIdentityPartitionFields() {
        Table icebergTable = getIcebergTable();
        PartitionSpec spec = icebergTable.spec();
        if (spec.isUnpartitioned()) {
            return false;
        }
        for (PartitionField field : spec.fields()) {
            if (field.transform().isIdentity()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        try {
            makeSureInitialized();
            return hasIdentityPartitionFields();
        } catch (Exception e) {
            LOG.warn("Failed to check partition support for Iceberg table {}", getName(), e);
            return false;
        }
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        try {
            makeSureInitialized();
            Table icebergTable = getIcebergTable();
            PartitionSpec spec = icebergTable.spec();
            if (spec.isUnpartitioned()) {
                return Collections.emptyList();
            }
            Schema icebergSchema = icebergTable.schema();
            List<Column> partitionColumns = new ArrayList<>();
            for (PartitionField field : spec.fields()) {
                if (!field.transform().isIdentity()) {
                    continue;
                }
                Types.NestedField sourceField = icebergSchema.findField(field.sourceId());
                if (sourceField != null) {
                    Type dorisType = IcebergUtils.icebergTypeToDorisType(sourceField.type());
                    partitionColumns.add(new Column(sourceField.name().toLowerCase(), dorisType,
                            true, null, true, null, true, sourceField.fieldId()));
                }
            }
            return partitionColumns;
        } catch (Exception e) {
            LOG.warn("Failed to get partition columns for Iceberg table {}", getName(), e);
            return Collections.emptyList();
        }
    }

    /**
     * Get the indices of identity partition fields within the full partition spec.
     * This is needed because the StructLike partition data from manifests uses
     * the full spec index, not just the identity field index.
     * For example, spec (days(ts), site) has identity field at index 1, not 0.
     */
    private List<Integer> getIdentityFieldIndices() {
        Table icebergTable = getIcebergTable();
        PartitionSpec spec = icebergTable.spec();
        List<Integer> indices = new ArrayList<>();
        List<PartitionField> fields = spec.fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).transform().isIdentity()) {
                indices.add(i);
            }
        }
        return indices;
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        try {
            makeSureInitialized();
            Table icebergTable = getIcebergTable();
            PartitionSpec spec = icebergTable.spec();
            if (spec.isUnpartitioned()) {
                return Collections.emptyMap();
            }
            org.apache.iceberg.Snapshot currentSnapshot = icebergTable.currentSnapshot();
            if (currentSnapshot == null) {
                return Collections.emptyMap();
            }
            List<Column> partitionColumns = getPartitionColumns(snapshot);
            if (partitionColumns.isEmpty()) {
                return Collections.emptyMap();
            }
            List<Type> partitionTypes = partitionColumns.stream()
                    .map(Column::getType)
                    .collect(java.util.stream.Collectors.toList());
            List<Integer> identityIndices = getIdentityFieldIndices();

            // Collect unique partition values from manifest entries (identity fields only)
            Map<String, PartitionItem> nameToPartitionItems = new HashMap<>();
            List<org.apache.iceberg.ManifestFile> manifests = currentSnapshot.allManifests(icebergTable.io());
            for (org.apache.iceberg.ManifestFile manifest : manifests) {
                if (manifest.content() != org.apache.iceberg.ManifestContent.DATA) {
                    continue;
                }
                try (org.apache.iceberg.ManifestReader<org.apache.iceberg.DataFile> reader =
                        org.apache.iceberg.ManifestFiles.read(manifest, icebergTable.io())) {
                    for (org.apache.iceberg.DataFile dataFile : reader) {
                        org.apache.iceberg.StructLike partition = dataFile.partition();
                        buildPartitionItem(partition, partitionColumns, partitionTypes,
                                identityIndices, nameToPartitionItems);
                    }
                }
            }
            return nameToPartitionItems;
        } catch (Exception e) {
            LOG.warn("Failed to get partition items for Iceberg table {}", getName(), e);
            return Collections.emptyMap();
        }
    }

    private void buildPartitionItem(org.apache.iceberg.StructLike partitionData,
            List<Column> partitionColumns, List<Type> partitionTypes,
            List<Integer> identityIndices,
            Map<String, PartitionItem> nameToPartitionItems) {
        try {
            List<org.apache.doris.analysis.PartitionValue> values = new ArrayList<>();
            StringBuilder nameBuilder = new StringBuilder();
            for (int i = 0; i < partitionColumns.size(); i++) {
                // Use the actual index in the full partition spec, not sequential index.
                // For spec (days(ts), site), the identity field "site" is at spec index 1.
                int specIndex = identityIndices.get(i);
                Object val = partitionData.get(specIndex, Object.class);
                String strVal = val == null ? "null" : val.toString();
                values.add(new org.apache.doris.analysis.PartitionValue(strVal));
                if (nameBuilder.length() > 0) {
                    nameBuilder.append("/");
                }
                nameBuilder.append(partitionColumns.get(i).getName()).append("=").append(strVal);
            }
            String name = nameBuilder.toString();
            if (nameToPartitionItems.containsKey(name)) {
                return; // already seen this partition combination
            }
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                    values, partitionTypes, true);
            ListPartitionItem item = new ListPartitionItem(Lists.newArrayList(partitionKey));
            nameToPartitionItems.put(name, item);
        } catch (AnalysisException e) {
            LOG.warn("Failed to build partition item for Iceberg table {}: {}", getName(), e.getMessage());
        }
    }
}
