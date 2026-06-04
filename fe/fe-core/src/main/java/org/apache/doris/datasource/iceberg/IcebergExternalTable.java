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
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IcebergExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(IcebergExternalTable.class);

    // Cached partition data for ResidualEvaluator-based pruning.
    // Populated by initSelectedPartitions() for non-identity partition transforms.
    private Map<String, StructLike> cachedPartitionData = null;

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
     * Check if all partition fields use identity transform.
     * Identity-only tables can use the fast Doris partition pruning framework.
     * Tables with non-identity transforms need ResidualEvaluator-based pruning.
     */
    public boolean isAllIdentityPartitions() {
        Table icebergTable = getIcebergTable();
        PartitionSpec spec = icebergTable.spec();
        if (spec.isUnpartitioned()) {
            return false;
        }
        for (PartitionField field : spec.fields()) {
            if (!field.transform().isIdentity()) {
                return false;
            }
        }
        return true;
    }

    private boolean hasPartitionFields() {
        Table icebergTable = getIcebergTable();
        return !icebergTable.spec().isUnpartitioned();
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        try {
            makeSureInitialized();
            return hasPartitionFields();
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
     * Override initSelectedPartitions to support non-identity partition transforms.
     * For identity-only tables, use the parent implementation (Doris partition pruning).
     * For tables with non-identity transforms, we read partition data from manifests
     * and cache the StructLike data for use with ResidualEvaluator-based pruning.
     */
    @Override
    public SelectedPartitions initSelectedPartitions(Optional<MvccSnapshot> snapshot) {
        try {
            makeSureInitialized();
            Table icebergTable = getIcebergTable();
            if (icebergTable.spec().isUnpartitioned()) {
                return SelectedPartitions.NOT_PRUNED;
            }
            if (isAllIdentityPartitions()) {
                return super.initSelectedPartitions(snapshot);
            }
            // Non-identity partitions: read all partitions and cache StructLike data
            Map<String, PartitionItem> nameToPartitionItems = new HashMap<>();
            Map<String, StructLike> partitionDataMap = new HashMap<>();
            fetchAllPartitions(snapshot, nameToPartitionItems, partitionDataMap);
            this.cachedPartitionData = partitionDataMap;
            if (nameToPartitionItems.isEmpty()) {
                return SelectedPartitions.NOT_PRUNED;
            }
            return new SelectedPartitions(nameToPartitionItems.size(), nameToPartitionItems, false);
        } catch (Exception e) {
            LOG.warn("Failed to init selected partitions for Iceberg table {}", getName(), e);
            return SelectedPartitions.NOT_PRUNED;
        }
    }

    /**
     * Read all partitions from manifest files for tables with non-identity transforms.
     * Builds both partition items (for bookkeeping) and caches StructLike data (for pruning).
     */
    private void fetchAllPartitions(Optional<MvccSnapshot> snapshot,
            Map<String, PartitionItem> nameToPartitionItems,
            Map<String, StructLike> partitionDataMap) {
        Table icebergTable = getIcebergTable();
        org.apache.iceberg.Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            return;
        }
        PartitionSpec spec = icebergTable.spec();
        List<PartitionField> fields = spec.fields();

        for (ManifestFile manifest : currentSnapshot.allManifests(icebergTable.io())) {
            if (manifest.content() != ManifestContent.DATA) {
                continue;
            }
            try (ManifestReader<org.apache.iceberg.DataFile> reader =
                    ManifestFiles.read(manifest, icebergTable.io())) {
                for (org.apache.iceberg.DataFile dataFile : reader) {
                    StructLike partition = dataFile.partition();
                    String name = buildPartitionName(fields, partition);
                    if (nameToPartitionItems.containsKey(name)) {
                        continue;
                    }
                    // Create a minimal partition item for bookkeeping.
                    // The partition values are the transformed values from the manifest,
                    // NOT the source column values. These items are only used for counting;
                    // actual pruning uses ResidualEvaluator with the cached StructLike data.
                    PartitionItem item = buildBookkeepingPartitionItem(fields, partition);
                    if (item == null) {
                        continue;
                    }
                    nameToPartitionItems.put(name, item);
                    // Deep-copy the partition data for later use in ResidualEvaluator
                    partitionDataMap.put(name, copyStructLike(partition));
                }
            } catch (Exception e) {
                LOG.warn("Failed to read manifest for Iceberg table {}: {}", getName(), e.getMessage());
            }
        }
    }

    /** Build a human-readable partition name from spec fields and partition data. */
    private String buildPartitionName(List<PartitionField> fields, StructLike partition) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (sb.length() > 0) {
                sb.append("/");
            }
            Object val = partition.get(i, Object.class);
            sb.append(fields.get(i).name()).append("=")
                    .append(val == null ? "null" : val.toString());
        }
        return sb.toString();
    }

    /**
     * Build a minimal partition item for bookkeeping purposes.
     * For non-identity transforms, the partition values are transformed values,
     * so we can't build proper source-column partition keys.
     * These items exist only so the pruning framework has partition entries to work with.
     */
    private PartitionItem buildBookkeepingPartitionItem(List<PartitionField> fields, StructLike partition) {
        try {
            List<org.apache.doris.analysis.PartitionValue> values = new ArrayList<>();
            List<Type> types = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                Object val = partition.get(i, Object.class);
                String strVal = val == null ? "null" : val.toString();
                values.add(new org.apache.doris.analysis.PartitionValue(strVal));
                // All transforms produce integer partition values except truncate(STRING)
                // which produces strings. Even day() whose getResultType()=DATE stores
                // the value as integer days since epoch in the manifest.
                PartitionField pf = fields.get(i);
                org.apache.iceberg.types.Type sourceType =
                        getIcebergTable().spec().schema().findField(pf.sourceId()).type();
                org.apache.iceberg.types.Type resultType =
                        pf.transform().getResultType(sourceType);
                if (resultType.typeId() == org.apache.iceberg.types.Type.TypeID.STRING) {
                    types.add(Type.VARCHAR);
                } else {
                    types.add(Type.INT);
                }
            }
            PartitionKey partitionKey = PartitionKey.createListPartitionKeyWithTypes(
                    values, types, true);
            return new ListPartitionItem(Lists.newArrayList(partitionKey));
        } catch (AnalysisException e) {
            LOG.warn("Failed to build bookkeeping partition item for Iceberg table {}: {}",
                    getName(), e.getMessage());
            return null;
        }
    }

    /** Deep-copy StructLike data so it's safe to use after the manifest reader closes. */
    private StructLike copyStructLike(StructLike source) {
        int size = source.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            values[i] = source.get(i, Object.class);
        }
        return new StructLike() {
            @Override public int size() { return values.length; }
            @Override public <T> T get(int pos, Class<T> javaClass) { return javaClass.cast(values[pos]); }
            @Override public <T> void set(int pos, T value) { values[pos] = value; }
        };
    }

    /**
     * Get the cached partition StructLike data. Only valid after initSelectedPartitions()
     * has been called for tables with non-identity partition transforms.
     */
    public Map<String, StructLike> getCachedPartitionData() {
        return cachedPartitionData != null ? cachedPartitionData : Collections.emptyMap();
    }

    /**
     * Prune partitions for Iceberg tables with non-identity transforms.
     *
     * Uses Iceberg's StrictProjection to rewrite the predicate from source-column
     * terms into partition-struct-field terms (applying the partition transform to
     * literal values), then evaluates the projected expression against each
     * partition's StructLike data directly.
     *
     * We do NOT use ResidualEvaluator because in Iceberg 1.6.1 it has a recursive
     * resolution issue: when transform.projectStrict() projects a predicate, the
     * recursive predicate(BoundPredicate) call fails to resolve partition-struct-field
     * references via getFieldsBySourceId(), causing the original predicate to be
     * returned as-is (never False), so no partition is ever pruned.
     *
     * @param nereidsPredicate the filter predicate from the query (Nereids expression)
     * @param nameToPartitionItem all partition items (for name iteration)
     * @return list of surviving partition names
     */
    public List<String> pruneWithResidualEvaluator(
            org.apache.doris.nereids.trees.expressions.Expression nereidsPredicate,
            Map<String, PartitionItem> nameToPartitionItem) {
        try {
            Table icebergTable = getIcebergTable();
            PartitionSpec spec = icebergTable.spec();
            if (cachedPartitionData == null || cachedPartitionData.isEmpty()) {
                return new ArrayList<>(nameToPartitionItem.keySet());
            }

            org.apache.iceberg.expressions.Expression icebergExpr =
                    convertNereidsToIcebergExpr(nereidsPredicate, icebergTable.schema());
            if (icebergExpr == null) {
                return new ArrayList<>(nameToPartitionItem.keySet());
            }

            // Bind to table schema so BoundReference field IDs match partition field source IDs.
            org.apache.iceberg.expressions.Expression boundToSchema =
                    Binder.bind(icebergTable.schema().asStruct(), icebergExpr, true);

            // Recursively project and evaluate, handling AND/OR compound expressions.
            return pruneRecursive(boundToSchema, spec, nameToPartitionItem, cachedPartitionData);
        } catch (Exception e) {
            LOG.warn("Failed to prune partitions with ResidualEvaluator for Iceberg table {}: {}",
                    getName(), e.getMessage(), e);
            return new ArrayList<>(nameToPartitionItem.keySet());
        }
    }

    /**
     * Recursively project and evaluate a bound Iceberg expression against partition data.
     * Handles AND, OR, NOT compound expressions as well as single predicates.
     *
     * @return list of partition names that satisfy (or might satisfy) the expression
     */
    private List<String> pruneRecursive(
            org.apache.iceberg.expressions.Expression boundExpr,
            PartitionSpec spec,
            Map<String, PartitionItem> nameToPartitionItem,
            Map<String, StructLike> partitionDataMap) {
        org.apache.iceberg.expressions.Expression.Operation op = boundExpr.op();
        if (op == org.apache.iceberg.expressions.Expression.Operation.TRUE) {
            return new ArrayList<>(nameToPartitionItem.keySet());
        }
        if (op == org.apache.iceberg.expressions.Expression.Operation.FALSE) {
            return new ArrayList<>();
        }
        if (op == org.apache.iceberg.expressions.Expression.Operation.NOT) {
            List<String> childResult = pruneRecursive(
                    ((org.apache.iceberg.expressions.Not) boundExpr).child(),
                    spec, nameToPartitionItem, partitionDataMap);
            // Complement: keep partitions not in childResult
            Set<String> pruned = new java.util.HashSet<>(childResult);
            List<String> complement = new ArrayList<>();
            for (String name : nameToPartitionItem.keySet()) {
                if (!pruned.contains(name)) {
                    complement.add(name);
                }
            }
            return complement;
        }
        if (op == org.apache.iceberg.expressions.Expression.Operation.AND) {
            org.apache.iceberg.expressions.And andExpr =
                    (org.apache.iceberg.expressions.And) boundExpr;
            List<String> leftResult = pruneRecursive(andExpr.left(), spec,
                    nameToPartitionItem, partitionDataMap);
            List<String> rightResult = pruneRecursive(andExpr.right(), spec,
                    nameToPartitionItem, partitionDataMap);
            // Intersection: keep partitions that survive both
            Set<String> intersect = new java.util.HashSet<>(leftResult);
            intersect.retainAll(rightResult);
            return new ArrayList<>(intersect);
        }
        if (op == org.apache.iceberg.expressions.Expression.Operation.OR) {
            org.apache.iceberg.expressions.Or orExpr =
                    (org.apache.iceberg.expressions.Or) boundExpr;
            List<String> leftResult = pruneRecursive(orExpr.left(), spec,
                    nameToPartitionItem, partitionDataMap);
            List<String> rightResult = pruneRecursive(orExpr.right(), spec,
                    nameToPartitionItem, partitionDataMap);
            // Union: keep partitions that survive either
            Set<String> union = new java.util.HashSet<>(leftResult);
            union.addAll(rightResult);
            return new ArrayList<>(union);
        }
        // Leaf predicate: project through transform and evaluate
        if (boundExpr instanceof BoundPredicate) {
            @SuppressWarnings("rawtypes")
            BoundPredicate pred = (BoundPredicate) boundExpr;
            return pruneSinglePredicate(pred, spec, nameToPartitionItem, partitionDataMap);
        }
        // Unknown expression type: keep all (conservative)
        return new ArrayList<>(nameToPartitionItem.keySet());
    }

    /**
     * Project a single bound predicate through the partition spec transform
     * and evaluate against each partition's data.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private List<String> pruneSinglePredicate(
            BoundPredicate pred,
            PartitionSpec spec,
            Map<String, PartitionItem> nameToPartitionItem,
            Map<String, StructLike> partitionDataMap) {
        List<PartitionField> fields = spec.getFieldsBySourceId(pred.ref().fieldId());
        if (fields == null || fields.isEmpty()) {
            return new ArrayList<>(nameToPartitionItem.keySet());
        }

        List<String> surviving = new ArrayList<>();
        for (PartitionField field : fields) {
            // Use project() (non-strict) instead of projectStrict() because
            // TruncateString.projectStrict returns null when literal length > truncation width.
            org.apache.iceberg.expressions.UnboundPredicate projectedPred =
                    (org.apache.iceberg.expressions.UnboundPredicate)
                            field.transform().project(field.name(), pred);
            if (projectedPred == null) {
                continue;
            }
            org.apache.iceberg.expressions.Expression boundProjected =
                    Binder.bind(spec.partitionType(), projectedPred, true);
            for (Map.Entry<String, PartitionItem> entry : nameToPartitionItem.entrySet()) {
                String name = entry.getKey();
                StructLike partitionData = partitionDataMap.get(name);
                if (partitionData == null
                        || evaluatePartitionExpr(boundProjected, partitionData)) {
                    surviving.add(name);
                }
            }
            return surviving;
        }
        return new ArrayList<>(nameToPartitionItem.keySet());
    }

    /**
     * Evaluate a bound (projected) Iceberg expression against a single partition's data.
     * The expression has been projected via StrictProjection and bound to the partition type,
     * so all BoundReferences reference partition struct fields.
     *
     * @return true if the partition satisfies (or might satisfy) the expression
     */
    private boolean evaluatePartitionExpr(
            org.apache.iceberg.expressions.Expression expr, StructLike partitionData) {
        org.apache.iceberg.expressions.Expression.Operation op = expr.op();
        switch (op) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            case NOT:
                return !evaluatePartitionExpr(
                        ((org.apache.iceberg.expressions.Not) expr).child(), partitionData);
            case AND:
                org.apache.iceberg.expressions.And andExpr =
                        (org.apache.iceberg.expressions.And) expr;
                return evaluatePartitionExpr(andExpr.left(), partitionData)
                        && evaluatePartitionExpr(andExpr.right(), partitionData);
            case OR:
                org.apache.iceberg.expressions.Or orExpr =
                        (org.apache.iceberg.expressions.Or) expr;
                return evaluatePartitionExpr(orExpr.left(), partitionData)
                        || evaluatePartitionExpr(orExpr.right(), partitionData);
            default:
                // Leaf predicate referencing partition struct fields
                if (expr instanceof BoundPredicate) {
                    return evaluateBoundPredicate((BoundPredicate<?>) expr, partitionData);
                }
                // Unknown expression type -- keep partition (conservative)
                return true;
        }
    }

    /**
     * Evaluate a single bound comparison predicate against partition data.
     * The BoundReference references a partition struct field, so ref.eval(partitionData)
     * extracts the actual transformed partition value (e.g., bucket number).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private boolean evaluateBoundPredicate(BoundPredicate<?> pred, StructLike partitionData) {
        org.apache.iceberg.expressions.Expression.Operation op = pred.op();
        if (op == org.apache.iceberg.expressions.Expression.Operation.IS_NULL) {
            return pred.ref().eval(partitionData) == null;
        }
        if (op == org.apache.iceberg.expressions.Expression.Operation.NOT_NULL) {
            return pred.ref().eval(partitionData) != null;
        }

        Object partitionValue = pred.ref().eval(partitionData);
        if (partitionValue == null) {
            // NULL partition value: can't evaluate, keep partition (conservative)
            return true;
        }

        if (pred.isSetPredicate()) {
            java.util.Set<?> literalSet = pred.asSetPredicate().literalSet();
            if (op == org.apache.iceberg.expressions.Expression.Operation.IN) {
                return literalSet.contains(partitionValue);
            }
            if (op == org.apache.iceberg.expressions.Expression.Operation.NOT_IN) {
                return !literalSet.contains(partitionValue);
            }
            return true;
        }

        if (pred.isLiteralPredicate()) {
            org.apache.iceberg.expressions.Literal<?> lit = pred.asLiteralPredicate().literal();
            java.util.Comparator cmp = lit.comparator();
            Object literalValue = lit.value();
            int result = cmp.compare(partitionValue, literalValue);
            switch (op) {
                case EQ:
                    return result == 0;
                case NOT_EQ:
                    return result != 0;
                case LT:
                    return result < 0;
                case LT_EQ:
                    return result <= 0;
                case GT:
                    return result > 0;
                case GT_EQ:
                    return result >= 0;
                default:
                    break;
            }
        }

        // Unknown predicate type -- keep partition (conservative)
        return true;
    }

    /**
     * Convert a Nereids expression to an Iceberg expression.
     * Handles the common predicate forms: comparison predicates, AND/OR/NOT,
     * InPredicate, IsNullPredicate. Returns null for expressions that cannot be converted.
     */
    private org.apache.iceberg.expressions.Expression convertNereidsToIcebergExpr(
            Expression nereidsExpr, Schema icebergSchema) {
        if (nereidsExpr == null) {
            return null;
        }
        // Handle AND
        if (nereidsExpr instanceof And) {
            org.apache.iceberg.expressions.Expression left =
                    convertNereidsToIcebergExpr(((And) nereidsExpr).left(), icebergSchema);
            org.apache.iceberg.expressions.Expression right =
                    convertNereidsToIcebergExpr(((And) nereidsExpr).right(), icebergSchema);
            if (left != null && right != null) {
                return Expressions.and(left, right);
            } else if (left != null) {
                return left;
            } else {
                return right;
            }
        }
        // Handle OR
        if (nereidsExpr instanceof Or) {
            org.apache.iceberg.expressions.Expression left =
                    convertNereidsToIcebergExpr(((Or) nereidsExpr).left(), icebergSchema);
            org.apache.iceberg.expressions.Expression right =
                    convertNereidsToIcebergExpr(((Or) nereidsExpr).right(), icebergSchema);
            if (left != null && right != null) {
                return Expressions.or(left, right);
            }
            return null; // can't safely handle partial OR
        }
        // Handle NOT
        if (nereidsExpr instanceof Not) {
            org.apache.iceberg.expressions.Expression child =
                    convertNereidsToIcebergExpr(((Not) nereidsExpr).child(), icebergSchema);
            return child != null ? Expressions.not(child) : null;
        }
        // Handle ComparisonPredicate (EqualTo, GreaterThan, etc.)
        if (nereidsExpr instanceof ComparisonPredicate) {
            return convertComparisonPredicate((ComparisonPredicate) nereidsExpr, icebergSchema);
        }
        // Handle InPredicate (NOT IN is represented as Not(InPredicate(...)))
        if (nereidsExpr instanceof InPredicate) {
            return convertInPredicate((InPredicate) nereidsExpr, icebergSchema);
        }
        // Handle IsNull (IS NOT NULL is represented as Not(IsNull(...)))
        if (nereidsExpr instanceof IsNull) {
            return convertIsNull((IsNull) nereidsExpr, icebergSchema);
        }
        // Handle BooleanLiteral (TRUE/FALSE from predicate rewriting)
        if (nereidsExpr instanceof BooleanLiteral) {
            return ((BooleanLiteral) nereidsExpr).getValue()
                    ? Expressions.alwaysTrue() : Expressions.alwaysFalse();
        }
        // Unsupported expression type; can't convert
        return null;
    }

    private org.apache.iceberg.expressions.Expression convertComparisonPredicate(
            ComparisonPredicate cp, Schema icebergSchema) {
        // Determine operand order: slotRef is the column, literal is the value.
        // reversed=true means the literal appears on the LEFT side (e.g., 'a' > col).
        SlotReference slotRef;
        Literal literal;
        boolean reversed;
        if (cp.left() instanceof SlotReference && cp.right() instanceof Literal) {
            slotRef = (SlotReference) cp.left();
            literal = (Literal) cp.right();
            reversed = false;
        } else if (cp.right() instanceof SlotReference && cp.left() instanceof Literal) {
            slotRef = (SlotReference) cp.right();
            literal = (Literal) cp.left();
            reversed = true;
        } else {
            return null;
        }
        return convertComparison(cp, slotRef, literal, icebergSchema, reversed);
    }

    private org.apache.iceberg.expressions.Expression convertComparison(
            ComparisonPredicate cp, SlotReference slotRef, Literal literal,
            Schema icebergSchema, boolean reversed) {
        String colName = slotRef.getName();
        // Look up the column in the Iceberg schema to get the correct type
        Types.NestedField field = icebergSchema.caseInsensitiveFindField(colName);
        if (field == null) {
            return null;
        }
        colName = field.name();
        Object value = extractIcebergLiteralValue(field.type(), literal);
        if (value == null) {
            return null;
        }

        if (cp instanceof org.apache.doris.nereids.trees.expressions.EqualTo
                || cp instanceof org.apache.doris.nereids.trees.expressions.NullSafeEqual) {
            return Expressions.equal(colName, value);
        }
        if (cp instanceof org.apache.doris.nereids.trees.expressions.GreaterThan) {
            return reversed ? Expressions.lessThan(colName, value)
                    : Expressions.greaterThan(colName, value);
        }
        if (cp instanceof org.apache.doris.nereids.trees.expressions.GreaterThanEqual) {
            return reversed ? Expressions.lessThanOrEqual(colName, value)
                    : Expressions.greaterThanOrEqual(colName, value);
        }
        if (cp instanceof org.apache.doris.nereids.trees.expressions.LessThan) {
            return reversed ? Expressions.greaterThan(colName, value)
                    : Expressions.lessThan(colName, value);
        }
        if (cp instanceof org.apache.doris.nereids.trees.expressions.LessThanEqual) {
            return reversed ? Expressions.greaterThanOrEqual(colName, value)
                    : Expressions.lessThanOrEqual(colName, value);
        }
        return null;
    }

    private org.apache.iceberg.expressions.Expression convertInPredicate(
            InPredicate inPredicate, Schema icebergSchema) {
        if (!(inPredicate.getCompareExpr() instanceof SlotReference)) {
            return null;
        }
        SlotReference slotRef = (SlotReference) inPredicate.getCompareExpr();
        String colName = slotRef.getName();
        Types.NestedField field = icebergSchema.caseInsensitiveFindField(colName);
        if (field == null) {
            return null;
        }
        colName = field.name();
        List<Object> valueList = new ArrayList<>();
        for (Expression opt : inPredicate.getOptions()) {
            if (!(opt instanceof Literal)) {
                return null;
            }
            Object value = extractIcebergLiteralValue(field.type(), (Literal) opt);
            if (value == null) {
                return null;
            }
            valueList.add(value);
        }
        return Expressions.in(colName, valueList);
    }

    private org.apache.iceberg.expressions.Expression convertIsNull(
            IsNull isNull, Schema icebergSchema) {
        if (!(isNull.child() instanceof SlotReference)) {
            return null;
        }
        SlotReference slotRef = (SlotReference) isNull.child();
        String colName = slotRef.getName();
        Types.NestedField field = icebergSchema.caseInsensitiveFindField(colName);
        if (field == null) {
            return null;
        }
        return Expressions.isNull(field.name());
    }

    /**
     * Extract a Java value compatible with the Iceberg type from a Nereids Literal.
     * Returns null if the literal cannot be converted to the target Iceberg type.
     */
    private Object extractIcebergLiteralValue(org.apache.iceberg.types.Type icebergType, Literal literal) {
        if (literal instanceof NullLiteral) {
            return null; // NullLiteral can't be used directly in Iceberg expressions
        }
        switch (icebergType.typeId()) {
            case BOOLEAN:
                if (literal instanceof BooleanLiteral) {
                    return ((BooleanLiteral) literal).getValue();
                }
                return null;
            case INTEGER:
                if (literal instanceof IntegerLikeLiteral) {
                    return (int) ((IntegerLikeLiteral) literal).getLongValue();
                }
                return null;
            case LONG:
                if (literal instanceof IntegerLikeLiteral) {
                    return ((IntegerLikeLiteral) literal).getLongValue();
                }
                return null;
            case FLOAT:
                if (literal instanceof org.apache.doris.nereids.trees.expressions.literal.FloatLiteral) {
                    return ((org.apache.doris.nereids.trees.expressions.literal.FloatLiteral) literal)
                            .getValue();
                }
                return null;
            case DOUBLE:
                if (literal instanceof org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral) {
                    return ((org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral) literal)
                            .getValue();
                }
                if (literal instanceof org.apache.doris.nereids.trees.expressions.literal.FloatLiteral) {
                    return (double) ((org.apache.doris.nereids.trees.expressions.literal.FloatLiteral) literal)
                            .getValue();
                }
                return null;
            case STRING:
                if (literal instanceof VarcharLiteral) {
                    return ((VarcharLiteral) literal).getValue();
                }
                if (literal instanceof org.apache.doris.nereids.trees.expressions.literal.StringLiteral) {
                    return ((org.apache.doris.nereids.trees.expressions.literal.StringLiteral) literal)
                            .getValue();
                }
                // Fallback: use toString() for any string-like literal
                return literal.toLegacyLiteral().getStringValue();
            case DATE:
                if (literal instanceof DateLiteral) {
                    DateLiteral dl = (DateLiteral) literal;
                    return (int) java.time.LocalDate.of(
                            (int) dl.getYear(), (int) dl.getMonth(), (int) dl.getDay())
                            .toEpochDay();
                }
                if (literal instanceof DateV2Literal) {
                    DateV2Literal dv2 = (DateV2Literal) literal;
                    return (int) java.time.LocalDate.of(
                            (int) dv2.getYear(), (int) dv2.getMonth(), (int) dv2.getDay())
                            .toEpochDay();
                }
                return null;
            case TIMESTAMP:
                if (literal instanceof DateTimeLiteral) {
                    DateTimeLiteral dt = (DateTimeLiteral) literal;
                    java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
                            (int) dt.getYear(), (int) dt.getMonth(), (int) dt.getDay(),
                            (int) dt.getHour(), (int) dt.getMinute(), (int) dt.getSecond());
                    return ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L
                            + dt.getMicroSecond();
                }
                if (literal instanceof DateTimeV2Literal) {
                    DateTimeV2Literal dt2 = (DateTimeV2Literal) literal;
                    java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
                            (int) dt2.getYear(), (int) dt2.getMonth(), (int) dt2.getDay(),
                            (int) dt2.getHour(), (int) dt2.getMinute(), (int) dt2.getSecond());
                    return ldt.toEpochSecond(java.time.ZoneOffset.UTC) * 1_000_000L
                            + dt2.getMicroSecond();
                }
                return null;
            case DECIMAL:
                if (literal instanceof org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral
                        || literal instanceof org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal) {
                    return new BigDecimal(literal.toLegacyLiteral().getStringValue());
                }
                return null;
            default:
                return null;
        }
    }

    // ---- Identity-only partition helper methods (used by parent class path) ----

    /**
     * Get the indices of identity partition fields within the full partition spec.
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
                return;
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
