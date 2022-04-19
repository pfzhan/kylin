/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.tool.bisync;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.NAggregationGroup;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.tool.bisync.model.ColumnDef;
import io.kyligence.kap.tool.bisync.model.JoinTreeNode;
import io.kyligence.kap.tool.bisync.model.MeasureDef;
import io.kyligence.kap.tool.bisync.model.SyncModel;

public class SyncModelBuilder {

    private final SyncContext syncContext;

    public SyncModelBuilder(SyncContext syncContext) {
        this.syncContext = syncContext;
    }

    public SyncModel buildSourceSyncModel() {
        NDataModel dataModelDesc = syncContext.getDataflow().getModel();
        IndexPlan indexPlan = syncContext.getDataflow().getIndexPlan();

        // init joinTree, dimension cols, measure cols, hierarchies
        Map<String, ColumnDef> columnDefMap = getAllColumns(dataModelDesc);

        List<MeasureDef> measureDefs = dataModelDesc.getEffectiveMeasures().values().stream().map(MeasureDef::new)
                .collect(Collectors.toList());
        markIndexedColumnsAndMeasures(columnDefMap, measureDefs, indexPlan, syncContext.getModelElement());
        markComputedColumnVisibility(columnDefMap, measureDefs, syncContext.getKylinConfig().exposeComputedColumn());
        Set<String[]> hierarchies = getHierarchies(indexPlan);
        JoinTreeNode joinTree = generateJoinTree(dataModelDesc.getJoinTables(), dataModelDesc.getRootFactTableName());

        return getSyncModel(dataModelDesc, columnDefMap, measureDefs, hierarchies, joinTree);
    }

    public SyncModel buildHasPermissionSourceSyncModel(Set<String> authTables, Set<String> authColumns) {
        NDataModel dataModelDesc = syncContext.getDataflow().getModel();
        IndexPlan indexPlan = syncContext.getDataflow().getIndexPlan();

        Set<String> allAuthColumns = addHasPermissionCCColumn(dataModelDesc, authColumns);
        // init joinTree, dimension cols, measure cols, hierarchies
        Map<String, ColumnDef> columnDefMap = authColumns(dataModelDesc, authTables, allAuthColumns);

        List<MeasureDef> measureDefs = dataModelDesc.getEffectiveMeasures().values().stream()
                .filter(measure -> checkMeasurePermission(allAuthColumns, measure)).map(MeasureDef::new)
                .collect(Collectors.toList());
        markHasPermissionIndexedColumnsAndMeasures(columnDefMap, measureDefs, indexPlan, syncContext.getModelElement(),
                allAuthColumns);
        markComputedColumnVisibility(columnDefMap, measureDefs, syncContext.getKylinConfig().exposeComputedColumn());
        Set<String[]> hierarchies = getHierarchies(indexPlan).stream()
                .map(hierarchyArray -> Arrays.stream(hierarchyArray).filter(renameColumnName(allAuthColumns)::contains)
                        .collect(Collectors.toSet()).toArray(new String[0]))
                .collect(Collectors.toSet()).stream().filter(x -> !Arrays.asList(x).isEmpty())
                .collect(Collectors.toSet());

        JoinTreeNode joinTree = generateJoinTree(dataModelDesc.getJoinTables(), dataModelDesc.getRootFactTableName());
        return getSyncModel(dataModelDesc, columnDefMap, measureDefs, hierarchies, joinTree);
    }

    private SyncModel getSyncModel(NDataModel dataModelDesc, Map<String, ColumnDef> columnDefMap,
            List<MeasureDef> measureDefs, Set<String[]> hierarchies, JoinTreeNode joinTree) {
        // populate CubeSyncModel
        SyncModel syncModel = new SyncModel();
        syncModel.setColumnDefMap(columnDefMap);
        syncModel.setJoinTree(joinTree);
        syncModel.setMetrics(measureDefs);
        syncModel.setHierarchies(hierarchies);
        syncModel.setProjectName(syncContext.getProjectName());
        syncModel.setModelName(dataModelDesc.getAlias());
        syncModel.setHost(syncContext.getHost());
        syncModel.setPort(String.valueOf(syncContext.getPort()));
        return syncModel;
    }

    private boolean checkMeasurePermission(Set<String> columns, NDataModel.Measure measure) {
        Set<String> measureColumns = measure.getFunction().getParameters().stream()
                .filter(parameterDesc -> parameterDesc.getColRef() != null)
                .map(parameterDesc -> parameterDesc.getColRef().getCanonicalName()).collect(Collectors.toSet());
        return columns.containsAll(measureColumns);
    }

    private void markComputedColumnVisibility(Map<String, ColumnDef> columnDefMap, List<MeasureDef> measureDefs,
            boolean exposeComputedColumns) {
        if (!exposeComputedColumns) {
            // hide all CC cols and related measures
            for (ColumnDef columnDef : columnDefMap.values()) {
                if (columnDef.isComputedColumn()) {
                    columnDef.setHidden(true);
                }
            }
            for (MeasureDef measureDef : measureDefs) {
                for (TblColRef paramColRef : measureDef.getMeasure().getFunction().getColRefs()) {
                    if (columnDefMap.get(paramColRef.getAliasDotName()).isComputedColumn()) {
                        measureDef.setHidden(true);
                        break;
                    }
                }
            }
        }
    }

    private void markIndexedColumnsAndMeasures(Map<String, ColumnDef> columnDefMap, List<MeasureDef> measureDefs,
            IndexPlan indexPlan, SyncContext.ModelElement modelElement) {
        Set<String> colsToShow = new HashSet<>();
        switch (modelElement) {
        case AGG_INDEX_COL:
            ImmutableBitSet aggDimBitSet = indexPlan.getAllIndexes().stream().filter(index -> !index.isTableIndex())
                    .map(IndexEntity::getDimensionBitset).reduce(ImmutableBitSet.EMPTY, ImmutableBitSet::or);
            Set<TblColRef> tblColRefs = indexPlan.getEffectiveDimCols().entrySet().stream()
                    .filter(entry -> aggDimBitSet.get(entry.getKey())).map(Map.Entry::getValue)
                    .collect(Collectors.toSet());
            colsToShow = tblColRefs.stream().map(TblColRef::getAliasDotName).collect(Collectors.toSet());
            break;
        case AGG_INDEX_AND_TABLE_INDEX_COL:
            colsToShow = indexPlan.getEffectiveDimCols().values().stream().map(TblColRef::getAliasDotName)
                    .collect(Collectors.toSet());
            break;
        case ALL_COLS:
            colsToShow = indexPlan.getModel().getDimensionNameIdMap().keySet();
            break;
        default:
            break;
        }

        colsToShow.forEach(colToShow -> columnDefMap.get(colToShow).setHidden(false));
        measureDefs.forEach(measureDef -> measureDef.setHidden(false));
    }

    private void markHasPermissionIndexedColumnsAndMeasures(Map<String, ColumnDef> columnDefMap,
            List<MeasureDef> measureDefs, IndexPlan indexPlan, SyncContext.ModelElement modelElement,
            Set<String> columns) {
        Set<String> colsToShow = Sets.newHashSet();
        switch (modelElement) {
        case AGG_INDEX_COL:
            ImmutableBitSet aggDimBitSet = indexPlan.getAllIndexes().stream().filter(index -> !index.isTableIndex())
                    .map(IndexEntity::getDimensionBitset).reduce(ImmutableBitSet.EMPTY, ImmutableBitSet::or);
            Set<TblColRef> tblColRefs = indexPlan.getEffectiveDimCols().entrySet().stream()
                    .filter(entry -> aggDimBitSet.get(entry.getKey())).map(Map.Entry::getValue)
                    .collect(Collectors.toSet());
            colsToShow = tblColRefs.stream().filter(column -> columns.contains(column.getCanonicalName()))
                    .map(TblColRef::getAliasDotName).collect(Collectors.toSet());
            break;
        case AGG_INDEX_AND_TABLE_INDEX_COL:
            colsToShow = indexPlan.getEffectiveDimCols().values().stream()
                    .filter(column -> columns.contains(column.getCanonicalName())).map(TblColRef::getAliasDotName)
                    .collect(Collectors.toSet());
            break;
        case ALL_COLS:
            colsToShow = indexPlan.getModel().getDimensionNameIdMap().keySet().stream()
                    .filter(renameColumnName(columns)::contains)
                    .collect(Collectors.toSet());
            break;
        default:
            break;
        }

        colsToShow.forEach(colToShow -> columnDefMap.get(colToShow).setHidden(false));
        measureDefs.forEach(measureDef -> measureDef.setHidden(false));
    }

    Set<String> renameColumnName(Set<String> columns) {
        return columns.stream().map(x -> {
            String[] split = x.split("\\.");
            if (split.length == 3) {
                return split[1] + "." + split[2];
            }
            return x;
        }).collect(Collectors.toSet());
    }

    private Map<String, ColumnDef> getAllColumns(NDataModel modelDesc) {
        Map<String, ColumnDef> modelColsMap = new HashMap<>();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            for (TblColRef column : tableRef.getColumns()) {
                ColumnDef columnDef = new ColumnDef("dimension", tableRef.getAlias(), null, column.getName(),
                        column.getDatatype(), true, column.getColumnDesc().isComputedColumn());
                String colName = tableRef.getAlias() + "." + column.getName();
                modelColsMap.put(colName, columnDef);
            }
        }

        // sync col alias
        for (NDataModel.NamedColumn namedColumn : modelDesc.getAllNamedColumns()) {
            if (modelColsMap.get(namedColumn.getAliasDotColumn()) != null) {
                modelColsMap.get(namedColumn.getAliasDotColumn()).setColumnAlias(namedColumn.getName());
            }
        }
        return modelColsMap;
    }

    private Map<String, ColumnDef> authColumns(NDataModel modelDesc, Set<String> tables, Set<String> columns) {
        Map<String, ColumnDef> modelColsMap = Maps.newHashMap();
        modelDesc.getAllTables().stream().filter(table -> tables.contains(table.getTableIdentity()))
                .forEach(tableRef -> tableRef.getColumns().stream()
                        .filter(column -> columns.contains(column.getCanonicalName())).forEach(column -> {
                            ColumnDef columnDef = new ColumnDef("dimension", tableRef.getAlias(), null,
                                    column.getName(), column.getDatatype(), true,
                                    column.getColumnDesc().isComputedColumn());
                            String colName = tableRef.getAlias() + "." + column.getName();
                            modelColsMap.put(colName, columnDef);
                        }));

        // sync col alias
        modelDesc.getAllNamedColumns().stream()
                .filter(namedColumn -> modelColsMap.get(namedColumn.getAliasDotColumn()) != null)
                .forEach(namedColumn -> modelColsMap.get(namedColumn.getAliasDotColumn())
                        .setColumnAlias(namedColumn.getName()));
        return modelColsMap;
    }

    private Set<String> addHasPermissionCCColumn(NDataModel modelDesc, Set<String> columns) {
        Set<String> allAuthColumns = Sets.newHashSet();
        allAuthColumns.addAll(columns);
        List<ComputedColumnDesc> computedColumnDescs = modelDesc.getComputedColumnDescs();
        Set<ComputedColumnDesc> computedColumnDescSet = computedColumnDescs.stream().filter(computedColumnDesc -> {
            Set<String> ccUsedColsWithModel = ComputedColumnUtil.getCCUsedColsWithModel(modelDesc, computedColumnDesc);
            return columns.containsAll(ccUsedColsWithModel);
        }).collect(Collectors.toSet());
        computedColumnDescSet.forEach(cc -> allAuthColumns.add(cc.getIdentName()));
        return allAuthColumns;
    }

    private Set<String[]> getHierarchies(IndexPlan indexPlan) {
        Set<String[]> hierarchies = Sets.newHashSet();
        if (indexPlan.getRuleBasedIndex() == null) {
            return hierarchies;
        }

        Set<String> hierarchyNameSet = Sets.newHashSet();
        for (NAggregationGroup group : indexPlan.getRuleBasedIndex().getAggregationGroups()) {
            SelectRule rule = group.getSelectRule();
            if (rule != null) {
                for (Integer[] hierarchyIds : rule.hierarchyDims) {
                    if (hierarchyIds != null && hierarchyIds.length != 0) {

                        String[] hierarchyNames = Arrays.stream(hierarchyIds)
                                .map(id -> indexPlan.getModel().getColumnNameByColumnId(id)).toArray(String[]::new);
                        String hierarchyNamesJoined = String.join(",", hierarchyNames);
                        if (!hierarchyNameSet.contains(hierarchyNamesJoined)) {
                            hierarchies.add(hierarchyNames);
                            hierarchyNameSet.add(hierarchyNamesJoined);
                        }
                    }
                }
            }
        }
        return hierarchies;
    }

    private JoinTreeNode generateJoinTree(List<JoinTableDesc> joinTables, String factTable) {
        Map<String, List<JoinTableDesc>> joinTreeMap = new HashMap<>();
        for (JoinTableDesc joinTable : joinTables) {
            String[] fks = joinTable.getJoin().getForeignKey();
            String leftTableName = fks[0].substring(0, fks[0].indexOf('.'));
            if (joinTreeMap.containsKey(leftTableName)) {
                joinTreeMap.get(leftTableName).add(joinTable);
            } else {
                List<JoinTableDesc> rightTables = new LinkedList<>();
                rightTables.add(joinTable);
                joinTreeMap.put(leftTableName, rightTables);
            }
        }
        return createJoinTree(factTable, joinTreeMap);
    }

    private JoinTreeNode createJoinTree(String factTableName, Map<String, List<JoinTableDesc>> joinTreeMap) {
        JoinTableDesc factTable = new JoinTableDesc();
        int dot = factTableName.indexOf('.');
        String alias = factTableName.substring(dot + 1);
        factTable.setTable(factTableName);
        factTable.setAlias(alias);
        factTable.setKind(NDataModel.TableKind.FACT);
        return buildChildJoinTree(factTable, joinTreeMap);
    }

    private JoinTreeNode buildChildJoinTree(JoinTableDesc root, Map<String, List<JoinTableDesc>> joinTreeMap) {
        JoinTreeNode joinTree = new JoinTreeNode();
        joinTree.setValue(root);
        List<JoinTableDesc> childTables = joinTreeMap.get(root.getAlias());
        if (childTables != null) {
            List<JoinTreeNode> childNodes = new LinkedList<>();
            for (JoinTableDesc childTable : childTables) {
                childNodes.add(buildChildJoinTree(childTable, joinTreeMap));
            }
            joinTree.setChildNodes(childNodes);
        }
        return joinTree;
    }
}
