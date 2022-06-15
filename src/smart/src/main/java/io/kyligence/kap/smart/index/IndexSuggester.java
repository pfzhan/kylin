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

package io.kyligence.kap.smart.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.ChooserContext;
import io.kyligence.kap.metadata.cube.cuboid.ComparatorUtils;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexEntity.IndexIdentifier;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.exception.NotSupportedSQLException;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AccelerateInfo.QueryLayoutRelation;
import io.kyligence.kap.smart.exception.PendingException;
import io.kyligence.kap.smart.model.ModelTree;
import io.kyligence.kap.smart.util.CubeUtils;
import io.kyligence.kap.smart.util.EntityBuilder;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class IndexSuggester {

    private static final String COLUMN_NOT_FOUND_PTN = "The model [%s] matches this query, but the dimension [%s] is missing. ";
    private static final String MEASURE_NOT_FOUND_PTN = "The model [%s] matches this query, but the measure [%s] is missing. ";
    private static final String JOIN_NOT_MATCHED = "The join of model [%s] has some difference with the joins of this query. ";
    private static final String COMPUTED_COLUMN_ON_EXCLUDED_LOOKUP_TABLE = "Computed column depends on excluded lookup table, stop the process of generate index.";
    private static final String MEASURE_ON_EXCLUDED_LOOKUP_TABLE = "Unsupported measure on dimension table, stop the process of generate index. ";

    private final AbstractContext proposeContext;
    private final AbstractContext.ModelContext modelContext;
    private final IndexPlan indexPlan;
    private final NDataModel model;

    private final Map<FunctionDesc, Integer> aggFuncIdMap;
    private final Map<IndexIdentifier, IndexEntity> collector;
    private final SortedSet<Long> layoutIds = Sets.newTreeSet();

    IndexSuggester(AbstractContext.ModelContext modelContext, IndexPlan indexPlan,
            Map<IndexIdentifier, IndexEntity> collector) {

        this.modelContext = modelContext;
        this.proposeContext = modelContext.getProposeContext();
        this.model = modelContext.getTargetModel();
        this.indexPlan = indexPlan;
        this.collector = collector;

        aggFuncIdMap = Maps.newHashMap();
        model.getEffectiveMeasures().forEach((measureId, measure) -> {
            FunctionDesc function = measure.getFunction();
            aggFuncIdMap.put(function, measureId);
        });

        collector.forEach((indexIdentifier, indexEntity) -> indexEntity.getLayouts()
                .forEach(layout -> layoutIds.add(layout.getId())));
    }

    void suggestIndexes(ModelTree modelTree) {
        final Map<String, AccelerateInfo> sql2AccelerateInfo = proposeContext.getAccelerateInfoMap();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {

            // check keySet of sql2AccelerateInfo contains ctx.sql
            AccelerateInfo accelerateInfo = sql2AccelerateInfo.get(ctx.sql);
            Preconditions.checkNotNull(accelerateInfo);
            if (accelerateInfo.isNotSucceed()) {
                continue;
            }

            try {
                if (CollectionUtils.isNotEmpty(ctx.getContainedNotSupportedFunc())) {
                    throw new NotSupportedSQLException(
                            StringUtils.join(ctx.getContainedNotSupportedFunc(), ", ") + " function not supported");
                }

                Map<String, String> aliasMap = RealizationChooser.matchJoins(model, ctx);
                if (MapUtils.isEmpty(aliasMap)) {
                    throw new PendingException(String.format(Locale.ROOT,
                            getMsgTemplateByModelMaintainType(JOIN_NOT_MATCHED, Type.TABLE), model.getAlias()));
                }
                ctx.fixModel(model, aliasMap);
                QueryLayoutRelation queryLayoutRelation = ingest(ctx, model);
                accelerateInfo.getRelatedLayouts().add(queryLayoutRelation);
            } catch (Exception e) {
                log.error("Unable to suggest cuboid for IndexPlan", e);
                // under expert mode
                if (e instanceof PendingException) {
                    accelerateInfo.setPendingMsg(e.getMessage());
                } else {
                    accelerateInfo.setFailedCause(e);
                }
                accelerateInfo.getRelatedLayouts().clear();
            } finally {
                ctx.unfixModel();
            }
        }
    }

    private List<Integer> suggestShardBy(List<Integer> sortedDimIds) {
        // for recommend dims order by filterLevel and Cardinality
        List<Integer> shardBy = Lists.newArrayList();
        if (CollectionUtils.isEmpty(sortedDimIds))
            return shardBy;

        TblColRef colRef = model.getEffectiveCols().get(sortedDimIds.get(0));
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                proposeContext.getProject());
        TableExtDesc.ColumnStats colStats = TableExtDesc.ColumnStats.getColumnStats(tableMgr, colRef);
        if (colStats != null
                && colStats.getCardinality() > proposeContext.getSmartConfig().getRowkeyUHCCardinalityMin()) {
            shardBy.add(sortedDimIds.get(0));
        }
        return shardBy;
    }

    private QueryLayoutRelation ingest(OLAPContext ctx, NDataModel model) {

        List<Integer> dimIds;
        SortedSet<Integer> measureIds;
        IndexEntity indexEntity;
        if (ctx.getSQLDigest().isRawQuery) {
            dimIds = suggestTableIndexDimensions(ctx);
            measureIds = Sets.newTreeSet();
            indexEntity = createIndexEntity(suggestDescId(true), dimIds, measureIds);
        } else {
            dimIds = suggestAggIndexDimensions(ctx);
            measureIds = suggestMeasures(dimIds, ctx);
            indexEntity = createIndexEntity(suggestDescId(false), dimIds, measureIds);
        }

        final IndexIdentifier cuboidIdentifier = indexEntity.createIndexIdentifier();
        if (collector.containsKey(cuboidIdentifier)) {
            indexEntity = collector.get(cuboidIdentifier);
        } else {
            collector.put(cuboidIdentifier, indexEntity);
        }

        LayoutEntity layout = new EntityBuilder.LayoutEntityBuilder(suggestLayoutId(indexEntity), indexEntity)
                .colOrderIds(suggestColOrder(dimIds, measureIds, Lists.newArrayList())).isAuto(true).build();
        layout.setInProposing(true);
        if (model.getStorageType() == 2 && dimIds.containsAll(indexPlan.getExtendPartitionColumns())) {
            layout.setPartitionByColumns(indexPlan.getExtendPartitionColumns());
        }
        if (!indexEntity.isTableIndex() && CollectionUtils.isNotEmpty(indexPlan.getAggShardByColumns())
                && layout.getColOrder().containsAll(indexPlan.getAggShardByColumns())) {
            layout.setShardByColumns(indexPlan.getAggShardByColumns());
        } else if (isQualifiedSuggestShardBy(ctx)) {
            layout.setShardByColumns(suggestShardBy(dimIds));
        }

        String modelId = model.getUuid();
        int semanticVersion = model.getSemanticVersion();
        for (LayoutEntity l : indexEntity.getLayouts()) {
            List<Integer> sortByColumns = layout.getSortByColumns();
            layout.setSortByColumns(l.getSortByColumns());
            if (l.equals(layout)) {
                return new QueryLayoutRelation(ctx.sql, modelId, l.getId(), semanticVersion);
            }
            layout.setSortByColumns(sortByColumns);
        }

        indexEntity.getLayouts().add(layout);
        layoutIds.add(layout.getId());
        modelContext.gatherLayoutRecItem(layout);

        return new QueryLayoutRelation(ctx.sql, modelId, layout.getId(), semanticVersion);
    }

    private boolean isQualifiedSuggestShardBy(OLAPContext context) {
        for (TblColRef colRef : context.getSQLDigest().filterColumns) {
            if (TblColRef.FilterColEnum.EQUAL_FILTER == colRef.getFilterLevel()) {
                return true;
            }
        }
        return false;
    }

    private List<Integer> suggestTableIndexDimensions(OLAPContext context) {
        // 1. determine filter columns and non-filter columns
        Set<TblColRef> filterColumns = Sets.newHashSet(context.filterColumns);
        Set<TblColRef> nonFilterColumnSet = new HashSet<>(context.allColumns);
        nonFilterColumnSet.addAll(context.getSubqueryJoinParticipants());

        // 2. add extra non-filter column if necessary
        // no column selected in raw query , select the first column of the model (i.e. select 1 from table1)
        if (nonFilterColumnSet.isEmpty() && context.filterColumns.isEmpty()) {
            Preconditions.checkState(CollectionUtils.isNotEmpty(model.getAllNamedColumns()),
                    "Cannot suggest any columns in table index.");
            NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().stream().filter(column -> {
                TblColRef tblColRef = model.getEffectiveCols().get(column.getId());
                return tblColRef.getTable().equalsIgnoreCase(model.getRootFactTableName());
            }).findFirst().get();

            nonFilterColumnSet.add(model.getEffectiveCols().get(namedColumn.getId()));
            // set extra-column as dimension in model
            namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        }
        nonFilterColumnSet.removeAll(context.filterColumns);
        replaceDimOfLookupTableWithFK(context, filterColumns, nonFilterColumnSet);

        // 3. sort filter columns and non-filter columns
        List<TblColRef> sortedDims = sortDimensionColumns(filterColumns, nonFilterColumnSet);

        // 4. generate dimension ids
        return generateDimensionIds(sortedDims, model.getEffectiveCols().inverse());
    }

    private List<Integer> suggestAggIndexDimensions(OLAPContext context) {
        // 1. determine filter columns and non-filter columns
        Set<TblColRef> filterColumns = Sets.newHashSet(context.filterColumns);
        Set<TblColRef> nonFilterColumnSet = new HashSet<>(context.getGroupByColumns());
        nonFilterColumnSet.addAll(context.getSubqueryJoinParticipants());
        nonFilterColumnSet.removeAll(context.filterColumns);
        replaceDimOfLookupTableWithFK(context, filterColumns, nonFilterColumnSet);

        // 2. sort filter columns and non-filter columns
        List<TblColRef> sortedDims = sortDimensionColumns(filterColumns, nonFilterColumnSet);

        // 3. generate dimension ids
        return generateDimensionIds(sortedDims, model.getEffectiveDimensions().inverse());
    }

    private void replaceDimOfLookupTableWithFK(OLAPContext context, Set<TblColRef> filterColumns,
            Set<TblColRef> nonFilterColumnSet) {
        ExcludedLookupChecker checker = modelContext.getChecker();
        AtomicBoolean isAnyCCDependsLookupTable = new AtomicBoolean();
        filterColumns.removeIf(tblColRef -> {
            boolean dependsLookupTable = checker.isColRefDependsLookupTable(tblColRef);
            if (tblColRef.getColumnDesc().isComputedColumn() && dependsLookupTable) {
                isAnyCCDependsLookupTable.set(true);
            }
            return dependsLookupTable;
        });
        nonFilterColumnSet.removeIf(tblColRef -> {
            boolean dependsLookupTable = checker.isColRefDependsLookupTable(tblColRef);
            if (tblColRef.getColumnDesc().isComputedColumn() && dependsLookupTable) {
                isAnyCCDependsLookupTable.set(true);
            }
            return dependsLookupTable;
        });
        if (isAnyCCDependsLookupTable.get()) {
            throw new PendingException(COMPUTED_COLUMN_ON_EXCLUDED_LOOKUP_TABLE);
        }

        // foreign key column as non-filter column
        Preconditions.checkNotNull(checker, "ExcludedLookupChecker is not prepared yet.");
        Map<String, TblColRef> fKAsDimensionMap = context.collectFKAsDimensionMap(checker);
        fKAsDimensionMap.forEach((name, tblColRef) -> {
            if (!filterColumns.contains(tblColRef)) {
                nonFilterColumnSet.add(tblColRef);
            }
        });
    }

    private List<TblColRef> sortDimensionColumns(Collection<TblColRef> filterColumnsCollection,
            Collection<TblColRef> nonFilterColumnsCollection) {
        List<TblColRef> filterColumns = new ArrayList<>(filterColumnsCollection);
        List<TblColRef> nonFilterColumns = new ArrayList<>(nonFilterColumnsCollection);

        val chooserContext = new ChooserContext(model);
        val filterColComparator = ComparatorUtils.filterColComparator(chooserContext);
        filterColumns.sort(filterColComparator);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());

        List<TblColRef> result = new LinkedList<>(filterColumns);
        result.addAll(nonFilterColumns);
        return result;
    }

    private List<Integer> generateDimensionIds(List<TblColRef> dimCols, ImmutableBiMap<TblColRef, Integer> colIdMap) {
        return dimCols.stream().map(dimCol -> {
            if (colIdMap.get(dimCol) == null) {
                throw new PendingException(
                        String.format(Locale.ROOT, getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN, Type.COLUMN),
                                model.getAlias(), dimCol.getIdentity()));
            }
            return colIdMap.get(dimCol);
        }).collect(Collectors.toList());
    }

    private SortedSet<Integer> suggestMeasures(List<Integer> dimIds, OLAPContext ctx) {
        Map<TblColRef, Integer> colIdMap = model.getEffectiveDimensions().inverse();
        SortedSet<Integer> measureIds = Sets.newTreeSet();
        // Add default measure count(1)
        measureIds.add(calcCountOneMeasureId());

        ctx.aggregations.forEach(aggFunc -> {
            Integer measureId = aggFuncIdMap.get(aggFunc);
            if (modelContext.getChecker().isMeasureOnLookupTable(aggFunc)) {
                throw new PendingException(MEASURE_ON_EXCLUDED_LOOKUP_TABLE + aggFunc);
            }
            if (measureId != null) {
                measureIds.add(measureId);
            } else if (CollectionUtils.isNotEmpty(aggFunc.getParameters())) {
                if (CubeUtils.isValidMeasure(aggFunc)) {
                    String measure = String.format(Locale.ROOT, "%s(%s)", aggFunc.getExpression(),
                            aggFunc.getParameters());
                    for (TblColRef tblColRef : aggFunc.getColRefs()) {
                        if (colIdMap.get(tblColRef) == null) {
                            throw new PendingException(String.format(Locale.ROOT,
                                    getMsgTemplateByModelMaintainType(MEASURE_NOT_FOUND_PTN, Type.MEASURE),
                                    model.getAlias(), measure));
                        }
                    }
                } else if (aggFunc.canAnsweredByDimensionAsMeasure()) {
                    List<Integer> newDimIds = generateDimensionIds(Lists.newArrayList(aggFunc.getSourceColRefs()),
                            model.getEffectiveDimensions().inverse());
                    newDimIds.removeIf(dimIds::contains);
                    dimIds.addAll(newDimIds);
                }
            }
        });
        return measureIds;
    }

    /**
     * By default, we will add a count(1) measure in our model if agg appears.
     * If current model without measures, the id of count one measure is 10_000;
     * else find existing count one measure's id.
     */
    private Integer calcCountOneMeasureId() {
        Integer countOne = aggFuncIdMap.get(FunctionDesc.newCountOne());
        return countOne == null ? NDataModel.MEASURE_ID_BASE : countOne;
    }

    private String getMsgTemplateByModelMaintainType(String messagePattern, Type type) {
        Preconditions.checkNotNull(model);
        String suggestion;
        if (type == Type.COLUMN) {
            suggestion = "Please add the above dimension before attempting to accelerate this query.";
        } else if (type == Type.MEASURE) {
            suggestion = "Please add the above measure before attempting to accelerate this query.";
        } else {
            suggestion = "Please adjust model's join to match the query.";
        }

        return messagePattern + suggestion;
    }

    private IndexEntity createIndexEntity(long id, List<Integer> dimIds, SortedSet<Integer> measureIds) {
        EntityBuilder.checkDimensionsAndMeasures(dimIds, Lists.newArrayList(measureIds));
        IndexEntity indexEntity = new IndexEntity();
        indexEntity.setId(id);
        indexEntity.setDimensions(dimIds);
        indexEntity.setMeasures(Lists.newArrayList(measureIds));
        indexEntity.setIndexPlan(indexPlan);
        return indexEntity;
    }

    private List<Integer> suggestColOrder(final List<Integer> orderedDimIds, Set<Integer> measureIds,
            List<Integer> shardBy) {
        ArrayList<Integer> copyDimension = new ArrayList<>(orderedDimIds);
        copyDimension.removeAll(shardBy);
        List<Integer> colOrder = Lists.newArrayList();
        colOrder.addAll(shardBy);
        colOrder.addAll(copyDimension);
        colOrder.addAll(measureIds);
        return colOrder;
    }

    private long suggestDescId(boolean isTableIndex) {
        return EntityBuilder.IndexEntityBuilder.findAvailableIndexEntityId(indexPlan, collector.values(), isTableIndex);
    }

    private long suggestLayoutId(IndexEntity indexEntity) {
        long s = indexEntity.getId() + indexEntity.getNextLayoutOffset();
        while (layoutIds.contains(s)) {
            s++;
        }
        return s;
    }

    private enum Type {
        TABLE, COLUMN, MEASURE
    }
}
