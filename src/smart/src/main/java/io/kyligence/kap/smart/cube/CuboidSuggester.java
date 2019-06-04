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

package io.kyligence.kap.smart.cube;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import io.kyligence.kap.smart.exception.PendingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.cuboid.ComparatorUtils;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexEntity.IndexIdentifier;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AccelerateInfo.QueryLayoutRelation;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class CuboidSuggester {

    private static final String COLUMN_NOT_FOUND_PTN = "The model [%s] matches this query, but the dimension [%s] is missing. ";
    private static final String MEASURE_NOT_FOUND_PTN = "The model [%s] matches this query, but the measure [%s] is missing. ";
    private static final String JOIN_NOT_MATCHED = "The join of model [%s] has some difference with the joins of this query. ";

    private NSmartContext smartContext;
    private IndexPlan indexPlan;
    private NDataModel model;
    private final ProjectInstance projectInstance;

    private Map<FunctionDesc, Integer> aggFuncIdMap;
    private Map<IndexIdentifier, IndexEntity> collector;
    private SortedSet<Long> cuboidLayoutIds = Sets.newTreeSet();

    CuboidSuggester(NModelContext modelContext, IndexPlan indexPlan, Map<IndexIdentifier, IndexEntity> collector) {

        this.smartContext = modelContext.getSmartContext();
        this.model = modelContext.getTargetModel();
        this.indexPlan = indexPlan;
        this.collector = collector;
        this.projectInstance = NProjectManager.getInstance(this.smartContext.getKylinConfig())
                .getProject(this.smartContext.getProject());

        aggFuncIdMap = Maps.newHashMap();
        model.getEffectiveMeasureMap()
                .forEach((measureId, measure) -> aggFuncIdMap.put(measure.getFunction(), measureId));

        collector.forEach((cuboidIdentifier, indexEntity) -> indexEntity.getLayouts()
                .forEach(layout -> cuboidLayoutIds.add(layout.getId())));
    }

    void suggestIndexes(ModelTree modelTree) {
        final Map<String, AccelerateInfo> sql2AccelerateInfo = smartContext.getAccelerateInfoMap();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {

            // check keySet of sql2AccelerateInfo contains ctx.sql
            AccelerateInfo accelerateInfo = sql2AccelerateInfo.get(ctx.sql);
            Preconditions.checkNotNull(accelerateInfo);
            if (accelerateInfo.isNotSucceed()) {
                continue;
            }

            try {
                Map<String, String> aliasMap = RealizationChooser.matchJoins(model, ctx);
                if (aliasMap == null) {
                    throw new PendingException(String.format(
                            getMsgTemplateByModelMaintainType(JOIN_NOT_MATCHED, Type.TABLE), model.getAlias()));
                }
                ctx.fixModel(model, aliasMap);
                QueryLayoutRelation queryLayoutRelation = ingest(ctx, model);
                accelerateInfo.getRelatedLayouts().add(queryLayoutRelation);
            } catch (Exception e) {
                log.error("Unable to suggest cuboid for IndexPlan", e);
                // under expert mode
                if (e instanceof PendingException
                        && projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN)
                    accelerateInfo.setPendingMsg(e.getMessage());
                else
                    accelerateInfo.setFailedCause(e);
                accelerateInfo.getRelatedLayouts().clear();
            } finally {
                ctx.unfixModel();
            }
        }
    }

    private List<Integer> suggestShardBy(Collection<Integer> dimIds) {
        List<Integer> shardBy = Lists.newArrayList();
        for (int dimId : dimIds) {
            TblColRef colRef = model.getEffectiveColsMap().get(dimId);
            TableExtDesc.ColumnStats colStats = TableExtDesc.ColumnStats
                    .getColumnStats(smartContext.getTableMetadataManager(), colRef);
            if (colStats != null
                    && colStats.getCardinality() > smartContext.getSmartConfig().getRowkeyUHCCardinalityMin()) {
                shardBy.add(dimId);
            }
        }
        return shardBy;
    }

    private List<Integer> suggestSortBy(OLAPContext ctx) {
        Map<TblColRef, Integer> colIdMap = model.getEffectiveColsMap().inverse();
        // TODO need a more proper fix
        if (CollectionUtils.isEmpty(ctx.getSortColumns())) {
            return Lists.newArrayList();
        }

        List<Integer> ret = Lists.newArrayList();
        for (TblColRef col : ctx.getSortColumns()) {
            final Integer id = colIdMap.get(col);
            if (id == null) {
                throw new PendingException(
                        String.format(getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN, Type.COLUMN),
                                model.getAlias(), col.getIdentity()));
            }
            ret.add(id);
        }
        return ret;
    }

    private QueryLayoutRelation ingest(OLAPContext ctx, NDataModel model) {

        final List<Integer> dimIds = suggestDimensions(ctx);
        boolean isTableIndex = ctx.getSQLDigest().isRawQuery;
        SortedSet<Integer> measureIds = isTableIndex ? Sets.newTreeSet() : suggestMeasures(ctx);
        IndexEntity indexEntity = createIndexEntity(dimIds, measureIds, isTableIndex);

        final IndexIdentifier cuboidIdentifier = indexEntity.createCuboidIdentifier();
        if (collector.containsKey(cuboidIdentifier)) {
            indexEntity = collector.get(cuboidIdentifier);
        } else {
            collector.put(cuboidIdentifier, indexEntity);
        }

        List<Integer> shardBy = Lists.newArrayList();
        List<Integer> sortBy = Lists.newArrayList();
        if (isTableIndex) {
            shardBy = suggestShardBy(dimIds);
            sortBy = suggestSortBy(ctx);
        }

        LayoutEntity layout = new LayoutEntity();
        layout.setId(suggestLayoutId(indexEntity));
        layout.setColOrder(suggestColOrder(dimIds, measureIds));
        layout.setIndex(indexEntity);
        layout.setShardByColumns(shardBy);
        layout.setSortByColumns(sortBy);
        layout.setAuto(true);
        layout.setUpdateTime(System.currentTimeMillis());
        layout.setDraftVersion(smartContext.getDraftVersion());

        String modelId = model.getUuid();
        int semanticVersion = model.getSemanticVersion();
        for (LayoutEntity l : indexEntity.getLayouts()) {
            if (l.equals(layout)) {
                return new QueryLayoutRelation(ctx.sql, modelId, l.getId(), semanticVersion);
            }
        }

        indexEntity.getLayouts().add(layout);
        cuboidLayoutIds.add(layout.getId());

        return new QueryLayoutRelation(ctx.sql, modelId, layout.getId(), semanticVersion);
    }

    private List<Integer> suggestDimensions(OLAPContext context) {

        // 1. determine filter columns and non-filter columns
        List<TblColRef> filterColumns = Lists.newArrayList(context.filterColumns);
        Set<TblColRef> nonFilterColumnSet = Sets.newHashSet(context.allColumns);
        nonFilterColumnSet.removeAll(context.filterColumns);
        context.aggregations.forEach(functionDesc -> {
            if (CollectionUtils.isEmpty(functionDesc.getParameters())) {
                return;
            }

            final List<TblColRef> aggCols = Lists.newArrayList(functionDesc.getColRefs());
            if (CollectionUtils.isNotEmpty(aggCols)) {
                aggCols.removeAll(context.getGroupByColumns());
                aggCols.removeAll(context.getSubqueryJoinParticipants());
                nonFilterColumnSet.removeAll(aggCols);
            }
        });

        // 2. add extra non-filter column if necessary
        boolean isTableIndex = context.getSQLDigest().isRawQuery;
        final ImmutableBiMap<Integer, TblColRef> colMap;
        if (isTableIndex) {
            colMap = model.getEffectiveColsMap();
            // no column selected in raw query , select the first column of the model (i.e. select 1 from table1)
            if (nonFilterColumnSet.isEmpty() && filterColumns.isEmpty()) {
                Preconditions.checkState(CollectionUtils.isNotEmpty(model.getAllNamedColumns()),
                        "Cannot suggest any columns in table index.");
                final NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().iterator().next();
                nonFilterColumnSet.add(colMap.get(namedColumn.getId()));
            }
        } else {
            colMap = model.getEffectiveDimenionsMap();
        }

        // 3. sort filter columns and non-filter columns
        final Comparator<TblColRef> filterColComparator = ComparatorUtils
                .filterColComparator(smartContext.getKylinConfig(), smartContext.getProject());
        filterColumns.sort(filterColComparator);
        final List<TblColRef> nonFilterColumns = Lists.newArrayList(nonFilterColumnSet);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());

        // 4. generate dimension ids
        final List<Integer> dimensions = Lists.newArrayList();
        final ImmutableBiMap<TblColRef, Integer> colIdMap = colMap.inverse();
        filterColumns.forEach(dimension -> {
            if (colIdMap.get(dimension) == null) {
                throw new PendingException(
                        String.format(getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN, Type.COLUMN),
                                model.getAlias(), dimension.getIdentity()));
            }
            dimensions.add(colIdMap.get(dimension));
        });
        nonFilterColumns.forEach(dimension -> {
            if (colIdMap.get(dimension) == null) {
                throw new PendingException(
                        String.format(getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN, Type.COLUMN),
                                model.getAlias(), dimension.getIdentity()));
            }
            dimensions.add(colIdMap.get(dimension));
        });

        return dimensions;
    }

    private SortedSet<Integer> suggestMeasures(OLAPContext ctx) {
        Map<TblColRef, Integer> colIdMap = model.getEffectiveDimenionsMap().inverse();
        SortedSet<Integer> measureIds = Sets.newTreeSet();
        // Add default measure count(1)
        measureIds.add(NDataModel.MEASURE_ID_BASE);

        ctx.aggregations.forEach(aggFunc -> {
            Integer measureId = aggFuncIdMap.get(aggFunc);
            if (measureId != null) {
                measureIds.add(measureId);
            } else if (CollectionUtils.isNotEmpty(aggFunc.getParameters())) {
                String measure = String.format("%s(%s)", aggFunc.getExpression(), aggFunc.getParameters());
                for (TblColRef tblColRef : aggFunc.getColRefs()) {
                    if (colIdMap.get(tblColRef) == null) {
                        throw new PendingException(
                                String.format(getMsgTemplateByModelMaintainType(MEASURE_NOT_FOUND_PTN, Type.MEASURE),
                                        model.getAlias(), measure));
                    }
                }
            }
        });
        return measureIds;
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

        return projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN //
                ? messagePattern + suggestion
                : messagePattern;
    }

    private IndexEntity createIndexEntity(List<Integer> dimIds, SortedSet<Integer> measureIds, boolean isTableIndex) {
        Preconditions.checkState(!dimIds.isEmpty() || !measureIds.isEmpty(),
                "Neither dimension nor measure could be proposed for indexEntity");

        IndexEntity indexEntity = new IndexEntity();
        indexEntity.setId(suggestDescId(isTableIndex));
        indexEntity.setDimensions(Lists.newArrayList(dimIds));
        indexEntity.getDimensions().sort(Integer::compareTo);
        indexEntity.setMeasures(Lists.newArrayList(measureIds));
        indexEntity.getMeasures().sort(Integer::compareTo);
        indexEntity.setIndexPlan(indexPlan);
        return indexEntity;
    }

    private List<Integer> suggestColOrder(final List<Integer> orderedDimIds, Set<Integer> measureIds) {
        List<Integer> colOrder = Lists.newArrayList();
        colOrder.addAll(orderedDimIds);
        colOrder.addAll(measureIds);
        return colOrder;
    }

    private long suggestDescId(boolean isTableIndex) {
        return findAvailableIndexEntityId(isTableIndex);
    }

    private long findAvailableIndexEntityId(boolean isTableIndex) {
        final Collection<IndexEntity> indexEntities = collector.values();
        long result = isTableIndex ? IndexEntity.TABLE_INDEX_START_ID : 0;
        List<Long> cuboidIds = Lists.newArrayList();
        for (IndexEntity indexEntity : indexEntities) {
            long indexEntityId = indexEntity.getId();
            if ((isTableIndex && indexEntityId >= IndexEntity.TABLE_INDEX_START_ID)
                    || (!isTableIndex && indexEntityId < IndexEntity.TABLE_INDEX_START_ID)) {
                cuboidIds.add(indexEntityId);
            }
        }

        if (!cuboidIds.isEmpty()) {
            // use the largest cuboid id + step
            cuboidIds.sort(Long::compareTo);
            result = cuboidIds.get(cuboidIds.size() - 1) + IndexEntity.INDEX_ID_STEP;
        }
        return Math.max(result, isTableIndex ? indexPlan.getNextTableIndexId() : indexPlan.getNextAggregationIndexId());
    }

    private long suggestLayoutId(IndexEntity indexEntity) {
        long s = indexEntity.getLastLayout() == null ? indexEntity.getId() + 1
                : indexEntity.getLastLayout().getId() + 1;
        while (cuboidLayoutIds.contains(s)) {
            s++;
        }
        return s;
    }

    private enum Type {
        TABLE, COLUMN, MEASURE
    }
}
