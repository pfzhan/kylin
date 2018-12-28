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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidDesc.NCuboidIdentifier;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AccelerateInfo.QueryLayoutRelation;
import io.kyligence.kap.smart.model.ModelTree;

class CuboidSuggester {

    private static final Logger logger = LoggerFactory.getLogger(CuboidSuggester.class);

    private static final String COLUMN_NOT_FOUND_PTN = "Column not found. Please add column [%s] into the model [%s].";
    private static final String MEASURE_NOT_FOUND_PTN = "Please add measure [%s] into model [%s] to enable system accelerate this query.";
    private static final String TABLE_NOT_MATCHED = "The join of model [%s] has some difference with the joins of this query. Please adjust model's join to match the query.";

    private class ColIndexSuggester {
        OLAPContext olapContext;

        private ColIndexSuggester(OLAPContext olapContext) {
            this.olapContext = olapContext;
        }

        private String suggest(TblColRef colRef) {
            TupleFilter filter = olapContext.filter;
            LinkedList<TupleFilter> filters = Lists.newLinkedList();
            filters.add(filter);
            while (!filters.isEmpty()) {
                TupleFilter f = filters.poll();
                if (f == null)
                    continue;
                if (f instanceof CompareTupleFilter) {
                    CompareTupleFilter cf = (CompareTupleFilter) f;
                    if (cf.isEvaluable() && cf.getColumn().getIdentity().equals(colRef.getIdentity())) {
                        switch (f.getOperator()) {
                        case GT:
                        case GTE:
                        case LT:
                        case LTE:
                            return "all";
                        default:
                            break;
                        }
                    }
                }
                if (f.getChildren() != null)
                    filters.addAll(f.getChildren());
            }
            return "eq";
        }
    }

    private NSmartContext smartContext;
    private NCubePlan cubePlan;
    private NDataModel model;
    private final ProjectInstance projectInstance;

    private Map<FunctionDesc, Integer> aggFuncIdMap;
    private Map<NCuboidIdentifier, NCuboidDesc> collector;
    private SortedSet<Long> cuboidLayoutIds = Sets.newTreeSet();

    CuboidSuggester(NModelContext modelContext, NCubePlan cubePlan, Map<NCuboidIdentifier, NCuboidDesc> collector) {

        this.smartContext = modelContext.getSmartContext();
        this.model = modelContext.getTargetModel();
        this.cubePlan = cubePlan;
        this.collector = collector;
        this.projectInstance = NProjectManager.getInstance(this.smartContext.getKylinConfig())
                .getProject(this.smartContext.getProject());

        aggFuncIdMap = Maps.newHashMap();
        model.getEffectiveMeasureMap()
                .forEach((measureId, measure) -> aggFuncIdMap.put(measure.getFunction(), measureId));

        collector.forEach((cuboidIdentifier, cuboidDesc) -> cuboidDesc.getLayouts()
                .forEach(layout -> cuboidLayoutIds.add(layout.getId())));
    }

    void suggestCuboids(ModelTree modelTree) {
        final Map<String, AccelerateInfo> sql2AccelerateInfo = smartContext.getAccelerateInfoMap();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            String sql = ctx.sql;
            if (!sql2AccelerateInfo.containsKey(sql)) {
                sql2AccelerateInfo.put(sql, new AccelerateInfo());
            }

            AccelerateInfo accelerateInfo = sql2AccelerateInfo.get(sql);
            if (accelerateInfo.isBlocked()) {
                continue;
            }

            try {
                Map<String, String> aliasMap = RealizationChooser.matchJoins(model, ctx);
                Preconditions.checkState(aliasMap != null, getMsgTemplateByModelMaintainType(TABLE_NOT_MATCHED),
                        model.getAlias());
                ctx.fixModel(model, aliasMap);
                QueryLayoutRelation queryLayoutRelation = ingest(ctx, model);
                accelerateInfo.getRelatedLayouts().add(queryLayoutRelation);
            } catch (Exception e) {
                logger.error("Unable to suggest cuboid for CubePlan", e);
                accelerateInfo.setBlockingCause(e);
                accelerateInfo.getRelatedLayouts().clear();
            } finally {
                ctx.unfixModel();
            }
        }
    }

    private Map<Integer, String> suggestIndexMap(OLAPContext ctx, final Map<Integer, Double> dimScores,
            Map<Integer, TblColRef> colRefMap) {
        ColIndexSuggester suggester = new ColIndexSuggester(ctx);
        Map<Integer, String> ret = Maps.newHashMap();
        for (Map.Entry<Integer, Double> dimEntry : dimScores.entrySet()) {
            int dimId = dimEntry.getKey();
            String index = suggester.suggest(colRefMap.get(dimId));
            if (!"eq".equals(index)) {
                ret.put(dimId, index);
            }
        }
        return ret;
    }

    private List<Integer> suggestShardBy(Collection<Integer> dimIds) {
        List<Integer> shardBy = Lists.newArrayList();
        for (int dimId : dimIds) {
            TblColRef colRef = model.getEffectiveColsMap().get(dimId);
            TableExtDesc.ColumnStats colStats = smartContext.getColumnStats(colRef);
            if (colStats != null
                    && colStats.getCardinality() > smartContext.getSmartConfig().getRowkeyUHCCardinalityMin()) {
                shardBy.add(dimId);
            }
        }
        return shardBy;
    }

    private List<Integer> suggestSortBy(OLAPContext ctx, Map<TblColRef, Integer> colIdMap) {

        // TODO need a more proper fix
        if (CollectionUtils.isEmpty(ctx.getSortColumns())) {
            return Lists.newArrayList();
        }

        List<Integer> ret = Lists.newArrayList();
        for (TblColRef col : ctx.getSortColumns()) {
            final Integer id = colIdMap.get(col);
            Preconditions.checkState(id != null, getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN),
                    col.getIdentity(), model.getId());
            ret.add(id);
        }
        return ret;
    }

    private Map<Integer, Double> getDimScores(OLAPContext ctx, Map<TblColRef, Integer> colIdMap, boolean isTableIndex) {

        final Map<Integer, Double> dimScores = Maps.newHashMap();
        Set<TblColRef> nonFilterCols = Sets.newHashSet(ctx.allColumns);
        nonFilterCols.removeAll(ctx.filterColumns);
        ctx.aggregations.forEach(functionDesc -> {
            if (functionDesc.getParameter() == null) {
                return;
            }

            final List<TblColRef> aggCols = functionDesc.getParameter().getColRefs();
            if (CollectionUtils.isNotEmpty(aggCols)) {
                aggCols.removeAll(ctx.getGroupByColumns());
                aggCols.removeAll(ctx.getSubqueryJoinParticipants());
                nonFilterCols.removeAll(aggCols);
            }
        });

        calcDimScores(nonFilterCols, dimScores, colIdMap, false, isTableIndex);
        calcDimScores(ctx.filterColumns, dimScores, colIdMap, true, isTableIndex);

        return dimScores;
    }

    private void calcDimScores(Set<TblColRef> cols, Map<Integer, Double> dimScores, Map<TblColRef, Integer> colIdMap,
            boolean isFilterCols, boolean isTableIndex) {

        if (CollectionUtils.isEmpty(cols)) {
            return;
        }

        cols.forEach(colRef -> {
            Integer colId = colIdMap.get(colRef);
            Preconditions.checkState(colId != null, getMsgTemplateByModelMaintainType(COLUMN_NOT_FOUND_PTN),
                    colRef.getIdentity(), model.getId());
            TableExtDesc.ColumnStats columnStats = smartContext.getColumnStats(colRef);

            // without column stats
            if (columnStats == null) {
                dimScores.put(colId, isFilterCols ? 1D : -1D);
                return;
            }

            // with column stats
            double cardinality = columnStats.getCardinality();
            cardinality = cardinality > 0 ? cardinality : 1;
            double score;
            if (isFilterCols) {
                score = cardinality;
            } else if (isTableIndex) {
                score = -1;
            } else {
                score = -1 / cardinality;
            }
            dimScores.put(colId, score);
        });
    }

    private QueryLayoutRelation ingest(OLAPContext ctx, NDataModel model) {

        boolean isTableIndex = ctx.getSQLDigest().isRawQuery;

        // for table index need all effective cols, for agg index only need all dimensions
        Map<TblColRef, Integer> colIdMap = Maps
                .newHashMap((isTableIndex ? model.getEffectiveColsMap() : model.getEffectiveDimenionsMap()).inverse());

        final Map<Integer, Double> dimScores = getDimScores(ctx, colIdMap, isTableIndex);

        SortedSet<Integer> measureIds = Sets.newTreeSet();
        NCuboidDesc cuboidDesc = isTableIndex ? createTableIndex(ctx, dimScores)
                : createAggregatedIndex(ctx, dimScores, measureIds, colIdMap);

        final NCuboidIdentifier cuboidIdentifier = cuboidDesc.createCuboidIdentifier();
        if (collector.containsKey(cuboidIdentifier)) {
            cuboidDesc = collector.get(cuboidIdentifier);
        } else {
            collector.put(cuboidIdentifier, cuboidDesc);
        }

        List<Integer> shardBy = Lists.newArrayList();
        List<Integer> sortBy = Lists.newArrayList();
        if (isTableIndex) {
            shardBy = suggestShardBy(dimScores.keySet());
            sortBy = suggestSortBy(ctx, colIdMap);
        }

        NCuboidLayout layout = new NCuboidLayout();
        layout.setId(suggestLayoutId(cuboidDesc));
        //        layout.setLayoutOverrideIndices(suggestIndexMap(ctx, dimScores, model.getEffectiveColsMap()));
        layout.setColOrder(suggestColOrder(dimScores, measureIds));
        layout.setCuboidDesc(cuboidDesc);
        layout.setShardByColumns(shardBy);
        layout.setSortByColumns(sortBy);
        layout.setAuto(true);
        layout.setDraftVersion(smartContext.getDraftVersion());

        String modelId = model.getName();
        String cubePlanId = cuboidDesc.getCubePlan().getName();
        int semanticVersion = model.getSemanticVersion();
        for (NCuboidLayout l : cuboidDesc.getLayouts()) {
            if (l.equals(layout)) {
                return new QueryLayoutRelation(ctx.sql, modelId, cubePlanId, l.getId(), semanticVersion);
            }
        }

        cuboidDesc.getLayouts().add(layout);
        cuboidLayoutIds.add(layout.getId());

        return new QueryLayoutRelation(ctx.sql, modelId, cubePlanId, layout.getId(), semanticVersion);
    }

    private NCuboidDesc createTableIndex(OLAPContext ctx, Map<Integer, Double> dimScores) {
        // no column selected in raw query (i.e. select 1 from kylin_sales)
        if (dimScores.isEmpty()) {
            Preconditions.checkState(CollectionUtils.isNotEmpty(model.getAllNamedColumns()),
                    "Cannot suggest any columns in table index.");
            final NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().iterator().next();
            dimScores.put(namedColumn.getId(), -1D);
        }

        final Set<TblColRef> allColumns = ctx.allColumns;
        for (TblColRef col : allColumns) {
            dimScores.putIfAbsent(model.getColumnIdByColumnName(col.getIdentity()), -1D);
        }

        return createCuboidDesc(dimScores, Sets.newTreeSet(), true);
    }

    private NCuboidDesc createAggregatedIndex(OLAPContext ctx, Map<Integer, Double> dimScores,
            SortedSet<Integer> measureIds, Map<TblColRef, Integer> colIdMap) {
        // Add default measure count(1)
        measureIds.add(NDataModel.MEASURE_ID_BASE);

        ctx.aggregations.forEach(aggFunc -> {
            Integer measureId = aggFuncIdMap.get(aggFunc);
            if (measureId != null) {
                measureIds.add(measureId);
            } else if (aggFunc.getParameter() != null) {
                for (TblColRef tblColRef : aggFunc.getParameter().getColRefs()) {
                    Preconditions.checkState(colIdMap.get(tblColRef) != null,
                            getMsgTemplateByModelMaintainType(MEASURE_NOT_FOUND_PTN), aggFunc, model.getAlias());
                }
            }
        });

        return createCuboidDesc(dimScores, measureIds, false);
    }

    private String getMsgTemplateByModelMaintainType(String messagePattern) {
        Preconditions.checkNotNull(model);
        String rst = "In the model designer project, the system is not allowed to modify the semantic layer "
                + "(dimensions, measures, tables, and joins) of the model. ";
        return projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN //
                ? rst + messagePattern
                : messagePattern;
    }

    private NCuboidDesc createCuboidDesc(Map<Integer, Double> dimScores, SortedSet<Integer> measureIds,
            boolean isTableIndex) {
        Preconditions.checkState(!dimScores.isEmpty() || !measureIds.isEmpty(),
                "Neither dimension nor measure could be proposed for CuboidDesc");

        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(suggestDescId(isTableIndex));
        cuboidDesc.setDimensions(dimScores.keySet().stream().sorted().collect(Collectors.toList()));
        cuboidDesc.setMeasures(Lists.newArrayList(measureIds));
        cuboidDesc.setCubePlan(cubePlan);

        return cuboidDesc;
    }

    private List<Integer> suggestColOrder(final Map<Integer, Double> dimScores, Set<Integer> measureIds) {
        List<Integer> colOrder = Lists.newArrayList();
        final List<Integer> orderedDimIds = dimScores.entrySet().stream() //
                .sorted((e1, e2) -> {
                    if (e1.getValue() < e2.getValue()) {
                        return 1;
                    } else if (e1.getValue() > e2.getValue()) {
                        return -1;
                    }
                    return Integer.compare(e1.getKey(), e2.getKey());
                }).map(Map.Entry::getKey).collect(Collectors.toList());
        colOrder.addAll(orderedDimIds);
        colOrder.addAll(measureIds);
        return colOrder;
    }

    private long suggestDescId(boolean isTableIndex) {
        return findAvailableCuboidDescId(isTableIndex);
    }

    private long findAvailableCuboidDescId(boolean isTableIndex) {
        final Collection<NCuboidDesc> cuboidDescs = collector.values();
        long result = isTableIndex ? NCuboidDesc.TABLE_INDEX_START_ID : 0;
        List<Long> cuboidIds = Lists.newArrayList();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            long cuboidDescId = cuboidDesc.getId();
            if ((isTableIndex && cuboidDescId >= NCuboidDesc.TABLE_INDEX_START_ID)
                    || (!isTableIndex && cuboidDescId < NCuboidDesc.TABLE_INDEX_START_ID)) {
                cuboidIds.add(cuboidDescId);
            }
        }

        if (!cuboidIds.isEmpty()) {
            // use the largest cuboid id + step
            cuboidIds.sort(Long::compareTo);
            result = cuboidIds.get(cuboidIds.size() - 1) + NCuboidDesc.CUBOID_DESC_ID_STEP;
        }
        return Math.max(result, isTableIndex ? cubePlan.getNextTableIndexId() : cubePlan.getNextAggregateIndexId());
    }

    private long suggestLayoutId(NCuboidDesc cuboidDesc) {
        long s = cuboidDesc.getLastLayout() == null ? cuboidDesc.getId() + 1 : cuboidDesc.getLastLayout().getId() + 1;
        while (cuboidLayoutIds.contains(s)) {
            s++;
        }
        return s;
    }
}
