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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
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
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AccelerateInfo.QueryLayoutRelation;
import io.kyligence.kap.smart.model.ModelTree;

class CuboidSuggester {
    private static final Logger logger = LoggerFactory.getLogger(CuboidSuggester.class);

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

    private NSmartContext context;
    private NCubePlan cubePlan;
    private NDataModel model;
    private Map<TblColRef, Integer> colIdMap;
    private Map<FunctionDesc, Integer> aggFuncIdMap;
    private Map<NCuboidIdentifier, NCuboidDesc> collector;

    private SortedSet<Long> cuboidLayoutIds = Sets.newTreeSet();

    CuboidSuggester(NModelContext context, NCubePlan cubePlan, Map<NCuboidIdentifier, NCuboidDesc> collector) {
        this.context = context.getSmartContext();
        this.model = context.getTargetModel();
        this.cubePlan = cubePlan;
        this.collector = collector;

        colIdMap = model.getEffectiveColsMap().inverse();

        aggFuncIdMap = Maps.newHashMap();
        model.getEffectiveMeasureMap()
                .forEach((measureId, measure) -> aggFuncIdMap.put(measure.getFunction(), measureId));

        collector.forEach((cuboidIdentifier, cuboidDesc) -> cuboidDesc.getLayouts()
                .forEach(layout -> cuboidLayoutIds.add(layout.getId())));
    }

    void suggestCuboids(ModelTree modelTree) {
        final Map<String, AccelerateInfo> sql2AccelerateInfo = context.getAccelerateInfoMap();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            String sql = ctx.sql;
            if (!sql2AccelerateInfo.containsKey(sql)) {
                sql2AccelerateInfo.put(sql, new AccelerateInfo());
            }
            ctx.fixModel(model, aliasMap);
            AccelerateInfo accelerateInfo = sql2AccelerateInfo.get(sql);
            try {
                QueryLayoutRelation queryLayoutRelation = ingest(ctx, model);
                accelerateInfo.getRelatedLayouts().add(queryLayoutRelation);
            } catch (Exception e) {
                logger.error("Unable to suggest cuboid for CubePlan", e);
                accelerateInfo.setBlockingCause(e);
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
            TableExtDesc.ColumnStats colStats = context.getColumnStats(colRef);
            if (colStats != null && colStats.getCardinality() > context.getSmartConfig().getRowkeyUHCCardinalityMin()) {
                shardBy.add(dimId);
            }
        }
        return shardBy;
    }

    private List<Integer> suggestSortBy(OLAPContext ctx) {
        // TODO need a more proper fix
        if (CollectionUtils.isEmpty(ctx.getSortColumns())) {
            return Lists.newArrayList();
        }

        List<Integer> ret = Lists.newArrayList();
        for (TblColRef col : ctx.getSortColumns()) {
            final Integer id = colIdMap.get(col);
            Preconditions.checkNotNull(id);
            ret.add(id);
        }
        return ret;
    }

    private Map<Integer, Double> getDimScores(OLAPContext ctx) {
        final Map<Integer, Double> dimScores = Maps.newHashMap();

        Set<TblColRef> groupByCols = Sets.newHashSet(ctx.allColumns);
        if (ctx.filterColumns != null)
            groupByCols.removeAll(ctx.filterColumns);
        for (FunctionDesc func : ctx.aggregations) {
            if (func.getParameter() == null)
                continue;

            List<TblColRef> aggCols = func.getParameter().getColRefs();
            if (aggCols != null)
                groupByCols.removeAll(aggCols);
        }
        if (ctx.groupByColumns != null)
            groupByCols.addAll(ctx.groupByColumns);
        if (ctx.subqueryJoinParticipants != null)
            groupByCols.addAll(ctx.subqueryJoinParticipants);

        calcDimScores(groupByCols, dimScores, false);
        calcDimScores(ctx.filterColumns, dimScores, true);

        return dimScores;
    }

    private void calcDimScores(Set<TblColRef> cols, Map<Integer, Double> dimScores, boolean isFilterCols) {
        if (CollectionUtils.isEmpty(cols)) {
            return;
        }

        for (TblColRef colRef : cols) {
            Integer colId = colIdMap.get(colRef);
            if (colId == null) {
                // FIXME model not contains all columns of ctx, this is not supposed to happen
                throw new IllegalArgumentException();
            }
            TableExtDesc.ColumnStats columnStats = context.getColumnStats(colRef);
            if (columnStats != null && columnStats.getCardinality() > 0) {
                if (isFilterCols) {
                    dimScores.put(colId, (double) columnStats.getCardinality());
                } else {
                    dimScores.put(colId, -1D / columnStats.getCardinality());
                }
            } else
                dimScores.put(colId, 0D);
        }
    }

    private QueryLayoutRelation ingest(OLAPContext ctx, NDataModel model) {

        final Map<Integer, Double> dimScores = getDimScores(ctx);
        SortedSet<Integer> measureIds = Sets.newTreeSet();

        boolean isTableIndex = ctx.getSQLDigest().isRawQuery;
        NCuboidDesc cuboidDesc = isTableIndex ? createTableIndex(ctx, dimScores)
                : createAggregatedIndex(ctx, dimScores, measureIds);

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
            sortBy = suggestSortBy(ctx);
        }

        NCuboidLayout layout = new NCuboidLayout();
        layout.setId(suggestLayoutId(cuboidDesc));
        layout.setLayoutOverrideIndices(suggestIndexMap(ctx, dimScores, model.getEffectiveColsMap()));
        layout.setColOrder(suggestColOrder(dimScores, measureIds, isTableIndex));
        layout.setCuboidDesc(cuboidDesc);
        layout.setShardByColumns(shardBy);
        layout.setSortByColumns(sortBy);
        layout.setDraftVersion(context.getDraftVersion());

        for (NCuboidLayout l : cuboidDesc.getLayouts()) {
            if (l.equals(layout)) {
                return new QueryLayoutRelation(ctx.sql, model.getId(), cuboidDesc.getCubePlan().getId(), l.getId());
            }
        }

        cuboidDesc.getLayouts().add(layout);
        cuboidLayoutIds.add(layout.getId());

        return new QueryLayoutRelation(ctx.sql, model.getId(), cuboidDesc.getCubePlan().getId(), layout.getId());
    }

    private NCuboidDesc createTableIndex(OLAPContext ctx, Map<Integer, Double> dimScores) {
        final Set<TblColRef> allColumns = ctx.allColumns;
        for (TblColRef col : allColumns) {
            dimScores.put(model.getColumnIdByColumnName(col.getIdentity()), -1D);
        }

        return createCuboidDesc(dimScores.keySet(), new HashSet<>(), true);
    }

    private NCuboidDesc createAggregatedIndex(OLAPContext ctx, Map<Integer, Double> dimScores,
            SortedSet<Integer> measureIds) {
        // Add default measure count(1)
        measureIds.add(NDataModel.MEASURE_ID_BASE);

        // FIXME this line work-around empty dimension case (to be fixed by KAP#7224)
        // Example: select count(*) from kylin_sales
        fixDimScoresIfEmpty(model, dimScores);

        for (FunctionDesc aggFunc : ctx.aggregations) {
            Integer measureId = aggFuncIdMap.get(aggFunc);
            if (measureId != null) {
                measureIds.add(measureId);
            } else if (aggFunc.getParameter() != null) {
                // dimension as measure, put cols to rowkey tail
                for (TblColRef colRef : aggFunc.getParameter().getColRefs()) {
                    int colId = colIdMap.get(colRef);
                    if (!dimScores.containsKey(colId))
                        dimScores.put(colId, -1D);
                }
            }
        }

        return createCuboidDesc(dimScores.keySet(), measureIds, false);
    }

    private void fixDimScoresIfEmpty(NDataModel model, Map<Integer, Double> dimScores) {
        if (dimScores.isEmpty()) {
            Map<String, NDataModel.NamedColumn> dimensionCandidate = new HashMap<>();
            for (NDataModel.NamedColumn namedColumn : model.getAllNamedColumns()) {
                dimensionCandidate.put(namedColumn.name, namedColumn);
            }
            for (NDataModel.Measure measure : model.getAllMeasures()) {
                FunctionDesc agg = measure.getFunction();
                if (agg == null || agg.getParameter() == null || !agg.getParameter().isColumnType()) {
                    continue;
                }
                dimensionCandidate.remove(agg.getParameter().getValue());
            }
            if (dimensionCandidate.isEmpty()) {
                throw new IllegalStateException("Suggest no dimension");
            }
            dimScores.put(dimensionCandidate.values().iterator().next().id, -1D);
        }
    }

    private NCuboidDesc createCuboidDesc(Set<Integer> dimIds, Set<Integer> measureIds, boolean isTableIndex) {
        Preconditions.checkState(!dimIds.isEmpty() || !measureIds.isEmpty(),
                "Neither dimension nor measure could be proposed for CuboidDesc");

        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(suggestDescId(isTableIndex));
        cuboidDesc.setDimensions(Lists.newArrayList(dimIds));
        cuboidDesc.setMeasures(Lists.newArrayList(measureIds));
        cuboidDesc.setCubePlan(cubePlan);

        Collections.sort(cuboidDesc.getDimensions());
        Collections.sort(cuboidDesc.getMeasures());
        return cuboidDesc;
    }

    private List<Integer> suggestColOrder(final Map<Integer, Double> dimScores, Set<Integer> measureIds,
            boolean isTableIndex) {
        List<Integer> colOrder = Lists.newArrayList();

        colOrder.addAll(dimScores.keySet());
        colOrder.sort((c1, c2) -> {
            if (!isTableIndex) {
                if (dimScores.get(c1) < dimScores.get(c2)) {
                    return 1;
                } else if (dimScores.get(c1) > dimScores.get(c2)) {
                    return -1;
                }
            }
            return Integer.compare(c1, c2);
        });

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

        if (cuboidIds.isEmpty()) {
            return result;
        }

        Collections.sort(cuboidIds);
        for (Long cuboidId : cuboidIds) {
            // exist cuboid gap
            if (cuboidId - result > NCuboidDesc.CUBOID_DESC_ID_STEP) {
                break;
            } else {
                result = cuboidId;
            }
        }

        return result + NCuboidDesc.CUBOID_DESC_ID_STEP;
    }

    private long suggestLayoutId(NCuboidDesc cuboidDesc) {
        long s = cuboidDesc.getLastLayout() == null ? cuboidDesc.getId() + 1 : cuboidDesc.getLastLayout().getId() + 1;
        while (cuboidLayoutIds.contains(s)) {
            s++;
        }
        return s;
    }
}
