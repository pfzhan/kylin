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

package io.kyligence.kap.cube.model;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.model.ModelTree;

public class NCuboidProposer extends NAbstractCubeProposer {
    NCuboidProposer(NSmartContext.NModelContext context) {
        super(context);
    }

    @Override
    void doPropose(NCubePlan cubePlan) {
        Map<Pair<BitSet, BitSet>, NCuboidDesc> cuboidDescs = Maps.newLinkedHashMap();
        for (NCuboidDesc cuboidDesc : cubePlan.getCuboids()) {
            Pair<BitSet, BitSet> key = new Pair<>(cuboidDesc.getDimensionSet().mutable(),
                    cuboidDesc.getMeasureSet().mutable());
            NCuboidDesc desc = cuboidDescs.get(key);

            if (desc == null) {
                cuboidDescs.put(key, cuboidDesc);
            } else {
                desc.getLayouts().addAll(cuboidDesc.getLayouts());
            }
        }

        NDataModel model = context.getTargetModel();
        ModelTree modelTree = context.getModelTree();
        CuboidSuggester suggester = new CuboidSuggester(cubePlan, cuboidDescs);
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            ctx.fixModel(model, aliasMap);
            suggester.ingest(ctx);
            ctx.unfixModel();
        }

        cubePlan.setCuboids(Lists.newArrayList(cuboidDescs.values()));
    }

    private class RowkeyComparator implements Comparator<NRowkeyColumnDesc> {
        final Map<Integer, Integer> dimScores;

        public RowkeyComparator(Map<Integer, Integer> dimScores) {
            this.dimScores = dimScores;
        }

        @Override
        public int compare(NRowkeyColumnDesc o1, NRowkeyColumnDesc o2) {
            int c1 = o1.getDimensionId();
            int c2 = o2.getDimensionId();
            return dimScores.get(c1) - dimScores.get(c2);
        }
    }

    class RowkeySuggester {
        OLAPContext olapContext;

        private RowkeySuggester(OLAPContext olapContext) {
            this.olapContext = olapContext;
        }

        private String suggestIndex(TblColRef colRef) {
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

        NRowkeyColumnDesc suggest(int dimId, TblColRef colRef) {
            NRowkeyColumnDesc desc = new NRowkeyColumnDesc();
            desc.setDimensionId(dimId);
            desc.setIndex(suggestIndex(colRef));
            return desc;
        }
    }

    class DimensionCFClusterer {
        NColumnFamilyDesc.DimensionCF[] cluster(Collection<Integer> dimIds) {
            // TODO: because of limitation of GTRecord, currently only support all dimensions in one column family
            NColumnFamilyDesc.DimensionCF[] dimCFs = new NColumnFamilyDesc.DimensionCF[1];
            dimCFs[0] = new NColumnFamilyDesc.DimensionCF();
            dimCFs[0].setName("ALL_DIM");
            dimCFs[0].setColumns(ArrayUtils.toPrimitive(dimIds.toArray(new Integer[0])));
            return dimCFs;
        }
    }

    class MeasureCFClusterer {
        NColumnFamilyDesc.MeasureCF[] cluster(Collection<Integer> measureIds) {
            NColumnFamilyDesc.MeasureCF[] measureCFs = new NColumnFamilyDesc.MeasureCF[measureIds.size()];
            int c = 0;
            for (Integer measureId : measureIds) {
                measureCFs[c] = new NColumnFamilyDesc.MeasureCF();
                measureCFs[c].setName(String.format("MEASURE_%d", measureId));
                measureCFs[c].setColumns(new int[] { measureId });
                c++;
            }
            return measureCFs;
        }
    }

    class CuboidSuggester {
        NCubePlan cubePlan;
        NDataModel model;
        Map<TblColRef, Integer> colIdMap;
        Map<FunctionDesc, Integer> aggFuncIdMap;
        Map<Pair<BitSet, BitSet>, NCuboidDesc> collector;

        SortedSet<Long> cuboidLayoutIds = Sets.newTreeSet();

        CuboidSuggester(NCubePlan cubePlan, Map<Pair<BitSet, BitSet>, NCuboidDesc> collector) {
            this.cubePlan = cubePlan;
            this.collector = collector;

            model = context.getTargetModel();
            colIdMap = model.getEffectiveColsMap().inverse();

            aggFuncIdMap = Maps.newHashMap();
            for (Map.Entry<Integer, NDataModel.Measure> measureEntry : model.getEffectiveMeasureMap().entrySet()) {
                aggFuncIdMap.put(measureEntry.getValue().getFunction(), measureEntry.getKey());
            }

            for (NCuboidDesc cuboidDesc : collector.values()) {
                for (NCuboidLayout layout : cuboidDesc.getLayouts())
                    cuboidLayoutIds.add(layout.getId());
            }
        }

        private NRowkeyColumnDesc[] suggestRowkeys(OLAPContext ctx, final Map<Integer, Integer> dimScores,
                Map<Integer, TblColRef> colRefMap) {
            RowkeySuggester suggester = new RowkeySuggester(ctx);
            NRowkeyColumnDesc[] descs = new NRowkeyColumnDesc[dimScores.size()];
            int i = 0;
            for (Map.Entry<Integer, Integer> dimEntry : dimScores.entrySet()) {
                int dimId = dimEntry.getKey();
                descs[i++] = suggester.suggest(dimId, colRefMap.get(dimId));
            }
            Arrays.sort(descs, new RowkeyComparator(dimScores));
            return descs;
        }

        private int[] suggestShardBy(Collection<Integer> dimIds) {
            TableMetadataManager tableMetadataManager = TableMetadataManager
                    .getInstance(context.getSmartContext().getKylinConfig());
            List<Integer> shardBy = Lists.newArrayList();
            for (int dimId : dimIds) {
                TblColRef colRef = model.getEffectiveColsMap().get(dimId);
                TableExtDesc tableExtDesc = tableMetadataManager.getTableExt(colRef.getTableRef().getTableDesc());
                if (tableExtDesc != null && !tableExtDesc.getColumnStats().isEmpty()) {
                    TableExtDesc.ColumnStats colStats = tableExtDesc.getColumnStats()
                            .get(colRef.getColumnDesc().getZeroBasedIndex());
                    if (colStats.getCardinality() > context.getSmartContext().getSmartConfig()
                            .getRowkeyUHCCardinalityMin())
                        shardBy.add(dimId);
                }
            }
            return ArrayUtils.toPrimitive(shardBy.toArray(new Integer[0]));
        }

        private Map<Integer, Integer> getDimScores(OLAPContext ctx) {
            final Map<Integer, Integer> dimScores = Maps.newHashMap();

            if (CollectionUtils.isNotEmpty(ctx.groupByColumns)) {
                for (TblColRef colRef : ctx.groupByColumns) {
                    int colId = colIdMap.get(colRef);
                    dimScores.put(colId, 0);
                }
            }
            if (CollectionUtils.isNotEmpty(ctx.filterColumns)) {
                for (TblColRef colRef : ctx.filterColumns) {
                    int colId = colIdMap.get(colRef);
                    dimScores.put(colId, 1);
                }
            }
            return dimScores;
        }

        private boolean compareLayouts(NCuboidLayout l1, NCuboidLayout l2) {
            // TODO: currently it's exact equals, we should tolerate some order and cf inconsistency
            return Arrays.equals(l1.getRowkeyColumns(), l2.getRowkeyColumns())
                    && Arrays.equals(l1.getDimensionCFs(), l2.getDimensionCFs())
                    && Arrays.equals(l1.getMeasureCFs(), l2.getMeasureCFs())
                    && Arrays.equals(l1.getShardByColumns(), l2.getShardByColumns())
                    && Arrays.equals(l1.getSortByColumns(), l2.getSortByColumns());
        }

        void ingest(OLAPContext ctx) {
            final BitSet dimBitSet = new BitSet();
            final BitSet measureBitSet = new BitSet();

            final Map<Integer, Integer> dimScores = getDimScores(ctx);
            for (int dimId : dimScores.keySet())
                dimBitSet.set(dimId);

            SortedSet<Integer> measureIds = Sets.newTreeSet();
            if (CollectionUtils.isNotEmpty(ctx.aggregations)) {
                for (FunctionDesc aggFunc : ctx.aggregations) {
                    int measureId = aggFuncIdMap.get(aggFunc);
                    measureIds.add(measureId);
                    measureBitSet.set(measureId);
                }
            }

            if (dimScores.isEmpty() && measureIds.isEmpty())
                return;

            NRowkeyColumnDesc[] rowkeyColumnDescs = suggestRowkeys(ctx, dimScores, model.getEffectiveColsMap());
            NColumnFamilyDesc.DimensionCF[] dimCFs = new DimensionCFClusterer().cluster(dimScores.keySet());
            NColumnFamilyDesc.MeasureCF[] measureCFS = new MeasureCFClusterer().cluster(measureIds);
            int[] shardBy = suggestShardBy(dimScores.keySet());
            int[] sortBy = new int[0]; // TODO: used for table index.

            Pair<BitSet, BitSet> cuboidKey = new Pair<>(dimBitSet, measureBitSet);

            NCuboidDesc cuboidDesc = collector.get(cuboidKey);
            if (cuboidDesc == null) {
                cuboidDesc = createCuboidDesc(dimScores.keySet(), measureIds);
                collector.put(cuboidKey, cuboidDesc);
            }

            NCuboidLayout layout = new NCuboidLayout();
            layout.setId(suggestLayoutId(cuboidDesc));
            layout.setRowkeyColumns(rowkeyColumnDescs);
            layout.setDimensionCFs(dimCFs);
            layout.setMeasureCFs(measureCFS);
            layout.setCuboidDesc(cuboidDesc);
            layout.setShardByColumns(shardBy);
            layout.setSortByColumns(sortBy);

            for (NCuboidLayout l : cuboidDesc.getLayouts()) {
                if (compareLayouts(l, layout))
                    return;
            }

            cuboidDesc.getLayouts().add(layout);
            cuboidLayoutIds.add(layout.getId());
        }

        private NCuboidDesc createCuboidDesc(Set<Integer> dimIds, Set<Integer> measureIds) {
            NCuboidDesc cuboidDesc = new NCuboidDesc();
            cuboidDesc.setId(suggestDescId());
            cuboidDesc.setDimensions(ArrayUtils.toPrimitive(dimIds.toArray(new Integer[0])));
            cuboidDesc.setMeasures(ArrayUtils.toPrimitive(measureIds.toArray(new Integer[0])));
            cuboidDesc.setCubePlan(cubePlan);

            Arrays.sort(cuboidDesc.getDimensions());
            Arrays.sort(cuboidDesc.getMeasures());
            return cuboidDesc;
        }

        private long suggestDescId() {
            return findLargestCuboidDescId(collector.values()) + NCubePlanManager.CUBOID_DESC_ID_STEP;
        }

        private long suggestLayoutId(NCuboidDesc cuboidDesc) {
            long s = cuboidDesc.getLastLayout() == null ? cuboidDesc.getId() + 1
                    : cuboidDesc.getLastLayout().getId() + 1;
            while (cuboidLayoutIds.contains(s)) {
                s++;
            }
            return s;
        }
    }

    private long findLargestCuboidDescId(Collection<NCuboidDesc> cuboidDescs) {
        long cuboidId = 0 - NCubePlanManager.CUBOID_DESC_ID_STEP;
        for (NCuboidDesc cuboidDesc : cuboidDescs)
            cuboidId = Math.max(cuboidId, cuboidDesc.getId());
        return cuboidId;
    }
}