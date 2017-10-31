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
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
                // do not remove duplicated here, because either maybe used on query. we can
                // remove duplication on metadata cleanup
            }
        }

        NDataModel model = context.getTargetModel();
        ModelTree modelTree = context.getModelTree();
        for (OLAPContext ctx : modelTree.getOlapContexts()) {
            Map<String, String> aliasMap = RealizationChooser.matches(model, ctx);
            ctx.fixModel(model, aliasMap);

            addCuboidLayout(cubePlan, ctx, cuboidDescs);

            ctx.unfixModel();
        }

        cubePlan.setCuboids(Lists.newArrayList(cuboidDescs.values()));
    }

    private void addCuboidLayout(NCubePlan cubePlan, OLAPContext ctx, Map<Pair<BitSet, BitSet>, NCuboidDesc> result) {
        NDataModel model = context.getTargetModel();
        Map<TblColRef, Integer> colIdMap = model.getEffectiveColsMap().inverse();
        Map<NDataModel.Measure, Integer> measureIdMap = model.getEffectiveMeasureMap().inverse();

        BitSet dimBitSet = new BitSet();
        BitSet measureBitSet = new BitSet();
        final Map<Integer, Integer> dimScores = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(ctx.groupByColumns)) {
            for (TblColRef colRef : ctx.groupByColumns) {
                int colId = colIdMap.get(colRef);
                dimBitSet.set(colId);
                dimScores.put(colId, 0);
            }
        }
        if (CollectionUtils.isNotEmpty(ctx.filterColumns)) {
            for (TblColRef colRef : ctx.filterColumns) {
                int colId = colIdMap.get(colRef);
                dimBitSet.set(colId);
                dimScores.put(colId, 1);
            }
        }

        List<Integer> measureIds = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(ctx.aggregations)) {
            for (FunctionDesc aggFunc : ctx.aggregations) {
                NDataModel.Measure m = new NDataModel.Measure();
                for (Map.Entry<NDataModel.Measure, Integer> measureEntry : measureIdMap.entrySet()) {
                    if (aggFunc.equals(measureEntry.getKey().getFunction())) {
                        measureIds.add(measureEntry.getValue());
                        measureBitSet.set(measureEntry.getValue());
                        break;
                    }
                }
            }
        }

        if (dimScores.isEmpty() && measureIds.isEmpty())
            return;

        NRowkeyColumnDesc[] rowkeyColumnDescs = new NRowkeyColumnDesc[dimScores.size()];
        {
            int i = 0;
            for (Map.Entry<Integer, Integer> dimEntry : dimScores.entrySet()) {
                rowkeyColumnDescs[i] = new NRowkeyColumnDesc();
                rowkeyColumnDescs[i].setDimensionId(dimEntry.getKey());
                rowkeyColumnDescs[i].setIndex("eq"); // TODO: decide index with logic
                i++;
            }

            Arrays.sort(rowkeyColumnDescs, new Comparator<NRowkeyColumnDesc>() {
                @Override
                public int compare(NRowkeyColumnDesc o1, NRowkeyColumnDesc o2) {
                    int c1 = o1.getDimensionId();
                    int c2 = o2.getDimensionId();
                    return dimScores.get(c1) - dimScores.get(c2);
                }
            });
        }

        // column family
        // FIXME: currently only support all dimensions in one column family
        NColumnFamilyDesc.DimensionCF[] dimCFs = new NColumnFamilyDesc.DimensionCF[1];
        dimCFs[0] = new NColumnFamilyDesc.DimensionCF();
        dimCFs[0].setName("D");
        dimCFs[0].setColumns(ArrayUtils.toPrimitive(dimScores.keySet().toArray(new Integer[0])));
        NColumnFamilyDesc.MeasureCF[] measureCFS = new NColumnFamilyDesc.MeasureCF[measureIds.size()];
        for (int c = 0; c < measureCFS.length; c++) {
            measureCFS[c] = new NColumnFamilyDesc.MeasureCF();
            measureCFS[c].setName(String.format("M%d", measureIds.get(c)));
            measureCFS[c].setColumns(new int[] { measureIds.get(c) });
        }

        Pair<BitSet, BitSet> cuboidKey = new Pair<>(dimBitSet, measureBitSet);
        NCuboidDesc cuboidDesc = result.get(cuboidKey);
        if (cuboidDesc == null) {
            cuboidDesc = new NCuboidDesc();
            cuboidDesc.setId(findLargestCuboidDescId(result.values()) + NCubePlanManager.CUBOID_DESC_ID_STEP);
            cuboidDesc.setDimensions(ArrayUtils.toPrimitive(dimScores.keySet().toArray(new Integer[0])));
            cuboidDesc.setMeasures(ArrayUtils.toPrimitive(measureIds.toArray(new Integer[0])));
            cuboidDesc.setCubePlan(cubePlan);
            result.put(cuboidKey, cuboidDesc);
        } else {
            for (NCuboidLayout layout : cuboidDesc.getLayouts()) {
                if (Arrays.equals(layout.getRowkeyColumns(), rowkeyColumnDescs)
                        && Arrays.equals(layout.getDimensionCFs(), dimCFs)
                        && Arrays.equals(layout.getMeasureCFs(), measureCFS)) {
                    return;
                }
            }
        }

        NCuboidLayout layout = new NCuboidLayout();
        layout.setId(
                cuboidDesc.getLastLayout() == null ? cuboidDesc.getId() + 1 : cuboidDesc.getLastLayout().getId() + 1);
        layout.setRowkeyColumns(rowkeyColumnDescs);
        layout.setDimensionCFs(dimCFs);
        layout.setMeasureCFs(measureCFS);
        layout.setCuboidDesc(cuboidDesc);
        cuboidDesc.getLayouts().add(layout);
    }

    private long findLargestCuboidDescId(Collection<NCuboidDesc> cuboidDescs) {
        long cuboidId = 0 - NCubePlanManager.CUBOID_DESC_ID_STEP;
        for (NCuboidDesc cuboidDesc : cuboidDescs)
            cuboidId = Math.max(cuboidId, cuboidDesc.getId());
        return cuboidId;
    }
}