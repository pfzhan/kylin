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

package io.kyligence.kap.metadata.cube.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.gridtable.GridTableMapping;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;

public class NLayoutToGridTableMapping extends GridTableMapping {
    private final LayoutEntity layoutEntity;
    private boolean isBatchOfHybrid = false;

    public NLayoutToGridTableMapping(LayoutEntity layoutEntity) {
        this.layoutEntity = layoutEntity;
        init();
    }

    public NLayoutToGridTableMapping(LayoutEntity layoutEntity, boolean isBatchOfHybrid) {
        this.layoutEntity = layoutEntity;
        this.isBatchOfHybrid = isBatchOfHybrid;
        init();
    }

    private void init() {
        // FIXME: currently only support all dimensions in one column family
        int gtColIdx = 0;
        gtDataTypes = Lists.newArrayList();
        gtColBlocks = Lists.newArrayList();

        // dimensions
        dim2gt = Maps.newHashMap();
        BitSet pk = new BitSet();
        List<TblColRef> columns = new ArrayList<>();
        if (isBatchOfHybrid) {
            columns.addAll(layoutEntity.getStreamingColumns());
        } else {
            columns.addAll(layoutEntity.getColumns());
        }
        for (TblColRef dimension : columns) {
            gtDataTypes.add(dimension.getType());
            dim2gt.put(dimension, gtColIdx);
            pk.set(gtColIdx);
            gtColIdx++;
        }
        gtPrimaryKey = new ImmutableBitSet(pk);
        gtColBlocks.add(gtPrimaryKey);

        nDimensions = gtColIdx;
        assert nDimensions == layoutEntity.getColumns().size();

        // column blocks of metrics
        ArrayList<BitSet> metricsColBlocks = Lists.newArrayList();
        for (int i = 0; i < layoutEntity.getOrderedMeasures().size(); i++) {
            metricsColBlocks.add(new BitSet());
        }

        // metrics
        metrics2gt = Maps.newHashMap();
        int mColBlock = 0;
        List<NDataModel.Measure> measureDescs = new ArrayList<>();
        if (isBatchOfHybrid) {
            measureDescs.addAll(layoutEntity.getStreamingMeasures());
        } else {
            measureDescs.addAll(layoutEntity.getOrderedMeasures().values());
        }
        for (NDataModel.Measure measure : measureDescs) {
            // Count distinct & holistic count distinct are equals() but different.
            // Ensure the holistic version if exists is always the first.
            FunctionDesc func = measure.getFunction();
            metrics2gt.put(func, gtColIdx);
            gtDataTypes.add(func.getReturnDataType());

            // map to column block
            metricsColBlocks.get(mColBlock++).set(gtColIdx++);
        }

        for (BitSet set : metricsColBlocks) {
            gtColBlocks.add(new ImmutableBitSet(set));
        }

        nMetrics = gtColIdx - nDimensions;
        assert nMetrics == layoutEntity.getOrderedMeasures().size();
    }

    @Override
    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        return layoutEntity.getColumns();
    }

    @Override
    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        List<TblColRef> dims = layoutEntity.getColumns();
        DimensionEncoding[] dimEncs = new DimensionEncoding[dims.size()];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dims.get(i));
        }
        return dimEncs;
    }

    @Override
    public Map<Integer, Integer> getDependentMetricsMap() {
        Map<Integer, Integer> result = Maps.newHashMap();
        Collection<NDataModel.Measure> measures = layoutEntity.getOrderedMeasures().values();
        for (NDataModel.Measure child : layoutEntity.getOrderedMeasures().values()) {
            if (child.getDependentMeasureRef() != null) {
                boolean ok = false;
                for (NDataModel.Measure parent : measures) {
                    if (parent.getName().equals(child.getDependentMeasureRef())) {
                        int childIndex = getIndexOf(child.getFunction());
                        int parentIndex = getIndexOf(parent.getFunction());
                        result.put(childIndex, parentIndex);
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    throw new IllegalStateException("Cannot find dependent measure: " + child.getDependentMeasureRef());
            }
        }
        return result.isEmpty() ? Collections.<Integer, Integer> emptyMap() : result;
    }

    @Override
    public String getTableName() {
        return "Cuboid " + layoutEntity.getId();
    }
}
