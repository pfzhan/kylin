/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.cube.gridtable;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CuboidToGridTableMapping extends GridTableMapping {

    final private Cuboid cuboid;

    public CuboidToGridTableMapping(Cuboid cuboid) {
        this.cuboid = cuboid;
        init();
    }

    private void init() {
        int gtColIdx = 0;
        gtDataTypes = Lists.newArrayList();
        gtColBlocks = Lists.newArrayList();

        // dimensions
        dim2gt = Maps.newHashMap();
        BitSet pk = new BitSet();
        for (TblColRef dimension : cuboid.getColumns()) {
            gtDataTypes.add(dimension.getType());
            dim2gt.put(dimension, gtColIdx);
            pk.set(gtColIdx);
            gtColIdx++;
        }
        gtPrimaryKey = new ImmutableBitSet(pk);
        gtColBlocks.add(gtPrimaryKey);

        nDimensions = gtColIdx;
        assert nDimensions == cuboid.getColumns().size();

        // column blocks of metrics
        ArrayList<BitSet> metricsColBlocks = Lists.newArrayList();
        for (HBaseColumnFamilyDesc familyDesc : cuboid.getCubeDesc().getHbaseMapping().getColumnFamily()) {
            for (int i = 0; i < familyDesc.getColumns().length; i++) {
                metricsColBlocks.add(new BitSet());
            }
        }

        // metrics
        metrics2gt = Maps.newHashMap();
        for (MeasureDesc measure : cuboid.getCubeDesc().getMeasures()) {
            // Count distinct & holistic count distinct are equals() but different.
            // Ensure the holistic version if exists is always the first.
            FunctionDesc func = measure.getFunction();
            metrics2gt.put(func, gtColIdx);
            gtDataTypes.add(func.getReturnDataType());

            // map to column block
            int cbIdx = 0;
            for (HBaseColumnFamilyDesc familyDesc : cuboid.getCubeDesc().getHbaseMapping().getColumnFamily()) {
                for (HBaseColumnDesc hbaseColDesc : familyDesc.getColumns()) {
                    if (hbaseColDesc.containsMeasure(measure.getName())) {
                        metricsColBlocks.get(cbIdx).set(gtColIdx);
                    }
                    cbIdx++;
                }
            }

            gtColIdx++;
        }

        for (BitSet set : metricsColBlocks) {
            gtColBlocks.add(new ImmutableBitSet(set));
        }

        nMetrics = gtColIdx - nDimensions;
        assert nMetrics == cuboid.getCubeDesc().getMeasures().size();
    }

    public List<TblColRef> getCuboidDimensionsInGTOrder() {
        return cuboid.getColumns();
    }

    public DimensionEncoding[] getDimensionEncodings(IDimensionEncodingMap dimEncMap) {
        List<TblColRef> dims = cuboid.getColumns();
        DimensionEncoding[] dimEncs = new DimensionEncoding[dims.size()];
        for (int i = 0; i < dimEncs.length; i++) {
            dimEncs[i] = dimEncMap.get(dims.get(i));
        }
        return dimEncs;
    }

    public Map<Integer, Integer> getDependentMetricsMap() {
        Map<Integer, Integer> result = Maps.newHashMap();
        List<MeasureDesc> measures = cuboid.getCubeDesc().getMeasures();
        for (MeasureDesc child : measures) {
            if (child.getDependentMeasureRef() != null) {
                boolean ok = false;
                for (MeasureDesc parent : measures) {
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
        return "Cuboid " + cuboid.getId();
    }
}
