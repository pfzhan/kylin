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

package io.kyligence.kap.modeling.auto.tuner.step;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.modeling.auto.ModelingContext;
import io.kyligence.kap.modeling.auto.util.Constants;
import io.kyligence.kap.modeling.auto.util.CubeDescUtil;

public abstract class AbstractStep {
    final CubeDesc workCubeDesc;
    final CubeDesc origCubeDesc;
    final ModelingContext context;

    AbstractStep(ModelingContext context, CubeDesc origCubeDesc, CubeDesc workCubeDesc) {
        this.workCubeDesc = workCubeDesc;
        this.origCubeDesc = origCubeDesc;
        this.context = context;
    }

    boolean isSkipped() {
        return false;
    }

    public void optimize() throws Exception {
        if (!isSkipped()) {
            beforeOptimize();
            doOptimize();
            afterOptimize();
        }
    }

    abstract void doOptimize();

    void beforeOptimize() throws Exception {
        Preconditions.checkNotNull(workCubeDesc);
        Preconditions.checkNotNull(origCubeDesc);
    }

    void afterOptimize() throws Exception {
        adjustCubeOverrideProps();

        workCubeDesc.init(context.getKylinConfig());
        workCubeDesc.setSignature(workCubeDesc.calculateSignature());
    }

    private void adjustCubeOverrideProps() {
        long combinationMax = 0;
        for (AggregationGroup aggGroup : workCubeDesc.getAggregationGroups()) {
            combinationMax = Math.max(combinationMax, aggGroup.calculateCuboidCombination());
        }
        long combinationMaxConfig = Long.highestOneBit(combinationMax) << 1;
        workCubeDesc.getOverrideKylinProps().put("kylin.cube.aggrgroup.max-combination", Long.toString(combinationMaxConfig));
    }

    void adjustAfterDimensionChanged() {
        // backup original rowkeys to a map
        Map<String, RowKeyColDesc> origRowKeyMap = Maps.newHashMap();
        for (RowKeyColDesc origRowKeyDesc : origCubeDesc.getRowkey().getRowKeyColumns()) {
            origRowKeyMap.put(origRowKeyDesc.getColRef().getIdentity(), origRowKeyDesc);
        }

        // update RowKeyColDesc in CubeDesc according to dimension updates
        Set<RowKeyColDesc> rowKeyColDescs = Sets.newHashSet();
        for (DimensionDesc dimDesc : workCubeDesc.getDimensions()) {
            if (dimDesc.isDerived()) {
                for (TblColRef colRef : dimDesc.getJoin().getForeignKeyColumns()) {
                    if (origRowKeyMap.containsKey(colRef.getIdentity())) {
                        rowKeyColDescs.add(origRowKeyMap.get(colRef.getIdentity()));
                    } else {
                        RowKeyColDesc rowKeyColDesc = new RowKeyColDesc();
                        rowKeyColDesc.setShardBy(false);
                        rowKeyColDesc.setEncoding(Constants.DIM_ENCODING_DEFAULT);
                        rowKeyColDesc.setColumn(colRef.getIdentity());
                        rowKeyColDescs.add(rowKeyColDesc);
                    }
                }
            } else {
                TblColRef colRef = dimDesc.getColumnRefs()[0]; // normal dimension only has one col.
                if (origRowKeyMap.containsKey(colRef.getIdentity())) {
                    rowKeyColDescs.add(origRowKeyMap.get(colRef.getIdentity()));
                } else {
                    RowKeyColDesc rowKeyColDesc = new RowKeyColDesc();
                    rowKeyColDesc.setShardBy(false);
                    rowKeyColDesc.setEncoding(Constants.DIM_ENCODING_DEFAULT);
                    rowKeyColDesc.setColumn(dimDesc.getColumnRefs()[0].getIdentity());
                    rowKeyColDescs.add(rowKeyColDesc);
                }
            }
        }
        workCubeDesc.getRowkey().setRowkeyColumns(rowKeyColDescs.toArray(new RowKeyColDesc[0]));

        // update AggGroup according to rowkey updates
        Collection<RowKeyColDesc> added = Sets.newHashSet(rowKeyColDescs);
        added.removeAll(origRowKeyMap.values());
        Collection<RowKeyColDesc> deleted = Sets.newHashSet(origRowKeyMap.values());
        deleted.removeAll(rowKeyColDescs);
        for (AggregationGroup aggGroup : workCubeDesc.getAggregationGroups()) {
            for (RowKeyColDesc add : added) {
                CubeDescUtil.addRowKeyToAggGroup(aggGroup, add.getColumn());
            }
            for (RowKeyColDesc del : deleted) {
                CubeDescUtil.removeRowKeyFromAggGroup(aggGroup, del.getColumn());
            }
        }
    }
}
