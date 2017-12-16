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

package io.kyligence.kap.cube.cuboid;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;

public class NCuboidLayoutChooser {
    public static NCuboidLayout selectCuboidLayout(NDataSegment segment, Set<TblColRef> dimensions,
            Set<TblColRef> filterColumns, Set<FunctionDesc> metrics) {
        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        return spanningTree.findBestMatching(new NQueryMatchVisitor(segment, dimensions, filterColumns, metrics));
    }

    public static NCuboidLayout selectCuboidLayout(NDataSegment segment, Set<Integer> dimensions,
            Set<Integer> measures) {
        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        return spanningTree.findBestMatching(new NStorageMatchVisitor(segment, dimensions, measures));
    }

    private static class NQueryMatchVisitor implements NSpanningTree.ICuboidTreeVisitor {
        final Comparator<NCuboidLayout> l1Comparator; // compare cuboid rows
        final Comparator<NCuboidLayout> l2Comparator; // compare cuboid layout
        final ImmutableBitSet dimensionBitSet;
        final ImmutableBitSet measureBitSet;
        final ImmutableBitSet filterBitSet;
        final NDataSegment segment;

        NCuboidLayout bestCuboidLayout = null;

        private NQueryMatchVisitor(NDataSegment segment, Set<TblColRef> dimensions, Set<TblColRef> filterColumns,
                Set<FunctionDesc> metrics) {
            this.segment = segment;

            NDataModel model = (NDataModel) segment.getModel();
            Map<TblColRef, Integer> colIdMap = model.getEffectiveColsMap().inverse();
            BitSet dimSet = new BitSet();
            BitSet filterSet = new BitSet();
            for (TblColRef dimColRef : dimensions) {
                Integer dimId = colIdMap.get(dimColRef);
                Preconditions.checkNotNull(dimId, "Dimensions Column %s not found on model %s. ", dimColRef,
                        model.getName());
                dimSet.set(dimId);
                if (filterColumns.contains(dimColRef)) {
                    filterSet.set(dimId);
                }
            }
            dimensionBitSet = new ImmutableBitSet(dimSet);
            filterBitSet = new ImmutableBitSet(filterSet);

            BitSet measureSet = new BitSet();
            for (FunctionDesc funcDesc : metrics) {
                int measureId = -1;
                for (Map.Entry<Integer, NDataModel.Measure> modelMeasureEntry : model.getEffectiveMeasureMap()
                        .entrySet()) {
                    if (modelMeasureEntry.getValue().getFunction().equals(funcDesc)) {
                        measureId = modelMeasureEntry.getKey();
                        break;
                    }
                }
                Preconditions.checkState(measureId >= 0, "Aggregation %s not found on model %s. ", funcDesc,
                        model.getName());
                measureSet.set(measureId);
            }
            measureBitSet = new ImmutableBitSet(measureSet);

            l1Comparator = NCuboidLayoutComparators.scored(segment.getSegDetails().getCuboidRowsMap());
            l2Comparator = NCuboidLayoutComparators.matchQueryPattern(dimensionBitSet, filterBitSet, measureBitSet);
        }

        @Override
        public boolean visit(NCuboidDesc cuboidDesc) {
            // ========== check cuboid_desc from here ==========
            Collection<NCuboidLayout> availableLayouts = Collections2.filter(cuboidDesc.getLayouts(),
                    new Predicate<NCuboidLayout>() {
                        @Override
                        public boolean apply(@Nullable NCuboidLayout input) {
                            if (input == null)
                                return false;

                            NDataCuboid cuboid = segment.getCuboid(input.getId());
                            return cuboid != null && cuboid.getStatus() == SegmentStatusEnum.READY;
                        }
                    });

            if (availableLayouts.isEmpty()) {
                return true;
            }

            // ensure all dimension column exists, TODO: consider dimension as measure
            if (!dimensionBitSet.andNot(cuboidDesc.getEffectiveDimBitSetIncludingDerived()).isEmpty()) {
                return false;
            }

            // ensure all measure exists
            if (!measureBitSet.andNot(cuboidDesc.getMeasureSet()).isEmpty()) {
                return false;
            }

            // ========== compare matched cuboid layout from here ==========

            if (bestCuboidLayout != null
                    && l1Comparator.compare(bestCuboidLayout, Iterables.get(availableLayouts, 0)) >= 0) {
                // input cuboid_desc has more rows than bestCuboidLayout, then skip all layouts under input cuboid_desc
                return true;
            }

            bestCuboidLayout = Collections.max(cuboidDesc.getLayouts(), l2Comparator);
            return true;
        }

        @Override
        public NCuboidLayout getMatched() {
            return bestCuboidLayout;
        }
    }

    private static class NStorageMatchVisitor implements NSpanningTree.ICuboidTreeVisitor {
        final Comparator<NCuboidLayout> smalllestComparator;
        final ImmutableBitSet dimensionBitSet;
        final ImmutableBitSet measureBitSet;
        final NDataSegment segment;

        NCuboidLayout bestCuboidLayout = null;

        private NStorageMatchVisitor(NDataSegment segment, Set<Integer> dimensions, Set<Integer> measures) {
            this.segment = segment;
            BitSet dimSet = new BitSet();
            BitSet measureSet = new BitSet();
            for (int id : dimensions) {
                dimSet.set(id);
            }
            dimensionBitSet = new ImmutableBitSet(dimSet);
            for (int id : measures) {
                measureSet.set(id);
            }
            measureBitSet = new ImmutableBitSet(measureSet);
            smalllestComparator = NCuboidLayoutComparators.cheapest();
        }

        @Override
        public boolean visit(NCuboidDesc cuboidDesc) {
            // ensure all dimension column exists, TODO: consider dimension as measure
            if (!dimensionBitSet.andNot(cuboidDesc.getEffectiveDimBitSetIncludingDerived()).isEmpty()) {
                return false;
            }

            // if dimensions match but measures not, try to find from its children.
            if (!measureBitSet.andNot(cuboidDesc.getMeasureSet()).isEmpty()) {
                return true;
            }

            Collection<NCuboidLayout> availableLayouts = Collections2.filter(cuboidDesc.getLayouts(),
                    new Predicate<NCuboidLayout>() {
                        @Override
                        public boolean apply(@Nullable NCuboidLayout input) {
                            if (input == null)
                                return false;

                            NDataCuboid cuboid = segment.getCuboid(input.getId());
                            return cuboid != null && cuboid.getStatus() == SegmentStatusEnum.READY;
                        }
                    });

            if (availableLayouts.isEmpty()) {
                return false;
            }

            if (bestCuboidLayout != null) {
                availableLayouts = new ArrayList<>(availableLayouts); // make modifiable
                availableLayouts.add(bestCuboidLayout);
            }

            bestCuboidLayout = Collections.min(availableLayouts, smalllestComparator);
            return true;
        }

        @Override
        public NCuboidLayout getMatched() {
            return bestCuboidLayout;
        }
    }
}
