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
import java.util.Set;

import org.apache.kylin.common.util.ImmutableBitSet;

import com.google.common.collect.Collections2;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;

public class NCuboidLayoutChooser {

    public static NCuboidLayout selectLayoutForBuild(NDataSegment segment, Set<Integer> dimensions,
            Set<Integer> measures) {
        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        NStorageSpanningTreeVisitor visitor = new NStorageSpanningTreeVisitor(segment, dimensions, measures);
        spanningTree.acceptVisitor(visitor);
        return visitor.getBestLayout();
    }

    private static class NStorageSpanningTreeVisitor implements NSpanningTree.ISpanningTreeVisitor {
        final Comparator<NCuboidLayout> smalllestComparator;
        final ImmutableBitSet dimensionBitSet;
        final ImmutableBitSet measureBitSet;
        final NDataSegment segment;

        NCuboidLayout bestCuboidLayout = null;

        private NStorageSpanningTreeVisitor(NDataSegment segment, Set<Integer> dimensions, Set<Integer> measures) {
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
            smalllestComparator = new Comparator<NCuboidLayout>() {
                @Override
                public int compare(NCuboidLayout o1, NCuboidLayout o2) {
                    return o1.getOrderedDimensions().size() - o2.getOrderedDimensions().size();
                }
            };
        }

        @Override
        public boolean visit(NCuboidDesc cuboidDesc) {
            // ensure all dimension column exists, TODO: consider dimension as measure
            if (!dimensionBitSet.andNot(cuboidDesc.getDimensionBitset()).isEmpty()) {
                return false;
            }

            // if dimensions match but measures not, try to find from its children.
            if (!measureBitSet.andNot(cuboidDesc.getMeasureBitset()).isEmpty()) {
                return true;
            }

            Collection<NCuboidLayout> availableLayouts = Collections2.filter(cuboidDesc.getLayouts(), input -> {
                if (input == null)
                    return false;

                NDataCuboid cuboid = segment.getCuboid(input.getId());
                return cuboid != null;
            });

            if (availableLayouts.isEmpty()) {
                return false;// ?? TODO: why false
            }

            if (bestCuboidLayout != null) {
                availableLayouts = new ArrayList<>(availableLayouts); // make modifiable
                availableLayouts.add(bestCuboidLayout);
            }

            bestCuboidLayout = Collections.min(availableLayouts, smalllestComparator);
            return true;
        }

        @Override
        public NLayoutCandidate getBestLayoutCandidate() {
            return null;
        }

        NCuboidLayout getBestLayout() {
            return bestCuboidLayout;
        }
    }
}
