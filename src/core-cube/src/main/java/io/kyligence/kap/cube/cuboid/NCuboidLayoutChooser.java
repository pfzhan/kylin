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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;

public class NCuboidLayoutChooser {

    public static NLayoutCandidate selectLayoutForQuery(NDataSegment segment, ImmutableSet<TblColRef> allCols,
            ImmutableSet<TblColRef> dimensions, ImmutableSet<TblColRef> filterColumns,
            ImmutableSet<FunctionDesc> metrics, boolean isRawQuery) {

        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        NSpanningTree.ISpanningTreeVisitor visitor = new NQuerySpanningTreeVisitor(segment, allCols, dimensions,
                filterColumns, metrics, isRawQuery);
        spanningTree.acceptVisitor(visitor);
        return visitor.getBestLayoutCandidate();
    }

    public static NCuboidLayout selectLayoutForBuild(NDataSegment segment, Set<Integer> dimensions,
            Set<Integer> measures) {
        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        NStorageSpanningTreeVisitor visitor = new NStorageSpanningTreeVisitor(segment, dimensions, measures);
        spanningTree.acceptVisitor(visitor);
        return visitor.getBestLayout();
    }

    private static class NQuerySpanningTreeVisitor implements NSpanningTree.ISpanningTreeVisitor {
        //final ImmutableBitSet dimensionBitSet;
        //final ImmutableBitSet filterBitSet;
        final ImmutableBitSet measureBitSet;

        final NDataSegment segment;
        final NDataModel model;
        final ImmutableSet<TblColRef> cols;
        final ImmutableSet<TblColRef> filterColumns;
        final ImmutableSet<FunctionDesc> metrics;
        final boolean isRawQuery;

        Ordering<NLayoutCandidate> ordering;//according to the ordering, the smaller wins
        NLayoutCandidate bestLayoutCandidate = null;

        private NQuerySpanningTreeVisitor(final NDataSegment segment, ImmutableSet<TblColRef> allCols,
                ImmutableSet<TblColRef> dimensions, ImmutableSet<TblColRef> filterColumns,
                ImmutableSet<FunctionDesc> metrics, boolean isRawQuery) {
            this.segment = segment;
            this.model = (NDataModel) segment.getModel();
            this.cols = isRawQuery ? allCols : dimensions;
            this.filterColumns = filterColumns;
            this.metrics = metrics;
            this.isRawQuery = isRawQuery;

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
            ordering = Ordering.natural().onResultOf(L1Comparator(segment)).compound(L2Comparator(filterColumns));
        }

        @Override
        //TODO: revisit to check perf
        //TODO: add UT for chooser
        //TODO: should prefer layouts without derived
        public boolean visit(final NCuboidDesc cuboidDesc) {
            // ========== check cuboid_desc from here ==========

            final Map<TblColRef, DeriveInfo> needDerive = Maps.newHashMap();
            Set<TblColRef> unmatchedDims = Sets.newHashSet(this.cols);
            unmatchedDims.removeAll(cuboidDesc.getDimensionSet());

            goThruUnmatchedDims(cuboidDesc, needDerive, unmatchedDims, this.model);

            if (!unmatchedDims.isEmpty()) {
                return false;
            }

            // ensure all measure exists
            if (!measureBitSet.andNot(cuboidDesc.getMeasureBitset()).isEmpty()) {
                return true;
            }

            TreeSet<NLayoutCandidate> availableCandidates = new TreeSet<>(ordering);
            Stream<NLayoutCandidate> transform = cuboidDesc.getLayouts().stream().filter(input -> {
                if (input == null || (isRawQuery != input.getCuboidDesc().isTableIndex())) {
                    return false;
                }
                NDataCuboid cuboid = segment.getCuboid(input.getId());
                return cuboid != null;
            }).map(input -> {
                NLayoutCandidate candidate = new NLayoutCandidate(input);
                if (!needDerive.isEmpty()) {
                    candidate.setDerivedToHostMap(needDerive);
                }
                return candidate;
            });

            Iterables.addAll(availableCandidates, transform.collect(Collectors.toList()));

            if (availableCandidates.isEmpty()) {
                return true;
            }

            // ========== compare matched cuboid layout from here ==========
            if (bestLayoutCandidate != null) {
                availableCandidates.add(bestLayoutCandidate);
            }
            bestLayoutCandidate = availableCandidates.first();
            return true;
        }

        public NLayoutCandidate getBestLayoutCandidate() {
            return bestLayoutCandidate;
        }
    }

    private static void goThruUnmatchedDims(final NCuboidDesc cuboidDesc,
            Map<TblColRef, DeriveInfo> needDeriveCollector, Set<TblColRef> unmatchedDims, NDataModel model) {
        Iterator<TblColRef> unmatchedDimItr = unmatchedDims.iterator();
        while (unmatchedDimItr.hasNext()) {
            TblColRef unmatchedDim = unmatchedDimItr.next();
            if (model.isLookupTable(unmatchedDim.getTableRef())) {
                JoinDesc joinByPKSide = model.getJoinByPKSide(unmatchedDim.getTableRef());
                Preconditions.checkNotNull(joinByPKSide);
                TblColRef[] foreignKeyColumns = joinByPKSide.getForeignKeyColumns();
                TblColRef[] primaryKeyColumns = joinByPKSide.getPrimaryKeyColumns();

                if (ArrayUtils.contains(primaryKeyColumns, unmatchedDim)) {
                    TblColRef relatedCol = foreignKeyColumns[ArrayUtils.indexOf(primaryKeyColumns, unmatchedDim)];
                    if (cuboidDesc.dimensionsDerive(relatedCol)) {
                        needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide,
                                new TblColRef[] { relatedCol }, true));
                        unmatchedDimItr.remove();
                        continue;
                    }
                } else {
                    if (cuboidDesc.dimensionsDerive(foreignKeyColumns)) {
                        needDeriveCollector.put(unmatchedDim,
                                new DeriveInfo(DeriveInfo.DeriveType.LOOKUP, joinByPKSide, foreignKeyColumns, false));
                        unmatchedDimItr.remove();
                        continue;
                    }
                }
            }

            // in some rare cases, FK needs to be derived from PK
            ImmutableCollection<TblColRef> pks = model.getFk2Pk().get(unmatchedDim);
            Iterable<TblColRef> pksOnCuboid = Iterables.filter(pks, new Predicate<TblColRef>() {
                @Override
                public boolean apply(@Nullable TblColRef input) {
                    return cuboidDesc.dimensionsDerive(input);
                }
            });
            TblColRef pk = Iterables.getFirst(pksOnCuboid, null);
            if (pk != null) {
                JoinDesc joinByPKSide = model.getJoinByPKSide(pk.getTableRef());
                Preconditions.checkNotNull(joinByPKSide);
                needDeriveCollector.put(unmatchedDim,
                        new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide, new TblColRef[] { pk }, true));
                unmatchedDimItr.remove();
            }
        }
    }

    private static Function<NLayoutCandidate, Comparable> L1Comparator(final NDataSegment segment) {
        return new Function<NLayoutCandidate, Comparable>() {
            //L1 comparator, compare cuboid rows
            @Nullable
            @Override
            public Comparable apply(@Nullable NLayoutCandidate input) {
                Preconditions.checkNotNull(input);
                long id = input.getCuboidLayout().getId();
                return segment.getSegDetails().getCuboidRowsMap().get(id).doubleValue();
            }
        };
    }

    private static Comparator<NLayoutCandidate> L2Comparator(ImmutableSet<TblColRef> filterColumns) {
        //L2 comparator, compare cuboid layout
        return NLayoutCandidateComparators.matchQueryPattern(filterColumns);
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
