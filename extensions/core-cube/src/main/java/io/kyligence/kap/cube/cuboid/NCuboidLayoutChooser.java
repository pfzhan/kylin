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
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
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

    public static NLayoutCandidate selectLayoutForQuery(NDataSegment segment, ImmutableSet<TblColRef> dimensions,
            ImmutableSet<TblColRef> filterColumns, ImmutableSet<FunctionDesc> metrics) {
        NSpanningTree spanningTree = segment.getCubePlan().getSpanningTree();
        NQuerySpanningTreeVisitor visitor = new NQuerySpanningTreeVisitor(segment, dimensions, filterColumns, metrics);
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
        final ImmutableSet<TblColRef> dimensions;
        final ImmutableSet<TblColRef> filterColumns;
        final ImmutableSet<FunctionDesc> metrics;

        Ordering<NLayoutCandidate> ordering;//according to the ordering, the smaller wins
        NLayoutCandidate bestLayoutCandidate = null;

        private NQuerySpanningTreeVisitor(final NDataSegment segment, ImmutableSet<TblColRef> dimensions,
                ImmutableSet<TblColRef> filterColumns, ImmutableSet<FunctionDesc> metrics) {
            this.segment = segment;
            this.model = (NDataModel) segment.getModel();
            this.dimensions = dimensions;
            this.filterColumns = filterColumns;
            this.metrics = metrics;
            //
            //            Map<TblColRef, Integer> colIdMap = model.getEffectiveColsMap().inverse();
            //            BitSet dimSet = new BitSet();
            //            BitSet filterSet = new BitSet();
            //            for (TblColRef dimColRef : dimensions) {
            //                Integer dimId = colIdMap.get(dimColRef);
            //
            //                if (dimId == null) {
            //                    //TODO: some layout need derive, some layout does not
            //                    // in case not defined as a normal dimension, a lookup table columna could be automatically derived
            //                    if (model.isLookupTable(dimColRef.getTableRef())) {
            //                        JoinDesc joinByPKSide = model.getJoinByPKSide(dimColRef.getTableRef());
            //                        Preconditions.checkNotNull(joinByPKSide);
            //                        TblColRef[] foreignKeyColumns = joinByPKSide.getForeignKeyColumns();
            //                        for (TblColRef fk : foreignKeyColumns) {
            //                            Integer fkId = colIdMap.get(fk);
            //                            Preconditions.checkNotNull(fkId,
            //                                    "To query derived columns on lookup table, you need to add FK to dimension: " + fk);
            //                            dimSet.set(fkId);
            //                            if (filterColumns.contains(dimColRef)) {
            //                                filterSet.set(fkId);
            //                            }
            //                        }
            //                    } else {
            //                        throw new IllegalStateException(String.format("Dimensions Column %s not defined on model %s",
            //                                dimColRef, model.getName()));
            //                    }
            //                } else {
            //                    dimSet.set(dimId);
            //                    if (filterColumns.contains(dimColRef)) {
            //                        filterSet.set(dimId);
            //                    }
            //                }
            //            }

            //dimensionBitSet = new ImmutableBitSet(dimSet);
            //filterBitSet = new ImmutableBitSet(filterSet);

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

            ordering = Ordering.natural().onResultOf(new Function<NLayoutCandidate, Comparable>() {
                //l1 comparator, compare cuboid rows
                @Nullable
                @Override
                public Comparable apply(@Nullable NLayoutCandidate input) {
                    return segment.getSegDetails().getCuboidRowsMap()
                            .get(Objects.requireNonNull(input).getCuboidLayout().getId()).doubleValue();
                }
            }).compound(
                    //l2 comparator, compare cuboid layout
                    NLayoutCandidateComparators.matchQueryPattern(dimensions, filterColumns, metrics));
        }

        @Override
        //TODO: revisit to check perf
        //TODO: add UT for chooser

        //TODO: should prefer layouts without derived
        public boolean visit(final NCuboidDesc cuboidDesc) {
            // ========== check cuboid_desc from here ==========

            final Map<TblColRef, DeriveInfo> needDerive = Maps.newHashMap();
            Set<TblColRef> unmatchedDims = Sets.newHashSet(this.dimensions);
            unmatchedDims.removeAll(cuboidDesc.getDimensionSet());

            goThruUnmatchedDims(cuboidDesc, needDerive, unmatchedDims);

            if (!unmatchedDims.isEmpty()) {
                return false;
            }

            // ensure all measure exists
            if (!measureBitSet.andNot(cuboidDesc.getMeasureBitset()).isEmpty()) {
                return true;
            }

            TreeSet<NLayoutCandidate> availableCandidates = new TreeSet<>(ordering);
            FluentIterable<NLayoutCandidate> transform = FluentIterable.from(cuboidDesc.getLayouts())
                    .filter(new Predicate<NCuboidLayout>() {
                        @Override
                        public boolean apply(@Nullable NCuboidLayout input) {
                            if (input == null)
                                return false;

                            NDataCuboid cuboid = segment.getCuboid(input.getId());
                            return cuboid != null && cuboid.getStatus() == SegmentStatusEnum.READY;
                        }
                    }).transform(new Function<NCuboidLayout, NLayoutCandidate>() {
                        @Override
                        public NLayoutCandidate apply(NCuboidLayout input) {
                            NLayoutCandidate candidate = new NLayoutCandidate(input);
                            if (!needDerive.isEmpty()) {
                                candidate.setDerivedToHostMap(needDerive);
                            }
                            return candidate;
                        }
                    });


            Iterables.addAll(availableCandidates, transform);

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

        private void goThruUnmatchedDims(final NCuboidDesc cuboidDesc, Map<TblColRef, DeriveInfo> needDeriveCollector,
                Set<TblColRef> unmatchedDims) {
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
                            needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK,
                                    joinByPKSide, new TblColRef[] { relatedCol }, true));
                            unmatchedDimItr.remove();
                            continue;
                        }
                    } else {
                        if (cuboidDesc.dimensionsDerive(foreignKeyColumns)) {
                            needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.LOOKUP,
                                    joinByPKSide, foreignKeyColumns, false));
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

        NLayoutCandidate getBestLayoutCandidate() {
            return bestLayoutCandidate;
        }
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
                return false;// ?? TODO: why false
            }

            if (bestCuboidLayout != null) {
                availableLayouts = new ArrayList<>(availableLayouts); // make modifiable
                availableLayouts.add(bestCuboidLayout);
            }

            bestCuboidLayout = Collections.min(availableLayouts, smalllestComparator);
            return true;
        }

        NCuboidLayout getBestLayout() {
            return bestCuboidLayout;
        }
    }
}
