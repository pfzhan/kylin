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

package io.kyligence.kap.metadata.cube.cuboid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;

public class NQueryLayoutChooser {
    private static final Logger logger = LoggerFactory.getLogger(NQueryLayoutChooser.class);

    public static Pair<NLayoutCandidate, List<CapabilityResult.CapabilityInfluence>> selectCuboidLayout(
            NDataSegment segment, SQLDigest sqlDigest) {
        if (segment == null) {
            logger.info("Exclude this segments because there are no ready segments");
            return null;
        }
        List<NLayoutCandidate> candidates = new ArrayList<>();

        Map<NLayoutCandidate, CapabilityResult> candidateCapabilityResultMap = Maps.newHashMap();
        for (NDataLayout cuboid : segment.getSegDetails().getLayouts()) {
            CapabilityResult tempResult = new CapabilityResult();
            // check indexEntity
            IndexEntity indexEntity = segment.getIndexPlan().getIndexEntity(cuboid.getIndexId());

            Set<TblColRef> unmatchedCols = Sets.newHashSet();
            Set<FunctionDesc> unmatchedMetrics = Sets.newHashSet(sqlDigest.aggregations);
            boolean matched = false;
            final Map<TblColRef, DeriveInfo> needDerive = Maps.newHashMap();
            if (indexEntity.isTableIndex() && sqlDigest.isRawQuery) {
                unmatchedCols.addAll(sqlDigest.allColumns);
                matched = matchTableIndex(cuboid.getLayout(), segment.getDataflow(), unmatchedCols, needDerive,
                        tempResult);
            }
            if (!indexEntity.isTableIndex() && !sqlDigest.isRawQuery) {
                unmatchedCols.addAll(sqlDigest.filterColumns);
                unmatchedCols.addAll(sqlDigest.groupbyColumns);
                matched = matchAggIndex(sqlDigest, cuboid.getLayout(), segment.getDataflow(), unmatchedCols,
                        unmatchedMetrics, needDerive, tempResult);
            }
            if (matched) {
                LayoutEntity layout = cuboid.getLayout();
                NLayoutCandidate candidate = new NLayoutCandidate(layout);
                candidate.setCost(cuboid.getRows() * (tempResult.influences.size() + 1.0));
                if (!needDerive.isEmpty()) {
                    candidate.setDerivedToHostMap(needDerive);
                }
                candidates.add(candidate);
                candidateCapabilityResultMap.put(candidate, tempResult);
            }
        }

        if (candidates.isEmpty()) {
            return null;
        } else {
            Ordering<NLayoutCandidate> ordering = Ordering.natural().onResultOf(L1Comparator())
                    .compound(L2Comparator(ImmutableSet.copyOf(sqlDigest.filterColumns), segment.getConfig()))
                    .compound(L3Comparator());
            candidates.sort(ordering);
            NLayoutCandidate chosenCandidate = candidates.get(0);
            return new Pair<>(chosenCandidate, candidateCapabilityResultMap.get(chosenCandidate).influences);
        }
    }

    private static void unmatchedAggregations(Collection<FunctionDesc> aggregations, LayoutEntity cuboidLayout) {
        for (MeasureDesc measureDesc : cuboidLayout.getOrderedMeasures().values()) {
            aggregations.remove(measureDesc.getFunction());
        }
    }

    private static boolean matchAggIndex(SQLDigest sqlDigest, final LayoutEntity cuboidLayout, final NDataflow dataFlow,
            Set<TblColRef> unmatchedCols, Collection<FunctionDesc> unmatchedMetrics,
            Map<TblColRef, DeriveInfo> needDerive, CapabilityResult result) {
        unmatchedCols.removeAll(cuboidLayout.getOrderedDimensions().values());
        goThruDerivedDims(cuboidLayout.getIndex(), dataFlow, needDerive, unmatchedCols, cuboidLayout.getModel());
        unmatchedAggregations(unmatchedMetrics, cuboidLayout);

        removeUnmatchedGroupingAgg(unmatchedMetrics);
        if (!unmatchedMetrics.isEmpty() || !unmatchedCols.isEmpty()) {
            applyAdvanceMeasureStrategy(cuboidLayout.getIndex(), sqlDigest, unmatchedCols, unmatchedMetrics, result);
            applyDimAsMeasureStrategy(cuboidLayout.getIndex(), unmatchedMetrics, result);
        }

        return unmatchedCols.isEmpty() && unmatchedMetrics.isEmpty();
    }

    private static boolean matchTableIndex(final LayoutEntity cuboidLayout, final NDataflow dataflow,
            Set<TblColRef> unmatchedCols, Map<TblColRef, DeriveInfo> needDerive, CapabilityResult result) {
        unmatchedCols.removeAll(cuboidLayout.getOrderedDimensions().values());
        goThruDerivedDims(cuboidLayout.getIndex(), dataflow, needDerive, unmatchedCols, cuboidLayout.getModel());
        if (!unmatchedCols.isEmpty()) {
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.TABLE_INDEX_MISSING_COLS);
            return false;
        }
        return true;
    }

    private static void removeUnmatchedGroupingAgg(Collection<FunctionDesc> unmatchedAggregations) {
        if (CollectionUtils.isEmpty(unmatchedAggregations))
            return;

        unmatchedAggregations
                .removeIf(functionDesc -> FunctionDesc.FUNC_GROUPING.equalsIgnoreCase(functionDesc.getExpression()));
    }

    private static void applyDimAsMeasureStrategy(IndexEntity indexEntity, Collection<FunctionDesc> unmatchedAggs,
            CapabilityResult result) {
        Iterator<FunctionDesc> it = unmatchedAggs.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();
            if (functionDesc.isCountConstant()) {
                it.remove();
                continue;
            }

            // calcite can do aggregation from columns on-the-fly
            ParameterDesc parameterDesc = functionDesc.getParameter();
            if (parameterDesc == null)
                continue;
            List<TblColRef> neededCols = parameterDesc.getColRefs();
            if (!indexEntity.getDimensionSet().containsAll(neededCols))
                continue;

            if (FunctionDesc.DIMENSION_AS_MEASURES.contains(functionDesc.getExpression())) {
                result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
            }
        }
    }

    private static void applyAdvanceMeasureStrategy(IndexEntity indexEntity, SQLDigest digest,
            Collection<TblColRef> unmatchedDims, Collection<FunctionDesc> unmatchedMetrics, CapabilityResult result) {
        List<String> influencingMeasures = Lists.newArrayList();
        for (MeasureDesc measure : indexEntity.getMeasureSet()) {
            MeasureType measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;

            CapabilityResult.CapabilityInfluence inf = measureType.influenceCapabilityCheck(unmatchedDims,
                    unmatchedMetrics, digest, measure);
            if (inf != null) {
                result.influences.add(inf);
                influencingMeasures.add(measure.getName() + "@" + measureType.getClass());
            }
        }
        if (influencingMeasures.size() != 0)
            logger.info("NDataflow {} CapabilityInfluences: {}", indexEntity.getIndexPlan().getUuid(),
                    StringUtils.join(influencingMeasures, ","));
    }

    private static Function<NLayoutCandidate, Comparable> L1Comparator() {
        return new Function<NLayoutCandidate, Comparable>() {
            //L1 comparator, compare cuboid rows
            @Override
            public Comparable apply(NLayoutCandidate input) {
                return input.getCost();
            }
        };
    }

    private static Comparator<NLayoutCandidate> L3Comparator() {
        //L3 comparator, compare cuboid columns
        return new Comparator<NLayoutCandidate>() {
            @Override
            public int compare(NLayoutCandidate o1, NLayoutCandidate o2) {
                return o1.getCuboidLayout().getColOrder().size() - o2.getCuboidLayout().getColOrder().size();
            }
        };
    }

    private static Comparator<NLayoutCandidate> L2Comparator(ImmutableSet<TblColRef> filterColumns,
            KylinConfig config) {
        //L2 comparator, compare cuboid layout
        return NLayoutCandidateComparators.matchQueryPattern(filterColumns, config);
    }

    private static void goThruDerivedDims(final IndexEntity indexEntity, final NDataflow dataflow,
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
                    if (indexEntity.dimensionsDerive(relatedCol)) {
                        needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide,
                                new TblColRef[] { relatedCol }, true));
                        unmatchedDimItr.remove();
                        continue;
                    }
                } else {
                    if (indexEntity.dimensionsDerive(foreignKeyColumns)
                            && dataflow.getLatestReadySegment().getSnapshots().containsKey(unmatchedDim.getTable())) {
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
                    return indexEntity.dimensionsDerive(input);
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

}
