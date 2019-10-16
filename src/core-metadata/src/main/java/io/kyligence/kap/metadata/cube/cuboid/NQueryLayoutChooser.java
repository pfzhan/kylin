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
import java.util.stream.Collectors;

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
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
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

    private NQueryLayoutChooser() {
    }

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
                if (!matched) {
                    logger.info("Table index {} does not match sql {}, unmatched columns {}", cuboid, sqlDigest,
                            unmatchedCols);
                }
            }
            if (!indexEntity.isTableIndex() && !sqlDigest.isRawQuery) {
                unmatchedCols.addAll(sqlDigest.filterColumns);
                unmatchedCols.addAll(sqlDigest.groupbyColumns);
                matched = matchAggIndex(sqlDigest, cuboid.getLayout(), segment.getDataflow(), unmatchedCols,
                        unmatchedMetrics, needDerive, tempResult);
                if (!matched) {
                    logger.info("Agg index {} does not match sql {}, unmatched columns {}, unmatched metrics {}",
                            cuboid, sqlDigest, unmatchedCols, unmatchedMetrics);
                }
            }
            if (!matched) {
                continue;
            }

            LayoutEntity layout = cuboid.getLayout();
            NLayoutCandidate candidate = new NLayoutCandidate(layout);
            candidate.setCost(cuboid.getRows() * (tempResult.influences.size() + 1.0));
            if (!needDerive.isEmpty()) {
                candidate.setDerivedToHostMap(needDerive);
            }
            candidates.add(candidate);
            candidateCapabilityResultMap.put(candidate, tempResult);
        }

        if (candidates.isEmpty()) {
            return null;
        }
        sortCandidates(candidates, segment, sqlDigest);
        NLayoutCandidate chosenCandidate = candidates.get(0);
        return new Pair<>(chosenCandidate, candidateCapabilityResultMap.get(chosenCandidate).influences);
    }

    private static void sortCandidates(List<NLayoutCandidate> candidates, NDataSegment segment, SQLDigest sqlDigest) {
        final KylinConfig config = segment.getConfig();
        final Set<TblColRef> filterColSet = ImmutableSet.copyOf(sqlDigest.filterColumns);
        final List<TblColRef> filterCols = Lists.newArrayList(filterColSet);
        filterCols.sort(ComparatorUtils.filterColComparator(config, segment.getProject()));

        final Set<TblColRef> nonFilterColSet = sqlDigest.isRawQuery ? sqlDigest.allColumns.stream()
                .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE).collect(Collectors.toSet())
                : sqlDigest.groupbyColumns.stream()
                        .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE)
                        .collect(Collectors.toSet());
        final List<TblColRef> nonFilterColumns = Lists.newArrayList(nonFilterColSet);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());

        Ordering<NLayoutCandidate> ordering = Ordering //
                .from(derivedLayoutComparator()).compound(rowSizeComparator()) // L1 comparator, compare cuboid rows
                .compound(filterColumnComparator(filterCols, config, segment.getProject())) // L2 comparator, order filter columns
                .compound(dimensionSizeComparator()) // the lower dimension the best
                .compound(measureSizeComparator()) // L3 comparator, order size of cuboid columns
                .compound(nonFilterColumnComparator(nonFilterColumns, config)); // L4 comparator, order non-filter columns
        candidates.sort(ordering);
    }

    private static void unmatchedAggregations(Collection<FunctionDesc> aggregations, LayoutEntity cuboidLayout) {
        for (MeasureDesc measureDesc : cuboidLayout.getOrderedMeasures().values()) {
            aggregations.remove(measureDesc.getFunction());
        }
    }

    private static void unmatchedCountColumnIfExistCountStar(Collection<FunctionDesc> aggregations) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Iterator<FunctionDesc> iterator = aggregations.iterator();
        while (iterator.hasNext()) {
            FunctionDesc functionDesc = iterator.next();
            if (kylinConfig.isReplaceColCountWithCountStar() && functionDesc.isCountOnColumn()) {
                iterator.remove();
            }
        }
    }

    private static boolean matchAggIndex(SQLDigest sqlDigest, final LayoutEntity cuboidLayout, final NDataflow dataFlow,
            Set<TblColRef> unmatchedCols, Collection<FunctionDesc> unmatchedMetrics,
            Map<TblColRef, DeriveInfo> needDerive, CapabilityResult result) {
        unmatchedCols.removeAll(cuboidLayout.getOrderedDimensions().values());
        goThruDerivedDims(cuboidLayout.getIndex(), dataFlow, needDerive, unmatchedCols, cuboidLayout.getModel());
        unmatchedAggregations(unmatchedMetrics, cuboidLayout);
        unmatchedCountColumnIfExistCountStar(unmatchedMetrics);

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
            if (CollectionUtils.isEmpty(functionDesc.getParameters()))
                continue;
            List<TblColRef> neededCols = functionDesc.getColRefs();
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
        if (!influencingMeasures.isEmpty()) {
            logger.info("NDataflow {} CapabilityInfluences: {}", indexEntity.getIndexPlan().getUuid(),
                    StringUtils.join(influencingMeasures, ","));
        }
    }

    private static Comparator<NLayoutCandidate> derivedLayoutComparator() {
        return (layoutCandidate1, layoutCandidate2) -> {
            if (layoutCandidate1.getDerivedToHostMap().isEmpty() && !layoutCandidate2.getDerivedToHostMap().isEmpty()) {
                return -1;
            } else if (!layoutCandidate1.getDerivedToHostMap().isEmpty()
                    && layoutCandidate2.getDerivedToHostMap().isEmpty()) {
                return 1;
            }

            return 0;
        };
    }

    private static Comparator<NLayoutCandidate> rowSizeComparator() {
        return Comparator.comparingDouble(NLayoutCandidate::getCost);
    }

    private static Comparator<NLayoutCandidate> dimensionSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getCuboidLayout().getOrderedDimensions().size());
    }

    private static Comparator<NLayoutCandidate> measureSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getCuboidLayout().getOrderedMeasures().size());
    }

    private static Comparator<NLayoutCandidate> filterColumnComparator(List<TblColRef> sortedFilters,
            KylinConfig config, String project) {
        return Ordering.from(colComparator(sortedFilters, config)).compound(shardByComparator(sortedFilters, config));
    }

    private static Comparator<NLayoutCandidate> nonFilterColumnComparator(List<TblColRef> sortedNonFilters,
            KylinConfig config) {
        return colComparator(sortedNonFilters, config);
    }

    private static Comparator<NLayoutCandidate> colComparator(List<TblColRef> sortedCols, KylinConfig config) {
        return (layoutCandidate1, layoutCandidate2) -> {
            List<Integer> position1 = getColumnsPos(layoutCandidate1, config, sortedCols);
            List<Integer> position2 = getColumnsPos(layoutCandidate2, config, sortedCols);
            Iterator<Integer> iter1 = position1.iterator();
            Iterator<Integer> iter2 = position2.iterator();

            while (iter1.hasNext() && iter2.hasNext()) {
                int i1 = iter1.next();
                int i2 = iter2.next();

                int c = i1 - i2;
                if (c != 0)
                    return c;
            }

            return 0;
        };
    }

    private static Comparator<NLayoutCandidate> shardByComparator(List<TblColRef> columns, KylinConfig config) {
        return (candidate1, candidate2) -> {
            TblColRef shardByCol1 = null;
            List<Integer> shardByCols1 = candidate1.getCuboidLayout().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols1)) {
                TblColRef tmpCol = candidate1.getCuboidLayout().getOrderedDimensions().get(shardByCols1.get(0));
                for (TblColRef colRef : columns) {
                    if (colRef.equals(tmpCol)) {
                        shardByCol1 = colRef;
                        break;
                    }
                }
            }

            TblColRef shardByCol2 = null;
            List<Integer> shardByCols2 = candidate2.getCuboidLayout().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols2)) {
                TblColRef tmpCol = candidate1.getCuboidLayout().getOrderedDimensions().get(shardByCols2.get(0));
                for (TblColRef colRef : columns) {
                    if (colRef.equals(tmpCol)) {
                        shardByCol2 = colRef;
                        break;
                    }
                }
            }

            String project = candidate1.getCuboidLayout().getModel().getProject();
            return Ordering.from(ComparatorUtils.nullLastComparator())
                    .compound(ComparatorUtils.filterColComparator(config, project)).compare(shardByCol1, shardByCol2);
        };
    }

    private static List<Integer> getColumnsPos(final NLayoutCandidate candidate, KylinConfig config,
            List<TblColRef> sortedColumns) {

        List<Integer> positions = Lists.newArrayList();
        for (TblColRef col : sortedColumns) {
            DeriveInfo deriveInfo = candidate.getDerivedToHostMap().get(col);
            if (deriveInfo == null) {
                positions.add(getDimsIndexInLayout(col, candidate));
            } else {
                TblColRef[] hostCols = deriveInfo.columns;
                for (TblColRef hostCol : hostCols) {
                    positions.add(getDimsIndexInLayout(hostCol, candidate));
                }
            }
        }
        return positions;
    }

    private static int getDimsIndexInLayout(TblColRef tblColRef, final NLayoutCandidate candidate) {
        //get dimension
        Integer id = candidate.getCuboidLayout().getDimensionPos(tblColRef);
        return id == null ? -1 : candidate.getCuboidLayout().getColOrder().indexOf(id);
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
            Iterable<TblColRef> pksOnCuboid = Iterables.filter(pks, indexEntity::dimensionsDerive);
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
