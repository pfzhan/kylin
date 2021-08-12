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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinTimeoutException;
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
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.model.ExcludedLookupChecker;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NQueryLayoutChooser {
    private static final Logger logger = LoggerFactory.getLogger(NQueryLayoutChooser.class);

    private NQueryLayoutChooser() {
    }

    public static NLayoutCandidate selectPartialLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest) {

        NLayoutCandidate candidate = null;
        List<NDataSegment> toRemovedSegments = Lists.newArrayList();
        for (NDataSegment segment : prunedSegments) {
            if (candidate == null) {
                candidate = selectLayoutCandidate(dataflow, Lists.newArrayList(segment), sqlDigest);
                if (candidate == null) {
                    toRemovedSegments.add(segment);
                }
            } else if (segment.getSegDetails().getLayoutById(candidate.getLayoutEntity().getId()) == null) {
                toRemovedSegments.add(segment);
            }
        }
        prunedSegments.removeAll(toRemovedSegments);
        return candidate;
    }

    public static NLayoutCandidate selectLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest) {

        if (CollectionUtils.isEmpty(prunedSegments)) {
            logger.info("There is no segment to answer sql");
            return NLayoutCandidate.EMPTY;
        }
        List<NLayoutCandidate> candidates = new ArrayList<>();
        val commonLayouts = getLayoutsFromSegments(prunedSegments, dataflow);
        val model = dataflow.getModel();
        val isBatchFusionModel = model.isFusionModel() && !dataflow.isStreaming();
        logger.info("Matching dataflow with seg num: {} layout num: {}", prunedSegments.size(), commonLayouts.size());
        for (NDataLayout dataLayout : commonLayouts) {
            logger.trace("Matching layout {}", dataLayout);
            CapabilityResult tempResult = new CapabilityResult();
            // check indexEntity
            IndexEntity indexEntity = dataflow.getIndexPlan().getIndexEntity(dataLayout.getIndexId());
            LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(dataLayout.getLayoutId());
            logger.trace("Matching indexEntity {}", indexEntity);

            boolean matched = false;
            final Map<TblColRef, DeriveInfo> needDerive = Maps.newHashMap();

            if (needTableIndexMatch(dataflow, sqlDigest, indexEntity)) {
                matched = matchTableIndex(sqlDigest, layout, model, needDerive, tempResult, isBatchFusionModel);
            } else if (needAggIndexMatch(sqlDigest, indexEntity)) {
                matched = matchAggIndex(sqlDigest, layout, model, needDerive, tempResult, isBatchFusionModel);
            }
            if (!matched) {
                logger.trace("Matching failed");
                continue;
            }

            NLayoutCandidate candidate = new NLayoutCandidate(layout);
            candidate.setCost(dataLayout.getRows() * (tempResult.influences.size() + 1.0));
            if (!needDerive.isEmpty()) {
                candidate.setDerivedToHostMap(needDerive);
            }
            candidate.setCapabilityResult(tempResult);
            candidates.add(candidate);
        }

        if (Thread.interrupted()) {
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s. Current step: Layout chooser. ");
        }

        logger.info("Matched candidates num : {}", candidates.size());
        if (candidates.isEmpty()) {
            return null;
        }
        sortCandidates(candidates, dataflow, sqlDigest);
        return candidates.get(0);
    }

    private static boolean needAggIndexMatch(SQLDigest sqlDigest, IndexEntity indexEntity) {
        return !indexEntity.isTableIndex() && !sqlDigest.isRawQuery;
    }

    private static boolean needTableIndexMatch(NDataflow dataflow, SQLDigest sqlDigest, IndexEntity indexEntity) {
        return indexEntity.isTableIndex()
                && (sqlDigest.isRawQuery || dataflow.getConfig().isUseTableIndexAnswerNonRawQuery());
    }

    private static Collection<NDataLayout> getLayoutsFromSegments(List<NDataSegment> segments, NDataflow dataflow) {
        val projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(dataflow.getProject());
        if (!projectInstance.getConfig().isHeterogeneousSegmentEnabled()) {
            return dataflow.getLatestReadySegment().getLayoutsMap().values();
        }

        val commonLayouts = Maps.<Long, NDataLayout> newHashMap();
        if (CollectionUtils.isEmpty(segments)) {
            return commonLayouts.values();
        }

        for (int i = 0; i < segments.size(); i++) {
            val dataSegment = segments.get(i);
            val layoutIdMapToDataLayout = dataSegment.getLayoutsMap();
            if (i == 0) {
                commonLayouts.putAll(layoutIdMapToDataLayout);
            } else {
                commonLayouts.keySet().retainAll(layoutIdMapToDataLayout.keySet());
            }
        }

        return commonLayouts.values();
    }

    private static void sortCandidates(List<NLayoutCandidate> candidates, NDataflow dataflow, SQLDigest sqlDigest) {
        final KylinConfig config = dataflow.getConfig();
        final Set<TblColRef> filterColSet = ImmutableSet.copyOf(sqlDigest.filterColumns);
        final List<TblColRef> filterCols = Lists.newArrayList(filterColSet);
        filterCols.sort(ComparatorUtils.filterColComparator(config, dataflow.getProject()));

        final Set<TblColRef> nonFilterColSet = sqlDigest.isRawQuery ? sqlDigest.allColumns.stream()
                .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE).collect(Collectors.toSet())
                : sqlDigest.groupbyColumns.stream()
                        .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE)
                        .collect(Collectors.toSet());
        final List<TblColRef> nonFilterColumns = Lists.newArrayList(nonFilterColSet);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());

        Ordering<NLayoutCandidate> ordering = Ordering //
                .from(derivedLayoutComparator()).compound(rowSizeComparator()) // L1 comparator, compare cuboid rows
                .compound(filterColumnComparator(filterCols, config, dataflow.getProject())) // L2 comparator, order filter columns
                .compound(dimensionSizeComparator()) // the lower dimension the best
                .compound(measureSizeComparator()) // L3 comparator, order size of cuboid columns
                .compound(nonFilterColumnComparator(nonFilterColumns)); // L4 comparator, order non-filter columns
        candidates.sort(ordering);
    }

    private static void unmatchedAggregations(Collection<FunctionDesc> aggregations, LayoutEntity layoutEntity,
            boolean isBatchFusionModel) {
        List<MeasureDesc> functionDescs = new ArrayList<>();
        if (isBatchFusionModel) {
            functionDescs.addAll(layoutEntity.getStreamingMeasures());
        }
        functionDescs.addAll(layoutEntity.getOrderedMeasures().values());

        for (MeasureDesc measureDesc : functionDescs) {
            aggregations.remove(measureDesc.getFunction());
        }
    }

    private static void unmatchedCountColumnIfExistCountStar(Collection<FunctionDesc> aggregations) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        aggregations.removeIf(
                functionDesc -> kylinConfig.isReplaceColCountWithCountStar() && functionDesc.isCountOnColumn());
    }

    private static boolean matchAggIndex(SQLDigest sqlDigest, final LayoutEntity layoutEntity, final NDataModel model,
            Map<TblColRef, DeriveInfo> needDerive, CapabilityResult result, boolean isBatchFusionModel) {
        logger.trace("Matching agg index");
        Set<TblColRef> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlDigest.filterColumns);
        unmatchedCols.addAll(sqlDigest.groupbyColumns);
        Set<FunctionDesc> unmatchedMetrics = Sets.newHashSet(sqlDigest.aggregations);

        if (isBatchFusionModel) {
            layoutEntity.getStreamingColumns().forEach(unmatchedCols::remove);
        }
        unmatchedCols.removeAll(layoutEntity.getOrderedDimensions().values());
        goThruDerivedDims(layoutEntity.getIndex(), model, needDerive, unmatchedCols, sqlDigest);
        unmatchedAggregations(unmatchedMetrics, layoutEntity, isBatchFusionModel);
        unmatchedCountColumnIfExistCountStar(unmatchedMetrics);

        removeUnmatchedGroupingAgg(unmatchedMetrics);
        if (!unmatchedMetrics.isEmpty() || !unmatchedCols.isEmpty()) {
            applyAdvanceMeasureStrategy(layoutEntity, sqlDigest, unmatchedCols, unmatchedMetrics, result, isBatchFusionModel);
            applyDimAsMeasureStrategy(layoutEntity, sqlDigest, model, unmatchedMetrics, needDerive, result, isBatchFusionModel);
        }

        boolean matched = unmatchedCols.isEmpty() && unmatchedMetrics.isEmpty();
        if (!matched && logger.isDebugEnabled()) {
            logger.debug("Agg index {} with unmatched columns {}, unmatched metrics {}", //
                    layoutEntity, unmatchedCols, unmatchedMetrics);
        }

        return matched;
    }

    private static boolean matchTableIndex(SQLDigest sqlDigest, final LayoutEntity layout, final NDataModel model,
            Map<TblColRef, DeriveInfo> needDerive, CapabilityResult result, boolean isBatchFusionModel) {
        logger.trace("Matching table index");
        Set<TblColRef> unmatchedCols = Sets.newHashSet();
        unmatchedCols.addAll(sqlDigest.allColumns);

        if (isBatchFusionModel) {
            layout.getStreamingColumns().forEach(unmatchedCols::remove);
        }
        unmatchedCols.removeAll(layout.getOrderedDimensions().values());
        goThruDerivedDims(layout.getIndex(), model, needDerive, unmatchedCols, sqlDigest);
        if (!unmatchedCols.isEmpty()) {
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.TABLE_INDEX_MISSING_COLS);
            if (logger.isDebugEnabled()) {
                logger.debug("Table index {} with unmatched columns {}", layout, unmatchedCols);
            }
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

    private static void applyDimAsMeasureStrategy(LayoutEntity layoutEntity, SQLDigest sqlDigest, NDataModel model,
            Collection<FunctionDesc> unmatchedAggs, Map<TblColRef, DeriveInfo> needDeriveCollector,
            CapabilityResult result, boolean isBatchFusionModel) {
        IndexEntity indexEntity = layoutEntity.getIndex();
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
            Set<TblColRef> dimensionsSet = Sets.newHashSet(indexEntity.getDimensionSet());
            if (isBatchFusionModel) {
                dimensionsSet.addAll(layoutEntity.getStreamingColumns());
            }
            val leftUnmatchedCols = Sets.newHashSet(
                    CollectionUtils.subtract(functionDesc.getSourceColRefs(), dimensionsSet));
            if (CollectionUtils.isNotEmpty(leftUnmatchedCols)) {
                goThruDerivedDims(indexEntity, model, needDeriveCollector, leftUnmatchedCols, sqlDigest);
            }

            if (CollectionUtils.isNotEmpty(leftUnmatchedCols))
                continue;

            if (FunctionDesc.DIMENSION_AS_MEASURES.contains(functionDesc.getExpression())) {
                result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                it.remove();
            }
        }
    }

    private static void applyAdvanceMeasureStrategy(LayoutEntity layoutEntity, SQLDigest digest,
            Collection<TblColRef> unmatchedDims, Collection<FunctionDesc> unmatchedMetrics, CapabilityResult result, boolean isBatchFusionModel) {
        IndexEntity indexEntity = layoutEntity.getIndex();
        List<String> influencingMeasures = Lists.newArrayList();
        Set<NDataModel.Measure> measureSet = Sets.newHashSet(indexEntity.getMeasureSet());
        if (isBatchFusionModel) {
            measureSet.addAll(layoutEntity.getStreamingMeasures());
        }
        for (MeasureDesc measure : measureSet) {
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
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedDimensions().size());
    }

    private static Comparator<NLayoutCandidate> measureSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedMeasures().size());
    }

    /**
     * compare filters in SQL with layout dims
     * 1. choose the layout if its shardby column is found in filters
     * 2. otherwise compare position of filter columns appear in the layout dims
     * @param sortedFilters
     * @param config
     * @param project
     * @return
     */
    private static Comparator<NLayoutCandidate> filterColumnComparator(List<TblColRef> sortedFilters,
            KylinConfig config, String project) {
        return Ordering.from(shardByComparator(sortedFilters, config, project)).compound(colComparator(sortedFilters));
    }

    private static Comparator<NLayoutCandidate> nonFilterColumnComparator(List<TblColRef> sortedNonFilters) {
        return colComparator(sortedNonFilters);
    }

    /**
     * compare filters with dim pos in layout, filter columns are sorted by filter type and selectivity (cardinality)
     * @param sortedCols
     * @return
     */
    private static Comparator<NLayoutCandidate> colComparator(List<TblColRef> sortedCols) {
        return (layoutCandidate1, layoutCandidate2) -> {
            List<Integer> position1 = getColumnsPos(layoutCandidate1, sortedCols);
            List<Integer> position2 = getColumnsPos(layoutCandidate2, sortedCols);
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

    /**
     * compare filter columns with shardby columns in layouts
     * 1. check if shardby columns appears in filters
     * 2. if both layout has shardy columns in filters, compare the filter type and selectivity (cardinality)
     * @param columns
     * @param config
     * @param project
     * @return
     */
    private static Comparator<NLayoutCandidate> shardByComparator(List<TblColRef> columns, KylinConfig config,
            String project) {
        return (candidate1, candidate2) -> {
            TblColRef shardByCol1 = null;
            List<Integer> shardByCols1 = candidate1.getLayoutEntity().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols1)) {
                TblColRef tmpCol = candidate1.getLayoutEntity().getOrderedDimensions().get(shardByCols1.get(0));
                for (TblColRef colRef : columns) {
                    if (colRef.equals(tmpCol)) {
                        shardByCol1 = colRef;
                        break;
                    }
                }
            }

            TblColRef shardByCol2 = null;
            List<Integer> shardByCols2 = candidate2.getLayoutEntity().getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols2)) {
                TblColRef tmpCol = candidate1.getLayoutEntity().getOrderedDimensions().get(shardByCols2.get(0));
                for (TblColRef colRef : columns) {
                    if (colRef.equals(tmpCol)) {
                        shardByCol2 = colRef;
                        break;
                    }
                }
            }

            return Ordering.from(ComparatorUtils.nullLastComparator())
                    .compound(ComparatorUtils.filterColComparator(config, project)).compare(shardByCol1, shardByCol2);
        };
    }

    private static List<Integer> getColumnsPos(final NLayoutCandidate candidate, List<TblColRef> sortedColumns) {

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
        Integer id = candidate.getLayoutEntity().getDimensionPos(tblColRef);
        return id == null ? -1 : candidate.getLayoutEntity().getColOrder().indexOf(id);
    }

    private static boolean matchNonEquiJoinFks(final IndexEntity indexEntity, final JoinDesc joinDesc) {
        return joinDesc.isNonEquiJoin() && indexEntity
                .dimensionsDerive(SCD2NonEquiCondSimplification.INSTANCE.extractFksFromNonEquiJoinDesc(joinDesc));
    }

    private static void goThruDerivedDims(final IndexEntity indexEntity, final NDataModel model,
            Map<TblColRef, DeriveInfo> needDeriveCollector, Set<TblColRef> unmatchedDims, SQLDigest sqlDigest) {
        Iterator<TblColRef> unmatchedDimItr = unmatchedDims.iterator();
        while (unmatchedDimItr.hasNext()) {
            TblColRef unmatchedDim = unmatchedDimItr.next();
            if (model.isLookupTable(unmatchedDim.getTableRef())
                    && model.isQueryDerivedEnabled(unmatchedDim.getTableRef()) //
                    && goThruDerivedDimsFromLookupTable(indexEntity, needDeriveCollector, model, unmatchedDimItr,
                            unmatchedDim)) {
                continue;
            }

            // in some rare cases, FK needs to be derived from PK
            goThruDerivedDimsFromFactTable(indexEntity, needDeriveCollector, model, unmatchedDimItr, unmatchedDim);

        }

        // suppose: A join B && A join C, the relation of A->C is TO_MANY and C need to derive,
        // then the built index of this join relation only based on the flat table of A join B,
        // in order to get the correct result, the query result must join the snapshot of C.
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Set<String> excludedTables = FavoriteRuleManager.getInstance(config, model.getProject()).getExcludedTables();
        ExcludedLookupChecker checker = new ExcludedLookupChecker(excludedTables, model.getJoinTables(), model);
        model.getJoinTables().forEach(joinTableDesc -> {
            if (checker.getExcludedLookups().contains(joinTableDesc.getTable())) {
                JoinDesc join = joinTableDesc.getJoin();
                if (!needJoinSnapshot(sqlDigest, join)) {
                    return;
                }
                TblColRef foreignKeyColumn = join.getForeignKeyColumns()[0];
                needDeriveCollector.put(foreignKeyColumn, new DeriveInfo(DeriveInfo.DeriveType.LOOKUP, join,
                        new TblColRef[] { foreignKeyColumn }, false));
            }
        });
    }

    private static boolean needJoinSnapshot(SQLDigest sqlDigest, JoinDesc join) {
        List<JoinDesc> sqlDigestJoins = sqlDigest.joinDescs == null ? Lists.newArrayList() : sqlDigest.joinDescs;
        for (JoinDesc digestJoin : sqlDigestJoins) {
            Set<TblColRef> digestPKs = Sets.newHashSet(digestJoin.getPrimaryKeyColumns());
            Set<TblColRef> digestFKs = Sets.newHashSet(digestJoin.getForeignKeyColumns());
            Set<TblColRef> joinPKs = Sets.newHashSet(join.getPrimaryKeyColumns());
            Set<TblColRef> joinFKs = Sets.newHashSet(join.getForeignKeyColumns());
            if (!CollectionUtils.isEmpty(digestFKs) && !CollectionUtils.isEmpty(digestPKs)
                    && !CollectionUtils.isEmpty(joinFKs) && !CollectionUtils.isEmpty(joinPKs)
                    && digestFKs.containsAll(joinFKs) && digestPKs.containsAll(joinPKs)
                    && joinFKs.containsAll(digestFKs) && joinPKs.containsAll(digestPKs)) {
                return true;
            }
        }
        return false;
    }

    private static void goThruDerivedDimsFromFactTable(IndexEntity indexEntity,
            Map<TblColRef, DeriveInfo> needDeriveCollector, NDataModel model, Iterator<TblColRef> unmatchedDimItr,
            TblColRef unmatchedDim) {
        ImmutableCollection<TblColRef> pks = model.getFk2Pk().get(unmatchedDim);
        Iterable<TblColRef> pksOnCuboid = Iterables.filter(pks, indexEntity::dimensionsDerive);
        TblColRef pk = Iterables.getFirst(pksOnCuboid, null);
        if (pk != null) {
            JoinDesc joinByPKSide = model.getJoinByPKSide(pk.getTableRef());
            Preconditions.checkNotNull(joinByPKSide);

            //cannot derived fk from pk when left join
            if (!joinByPKSide.isInnerJoin()) {
                return;
            }
            needDeriveCollector.put(unmatchedDim,
                    new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide, new TblColRef[] { pk }, true));
            unmatchedDimItr.remove();
        }
    }

    private static boolean goThruDerivedDimsFromLookupTable(IndexEntity indexEntity,
            Map<TblColRef, DeriveInfo> needDeriveCollector, NDataModel model, Iterator<TblColRef> unmatchedDimItr,
            TblColRef unmatchedDim) {
        JoinDesc joinByPKSide = model.getJoinByPKSide(unmatchedDim.getTableRef());
        Preconditions.checkNotNull(joinByPKSide);
        TblColRef[] foreignKeyColumns = joinByPKSide.getForeignKeyColumns();
        TblColRef[] primaryKeyColumns = joinByPKSide.getPrimaryKeyColumns();

        val tables = model.getAliasMap();
        if (joinByPKSide.isInnerJoin() && ArrayUtils.contains(primaryKeyColumns, unmatchedDim)) {
            TblColRef relatedCol = foreignKeyColumns[ArrayUtils.indexOf(primaryKeyColumns, unmatchedDim)];
            if (indexEntity.dimensionsDerive(relatedCol)) {
                needDeriveCollector.put(unmatchedDim, new DeriveInfo(DeriveInfo.DeriveType.PK_FK, joinByPKSide,
                        new TblColRef[] { relatedCol }, true));
                unmatchedDimItr.remove();
                return true;
            }
        } else if (indexEntity.dimensionsDerive(foreignKeyColumns)
                && Optional.ofNullable(tables.get(unmatchedDim.getTableAlias()))
                        .map(ref -> StringUtils.isNotEmpty(ref.getTableDesc().getLastSnapshotPath())).orElse(false)) {

            DeriveInfo.DeriveType deriveType = matchNonEquiJoinFks(indexEntity, joinByPKSide)
                    ? DeriveInfo.DeriveType.LOOKUP_NON_EQUI
                    : DeriveInfo.DeriveType.LOOKUP;
            needDeriveCollector.put(unmatchedDim, new DeriveInfo(deriveType, joinByPKSide, foreignKeyColumns, false));
            unmatchedDimItr.remove();
            return true;
        }
        return false;
    }

}
