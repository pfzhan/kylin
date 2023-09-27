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

package org.apache.kylin.query.routing;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SegmentOnlineMode;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.cuboid.ChooserContext;
import org.apache.kylin.metadata.cube.cuboid.ComparatorUtils;
import org.apache.kylin.metadata.cube.cuboid.IndexMatcher;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.util.QueryInterruptChecker;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryLayoutChooser {

    private QueryLayoutChooser() {
    }

    public static NLayoutCandidate selectPartialLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest, Map<String, Set<Long>> chSegmentToLayoutsMap) {

        NLayoutCandidate candidate = null;
        List<NDataSegment> toRemovedSegments = Lists.newArrayList();
        for (NDataSegment segment : prunedSegments) {
            if (candidate == null) {
                candidate = selectLayoutCandidate(dataflow, Lists.newArrayList(segment), sqlDigest,
                        chSegmentToLayoutsMap);
            }

            long layoutId = candidate == null ? -1L : candidate.getLayoutEntity().getId();
            if (segment.getSegDetails().getLayoutById(layoutId) == null) {
                toRemovedSegments.add(segment);
            }
        }
        prunedSegments.removeAll(toRemovedSegments);
        return candidate;
    }

    public static NLayoutCandidate selectLayoutCandidate(NDataflow dataflow, List<NDataSegment> prunedSegments,
            SQLDigest sqlDigest, Map<String, Set<Long>> chSegmentToLayoutsMap) {

        if (CollectionUtils.isEmpty(prunedSegments)) {
            log.info("There is no segment to answer sql");
            return NLayoutCandidate.EMPTY;
        }

        ChooserContext chooserContext = new ChooserContext(sqlDigest, dataflow);
        if (chooserContext.isIndexMatchersInvalid()) {
            return null;
        }

        Collection<NDataLayout> commonLayouts = getCommonLayouts(prunedSegments, dataflow, chSegmentToLayoutsMap);
        log.info("Matching dataflow with seg num: {} layout num: {}", prunedSegments.size(), commonLayouts.size());
        Map<Long, List<NDataLayout>> commonLayoutsMap = commonLayouts.stream()
                .collect(Collectors.toMap(NDataLayout::getLayoutId, Lists::newArrayList));
        List<NLayoutCandidate> candidates = collectAllLayoutCandidates(dataflow, chooserContext, commonLayoutsMap);
        return chooseBestLayoutCandidate(dataflow, sqlDigest, chooserContext, candidates, "selectLayoutCandidate");
    }

    public static List<NLayoutCandidate> collectAllLayoutCandidates(NDataflow dataflow, ChooserContext chooserContext,
            Map<Long, List<NDataLayout>> dataLayoutMap) {
        List<NLayoutCandidate> candidates = Lists.newArrayList();
        for (Map.Entry<Long, List<NDataLayout>> entry : dataLayoutMap.entrySet()) {
            LayoutEntity layout = dataflow.getIndexPlan().getLayoutEntity(entry.getKey());
            log.trace("Matching index: id = {}", entry.getKey());
            IndexMatcher.MatchResult matchResult = chooserContext.getTableIndexMatcher().match(layout);
            if (!matchResult.isMatched()) {
                matchResult = chooserContext.getAggIndexMatcher().match(layout);
            }

            if (!matchResult.isMatched()) {
                log.trace("The [{}] cannot match with the {}", chooserContext.getSqlDigest().toString(), layout);
                continue;
            }

            NLayoutCandidate candidate = new NLayoutCandidate(layout);
            CapabilityResult tempResult = new CapabilityResult(matchResult);
            if (!matchResult.getNeedDerive().isEmpty()) {
                candidate.setDerivedToHostMap(matchResult.getNeedDerive());
                candidate.setDerivedTableSnapshots(candidate.getDerivedToHostMap().keySet().stream()
                        .map(i -> chooserContext.convertToRef(i).getTable()).collect(Collectors.toSet()));
            }
            List<NDataLayout> dataLayouts = entry.getValue();
            long allRows = dataLayouts.stream().mapToLong(NDataLayout::getRows).sum();
            candidate.setCost(allRows * (tempResult.influences.size() + matchResult.getInfluenceFactor()));
            candidate.setCapabilityResult(tempResult);

            long[] rangeAndLatest = calcSegRangeAndMaxEnd(chooserContext, dataflow, dataLayouts);
            candidate.setRange(rangeAndLatest[0]);
            candidate.setMaxSegEnd(rangeAndLatest[1]);
            candidates.add(candidate);
        }
        return candidates;
    }

    private static long[] calcSegRangeAndMaxEnd(ChooserContext chooserContext, NDataflow df,
            List<NDataLayout> dataLayouts) {
        long[] rangeAndLatest = new long[2];
        if (!QueryRouter.isVacantIndexPruningEnabled(chooserContext.getKylinConfig())) {
            return rangeAndLatest;
        }
        List<String> segmentNameList = Lists.newArrayList();
        for (NDataLayout dataLayout : dataLayouts) {
            NDataSegment segment = df.getSegment(dataLayout.getSegDetails().getId());
            Long end = (Long) segment.getSegRange().getEnd();
            Long start = (Long) segment.getSegRange().getStart();
            rangeAndLatest[0] += (end - start);
            rangeAndLatest[1] = Math.max(rangeAndLatest[1], end);
            segmentNameList.add(segment.getName());
        }
        log.trace("All available segments are: {}", segmentNameList);
        return rangeAndLatest;
    }

    public static NLayoutCandidate selectHighIntegrityCandidate(NDataflow dataflow, Candidate candidate,
            SQLDigest digest) {
        List<NDataSegment> prunedSegments = candidate.getPrunedSegments(dataflow);
        if (!QueryRouter.isVacantIndexPruningEnabled(NProjectManager.getProjectConfig(dataflow.getProject()))) {
            return null;
        }
        if (CollectionUtils.isEmpty(prunedSegments)) {
            log.info("There is no segment to answer sql");
            return NLayoutCandidate.EMPTY;
        }

        ChooserContext chooserContext = new ChooserContext(digest, dataflow);
        if (chooserContext.isIndexMatchersInvalid()) {
            return null;
        }

        Map<Long, List<NDataLayout>> idToDataLayoutsMap = getCommonLayouts(dataflow, chooserContext, candidate,
                prunedSegments);
        List<NLayoutCandidate> allLayoutCandidates = QueryLayoutChooser.collectAllLayoutCandidates(dataflow,
                chooserContext, idToDataLayoutsMap);
        return chooseBestLayoutCandidate(dataflow, digest, chooserContext, allLayoutCandidates,
                "selectHighIntegrityCandidate");
    }

    private static NLayoutCandidate chooseBestLayoutCandidate(NDataflow dataflow, SQLDigest digest,
            ChooserContext chooserContext, List<NLayoutCandidate> allLayoutCandidates, String invokedByMethod) {
        QueryInterruptChecker.checkThreadInterrupted("Interrupted exception occurs.",
                "Current step involves gathering all the layouts that "
                        + "can potentially provide a response to this query.");

        if (allLayoutCandidates.isEmpty()) {
            log.info("There is no layouts can match with the [{}]", digest.toString());
            return null;
        }
        sortCandidates(allLayoutCandidates, chooserContext, digest);
        log.debug("Invoked by method {}. Successfully matched {} candidates within the model ({}/{}), " //
                + "and {} has been selected.", invokedByMethod, allLayoutCandidates.size(), dataflow.getProject(),
                dataflow.getId(), allLayoutCandidates.get(0).toString());
        return allLayoutCandidates.get(0);
    }

    private static Map<Long, List<NDataLayout>> getCommonLayouts(NDataflow dataflow, ChooserContext chooserContext,
            Candidate candidate, List<NDataSegment> prunedSegments) {
        Map<Long, List<NDataLayout>> idToDataLayoutsMap = Maps.newHashMap();
        for (NDataSegment segment : prunedSegments) {
            segment.getLayoutsMap().forEach((id, dataLayout) -> {
                idToDataLayoutsMap.putIfAbsent(id, Lists.newArrayList());
                idToDataLayoutsMap.get(id).add(dataLayout);
            });
        }

        KylinConfig projectConfig = NProjectManager.getProjectConfig(dataflow.getProject());
        String segmentOnlineMode = projectConfig.getKylinEngineSegmentOnlineMode();
        Map<String, Set<Long>> chSegToLayoutsMap = candidate.getChSegToLayoutsMap(dataflow);
        if (SegmentOnlineMode.ANY.toString().equalsIgnoreCase(segmentOnlineMode)
                && MapUtils.isNotEmpty(chSegToLayoutsMap)) {
            Map<Long, List<NDataLayout>> chLayoutsMap = Maps.newHashMap();
            chSegToLayoutsMap.forEach((segId, chLayoutIds) -> chLayoutIds.forEach(id -> {
                NDataLayout dataLayout = NDataLayout.newDataLayout(dataflow, segId, id);
                chLayoutsMap.putIfAbsent(id, Lists.newArrayList());
                chLayoutsMap.get(id).add(dataLayout);
            }));
            chLayoutsMap.forEach((layoutId, chLayouts) -> {
                if (!idToDataLayoutsMap.containsKey(layoutId)) {
                    idToDataLayoutsMap.put(layoutId, chLayouts);
                } else {
                    List<NDataLayout> normalLayouts = idToDataLayoutsMap.get(layoutId);
                    long[] normalRangeAndMax = calcSegRangeAndMaxEnd(chooserContext, dataflow, normalLayouts);
                    long[] chRangeAndMax = calcSegRangeAndMaxEnd(chooserContext, dataflow, chLayouts);
                    // CH Layouts is more preferred: 
                    // 1. with bigger built data range
                    // 2. have the latest build data
                    if ((normalRangeAndMax[0] < chRangeAndMax[0])
                            || (normalRangeAndMax[0] == chRangeAndMax[0] && normalRangeAndMax[1] <= chRangeAndMax[1])) {
                        idToDataLayoutsMap.put(layoutId, chLayouts);
                    }
                }
            });
        }
        return idToDataLayoutsMap;
    }

    private static Collection<NDataLayout> getCommonLayouts(List<NDataSegment> segments, NDataflow dataflow,
            Map<String, Set<Long>> chSegmentToLayoutsMap) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(dataflow.getProject());
        if (!projectConfig.isHeterogeneousSegmentEnabled()) {
            return dataflow.getLatestReadySegment().getLayoutsMap().values();
        }

        Map<Long, NDataLayout> commonLayouts = Maps.newHashMap();
        if (CollectionUtils.isEmpty(segments)) {
            return commonLayouts.values();
        }

        val isMppOnTheFlyLayoutsEnabled = dataflow.getConfig().isMppOnTheFlyLayoutsEnabled();

        String segmentOnlineMode = projectConfig.getKylinEngineSegmentOnlineMode();
        for (int i = 0; i < segments.size(); i++) {
            val dataSegment = segments.get(i);
            var layoutIdMapToDataLayout = dataSegment.getLayoutsMap();
            val isReadyEmptySeg = isMppOnTheFlyLayoutsEnabled
                    && dataSegment.getStatus() == SegmentStatusEnum.READY && layoutIdMapToDataLayout.isEmpty();
            if (SegmentOnlineMode.ANY.toString().equalsIgnoreCase(segmentOnlineMode)
                    && MapUtils.isNotEmpty(chSegmentToLayoutsMap)) {
                // Only the basic TableIndex is built in the CH storage
                Set<Long> chLayouts = chSegmentToLayoutsMap.getOrDefault(dataSegment.getId(), Sets.newHashSet());
                Map<Long, NDataLayout> nDataLayoutMap = chLayouts.stream()
                        .map(id -> NDataLayout.newDataLayout(dataflow, dataSegment.getId(), id))
                        .collect(Collectors.toMap(NDataLayout::getLayoutId, Function.identity()));

                nDataLayoutMap.putAll(layoutIdMapToDataLayout);
                layoutIdMapToDataLayout = nDataLayoutMap;
            }
            if (i == 0 || isReadyEmptySeg) { // ACCEPT empty & ready segments, they can be served by mpp on-the-fly
                commonLayouts.putAll(layoutIdMapToDataLayout);
            } else {
                commonLayouts.keySet().retainAll(layoutIdMapToDataLayout.keySet());
            }
        }
        return commonLayouts.values();
    }

    public static void sortCandidates(List<NLayoutCandidate> candidates, ChooserContext chooserContext,
            SQLDigest sqlDigest) {
        List<Integer> filterColIds = getFilterColIds(chooserContext, sqlDigest);
        List<Integer> nonFilterColIds = getNonFilterColIds(chooserContext, sqlDigest);
        Ordering<NLayoutCandidate> ordering = QueryRouter.isVacantIndexPruningEnabled(chooserContext.getKylinConfig())
                ? getEnhancedSorter(filterColIds, nonFilterColIds)
                : getDefaultSorter(filterColIds, nonFilterColIds);
        candidates.sort(ordering);
    }

    private static Ordering<NLayoutCandidate> getEnhancedSorter(List<Integer> filterColIds,
            List<Integer> nonFilterColIds) {
        return Ordering.from(segmentRangeComparator()) // high data integrity
                .compound(preferAggComparator()) //
                .compound(derivedLayoutComparator()) //
                .compound(rowSizeComparator()) // lower cost
                .compound(filterColumnComparator(filterColIds)) //
                .compound(dimensionSizeComparator()) //
                .compound(measureSizeComparator()) //
                .compound(nonFilterColumnComparator(nonFilterColIds)) //
                .compound(segmentEffectivenessComparator()); // the latest segment
    }

    private static Ordering<NLayoutCandidate> getDefaultSorter(List<Integer> filterColIds,
            List<Integer> nonFilterColIds) {
        return Ordering //
                .from(preferAggComparator()) //
                .compound(derivedLayoutComparator()) //
                .compound(rowSizeComparator()) // L1 comparator, compare cuboid rows
                .compound(filterColumnComparator(filterColIds)) // L2 comparator, order filter columns
                .compound(dimensionSizeComparator()) // the lower dimension the best
                .compound(measureSizeComparator()) // L3 comparator, order size of cuboid columns
                .compound(nonFilterColumnComparator(nonFilterColIds));
    }

    private static List<Integer> getFilterColIds(ChooserContext chooserContext, SQLDigest sqlDigest) {
        Set<TblColRef> filterColSet = ImmutableSet.copyOf(sqlDigest.filterColumns);
        List<TblColRef> filterCols = Lists.newArrayList(filterColSet);
        return filterCols.stream().sorted(ComparatorUtils.filterColComparator(chooserContext))
                .map(col -> chooserContext.getTblColMap().get(col)).collect(Collectors.toList());
    }

    private static List<Integer> getNonFilterColIds(ChooserContext chooserContext, SQLDigest sqlDigest) {

        Set<TblColRef> nonFilterColSet;
        if (sqlDigest.isRawQuery) {
            nonFilterColSet = sqlDigest.allColumns.stream()
                    .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE)
                    .collect(Collectors.toSet());
        } else {
            nonFilterColSet = sqlDigest.groupbyColumns.stream()
                    .filter(colRef -> colRef.getFilterLevel() == TblColRef.FilterColEnum.NONE)
                    .collect(Collectors.toSet());
        }
        List<TblColRef> nonFilterColumns = Lists.newArrayList(nonFilterColSet);
        nonFilterColumns.sort(ComparatorUtils.nonFilterColComparator());
        return nonFilterColumns.stream().map(col -> chooserContext.getTblColMap().get(col))
                .collect(Collectors.toList());
    }

    public static Comparator<NLayoutCandidate> segmentRangeComparator() {
        return (c1, c2) -> Long.compare(c2.getRange(), c1.getRange());
    }

    public static Comparator<NLayoutCandidate> segmentEffectivenessComparator() {
        return (c1, c2) -> Long.compare(c2.getMaxSegEnd(), c1.getMaxSegEnd());
    }

    public static Comparator<NLayoutCandidate> preferAggComparator() {
        return (layoutCandidate1, layoutCandidate2) -> {
            if (!KylinConfig.getInstanceFromEnv().isPreferAggIndex()) {
                return 0;
            }
            if (!layoutCandidate1.getLayoutEntity().getIndex().isTableIndex()
                    && layoutCandidate2.getLayoutEntity().getIndex().isTableIndex()) {
                return -1;
            } else if (layoutCandidate1.getLayoutEntity().getIndex().isTableIndex()
                    && !layoutCandidate2.getLayoutEntity().getIndex().isTableIndex()) {
                return 1;
            }
            return 0;
        };
    }

    public static Comparator<NLayoutCandidate> derivedLayoutComparator() {
        return (candidate1, candidate2) -> {
            int result = 0;
            if (candidate1.getDerivedToHostMap().isEmpty() && !candidate2.getDerivedToHostMap().isEmpty()) {
                result = -1;
            } else if (!candidate1.getDerivedToHostMap().isEmpty() && candidate2.getDerivedToHostMap().isEmpty()) {
                result = 1;
            }

            IndexPlan indexPlan = candidate1.getLayoutEntity().getIndex().getIndexPlan();
            KylinConfig config = indexPlan.getConfig();
            if (config.isTableExclusionEnabled() && config.isSnapshotPreferred()) {
                result = -1 * result;
            }
            return result;
        };
    }

    public static Comparator<NLayoutCandidate> rowSizeComparator() {
        return Comparator.comparingDouble(NLayoutCandidate::getCost);
    }

    public static Comparator<NLayoutCandidate> dimensionSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedDimensions().size());
    }

    public static Comparator<NLayoutCandidate> measureSizeComparator() {
        return Comparator.comparingInt(candidate -> candidate.getLayoutEntity().getOrderedMeasures().size());
    }

    /**
     * compare filters in SQL with layout dims
     * 1. choose the layout if its shardby column is found in filters
     * 2. otherwise, compare position of filter columns appear in the layout dims
     */
    public static Comparator<NLayoutCandidate> filterColumnComparator(List<Integer> sortedFilters) {
        return Ordering.from(shardByComparator(sortedFilters)).compound(colComparator(sortedFilters));
    }

    public static Comparator<NLayoutCandidate> nonFilterColumnComparator(List<Integer> sortedNonFilters) {
        return colComparator(sortedNonFilters);
    }

    /**
     * compare filters with dim pos in layout, filter columns are sorted by filter type and selectivity (cardinality)
     */
    public static Comparator<NLayoutCandidate> colComparator(List<Integer> sortedCols) {
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
     */
    public static Comparator<NLayoutCandidate> shardByComparator(List<Integer> columns) {
        return (candidate1, candidate2) -> {
            int shardByCol1Idx = getShardByColIndex(candidate1, columns);
            int shardByCol2Idx = getShardByColIndex(candidate2, columns);
            return shardByCol1Idx - shardByCol2Idx;
        };
    }

    private static int getShardByColIndex(NLayoutCandidate candidate1, List<Integer> columns) {
        int shardByCol1Idx = Integer.MAX_VALUE;
        List<Integer> shardByCols1 = candidate1.getLayoutEntity().getShardByColumns();
        if (CollectionUtils.isNotEmpty(shardByCols1)) {
            int tmpCol = shardByCols1.get(0);
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i) == tmpCol) {
                    shardByCol1Idx = i;
                    break;
                }
            }
        }
        return shardByCol1Idx;
    }

    private static List<Integer> getColumnsPos(final NLayoutCandidate candidate, List<Integer> sortedColumns) {

        List<Integer> positions = Lists.newArrayList();
        for (Integer col : sortedColumns) {
            DeriveInfo deriveInfo = candidate.getDerivedToHostMap().get(col);
            if (deriveInfo == null) {
                positions.add(getDimsIndexInLayout(col, candidate));
            } else {
                for (Integer hostColId : deriveInfo.columns) {
                    positions.add(getDimsIndexInLayout(hostColId, candidate));
                }
            }
        }
        return positions;
    }

    private static int getDimsIndexInLayout(Integer id, final NLayoutCandidate candidate) {
        //get dimension
        return id == null ? -1 : candidate.getLayoutEntity().getColOrder().indexOf(id);
    }
}
