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

package io.kyligence.kap.storage.gtrecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.common.FuzzyValueCombination;
import org.apache.kylin.cube.gridtable.GridTables;
import org.apache.kylin.cube.gridtable.RecordComparators;
import org.apache.kylin.cube.gridtable.ScanRangePlannerBase;
import org.apache.kylin.cube.gridtable.SegmentGTStartAndEnd;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DeriveInfo;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.cube.gridtable.NCuboidToGridTableMapping;
import io.kyligence.kap.cube.kv.NCubeDimEncMap;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;

public class NDataflowScanRangePlanner extends ScanRangePlannerBase {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowScanRangePlanner.class);

    protected int maxScanRanges;
    protected int maxFuzzyKeys;

    //non-GT
    protected NDataSegment dataSegment;
    protected NCubePlan cubePlan;
    protected NCuboidLayout cuboid;

    protected StorageContext context;

    public NDataflowScanRangePlanner(NDataSegment dataSegment, NLayoutCandidate candidate, TupleFilter filter,
            Set<TblColRef> dimensions, Set<TblColRef> groupByDims, //
            Collection<FunctionDesc> metrics, TupleFilter havingFilter, StorageContext context) {
        this.context = context;

        this.maxScanRanges = dataSegment.getConfig().getQueryStorageVisitScanRangeMax();
        this.maxFuzzyKeys = dataSegment.getConfig().getQueryScanFuzzyKeyMax();

        this.dataSegment = dataSegment;
        this.cubePlan = dataSegment.getCubePlan();
        this.cuboid = candidate.getCuboidLayout();

        Set<TblColRef> filterDims = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterDims);

        NCuboidToGridTableMapping mapping = new NCuboidToGridTableMapping(cuboid);
        this.gtInfo = GridTables.newGTInfo(mapping, new NCubeDimEncMap(dataSegment));

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        //replace the constant values in filter to dictionary codes
        Set<TblColRef> groupByPushDown = Sets.newHashSet(groupByDims);
        this.gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, gtInfo, mapping.getCuboidDimensionsInGTOrder(),
                groupByPushDown);
        this.havingFilter = havingFilter;

        this.gtDimensions = mapping.makeGridTableColumns(dimensions);
        //        this.gtAggrGroups = mapping.makeGridTableColumns(replaceDerivedColumns(groupByPushDown, cubePlan));
        this.gtAggrGroups = mapping.makeGridTableColumns(replaceDerivedColumns(groupByPushDown, candidate));
        this.gtAggrMetrics = mapping.makeGridTableColumns(metrics);
        this.gtAggrFuncs = mapping.makeAggrFuncs(metrics);

        if (dataSegment.getModel().getPartitionDesc().isPartitioned()) {
            int index = mapping.getIndexOf(dataSegment.getModel().getPartitionDesc().getPartitionDateColumnRef());
            if (index >= 0) {
                SegmentGTStartAndEnd segmentGTStartAndEnd = new SegmentGTStartAndEnd(dataSegment, gtInfo);
                this.gtStartAndEnd = segmentGTStartAndEnd.getSegmentStartAndEnd(index);
                this.isPartitionColUsingDatetimeEncoding = segmentGTStartAndEnd.isUsingDatetimeEncoding(index);
                this.gtPartitionCol = gtInfo.colRef(index);
            }
        }
    }

    private Set<TblColRef> replaceDerivedColumns(Set<TblColRef> input, NLayoutCandidate layoutCandidate) {
        Set<TblColRef> ret = Sets.newHashSet();
        for (TblColRef col : input) {
            DeriveInfo hostInfo = layoutCandidate.getDerivedToHostMap().get(col);
            if (hostInfo != null) {
                Collections.addAll(ret, hostInfo.columns);
            } else {
                ret.add(col);
            }
        }
        return ret;
    }

    /**
     * Construct  GTScanRangePlanner with incomplete information. For UT only.
     */
    public NDataflowScanRangePlanner(GTInfo info, Pair<ByteArray, ByteArray> gtStartAndEnd, TblColRef gtPartitionCol,
            TupleFilter gtFilter) {

        this.maxScanRanges = KylinConfig.getInstanceFromEnv().getQueryStorageVisitScanRangeMax();
        this.maxFuzzyKeys = KylinConfig.getInstanceFromEnv().getQueryScanFuzzyKeyMax();

        this.gtInfo = info;

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        this.gtFilter = gtFilter;
        this.gtStartAndEnd = gtStartAndEnd;
        this.gtPartitionCol = gtPartitionCol;
    }

    public GTScanRequest planScanRequest() {
        GTScanRequest scanRequest;
        List<GTScanRange> scanRanges = this.planScanRanges();
        if (scanRanges != null && scanRanges.size() != 0) {
            scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(scanRanges).setDimensions(gtDimensions).//
                    setAggrGroupBy(gtAggrGroups).setAggrMetrics(gtAggrMetrics).setAggrMetricsFuncs(gtAggrFuncs)
                    .setFilterPushDown(gtFilter).//
                    setAllowStorageAggregation(context.isNeedStorageAggregation())
                    .setAggCacheMemThreshold(dataSegment.getConfig().getQueryCoprocessorMemGB()).//
                    setStoragePushDownLimit(context.getFinalPushDownLimit())
                    .setStorageLimitLevel(context.getStorageLimitLevel()).setHavingFilterPushDown(havingFilter)
                    .setTimeout(dataSegment.getConfig().getQueryTimeoutSeconds() * 1000L)
                    .createGTScanRequest();
        } else {
            scanRequest = null;
        }
        return scanRequest;
    }

    /**
     * Overwrite this method to provide smarter storage visit plans
     * @return
     */
    public List<GTScanRange> planScanRanges() {
        TupleFilter flatFilter = flattenToOrAndFilter(gtFilter);

        List<Collection<ScanRangePlannerBase.ColumnRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter);

        List<GTScanRange> scanRanges = Lists.newArrayListWithCapacity(orAndDimRanges.size());
        for (Collection<ScanRangePlannerBase.ColumnRange> andDimRanges : orAndDimRanges) {
            GTScanRange scanRange = newScanRange(andDimRanges);
            if (scanRange != null)
                scanRanges.add(scanRange);
        }

        List<GTScanRange> mergedRanges = mergeOverlapRanges(scanRanges);
        mergedRanges = mergeTooManyRanges(mergedRanges, maxScanRanges);

        return mergedRanges;
    }

    protected GTScanRange newScanRange(Collection<ScanRangePlannerBase.ColumnRange> andDimRanges) {
        GTRecord pkStart = new GTRecord(gtInfo);
        GTRecord pkEnd = new GTRecord(gtInfo);
        Map<Integer, Set<ByteArray>> fuzzyValues = Maps.newHashMap();

        List<GTRecord> fuzzyKeys;

        for (ScanRangePlannerBase.ColumnRange range : andDimRanges) {
            if (gtPartitionCol != null && range.column.equals(gtPartitionCol)) {
                int beginCompare = rangeStartEndComparator.comparator.compare(range.begin, gtStartAndEnd.getSecond());
                int endCompare = rangeStartEndComparator.comparator.compare(gtStartAndEnd.getFirst(), range.end);

                if ((isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare < 0)
                        || (!isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare <= 0)) {
                    //segment range is [Closed,Open), but segmentStartAndEnd.getSecond() might be rounded when using dict encoding, so use <= when has equals in condition.
                } else {
                    logger.debug(
                            "Pre-check partition col filter failed, partitionColRef {}, segment start {}, segment end {}, range begin {}, range end {}", //
                            gtPartitionCol, makeReadable(gtStartAndEnd.getFirst()),
                            makeReadable(gtStartAndEnd.getSecond()), makeReadable(range.begin),
                            makeReadable(range.end));
                    return null;
                }
            }

            int col = range.column.getColumnDesc().getZeroBasedIndex();
            if (!gtInfo.getPrimaryKey().get(col))
                continue;

            pkStart.set(col, range.begin);
            pkEnd.set(col, range.end);

            if (range.valueSet != null && !range.valueSet.isEmpty()) {
                fuzzyValues.put(col, range.valueSet);
            }
        }

        fuzzyKeys =

                buildFuzzyKeys(fuzzyValues);
        return new GTScanRange(pkStart, pkEnd, fuzzyKeys);
    }

    private List<GTRecord> buildFuzzyKeys(Map<Integer, Set<ByteArray>> fuzzyValueSet) {
        ArrayList<GTRecord> result = Lists.newArrayList();

        if (fuzzyValueSet.isEmpty())
            return result;

        // debug/profiling purpose
        if (BackdoorToggles.getDisableFuzzyKey()) {
            logger.info("The execution of this query will not use fuzzy key");
            return result;
        }

        List<Map<Integer, ByteArray>> fuzzyValueCombinations = FuzzyValueCombination.calculate(fuzzyValueSet,
                maxFuzzyKeys);

        for (Map<Integer, ByteArray> fuzzyValue : fuzzyValueCombinations) {

            //            BitSet bitSet = new BitSet(gtInfo.getColumnCount());
            //            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
            //                bitSet.set(entry.getKey());
            //            }
            GTRecord fuzzy = new GTRecord(gtInfo);
            for (Map.Entry<Integer, ByteArray> entry : fuzzyValue.entrySet()) {
                fuzzy.set(entry.getKey(), entry.getValue());
            }

            result.add(fuzzy);
        }
        return result;
    }

    protected List<GTScanRange> mergeOverlapRanges(List<GTScanRange> ranges) {
        if (ranges.size() <= 1) {
            return ranges;
        }

        // sort ranges by start key
        Collections.sort(ranges, new Comparator<GTScanRange>() {
            @Override
            public int compare(GTScanRange a, GTScanRange b) {
                return rangeStartComparator.compare(a.pkStart, b.pkStart);
            }
        });

        // merge the overlap range
        List<GTScanRange> mergedRanges = new ArrayList<GTScanRange>();
        int mergeBeginIndex = 0;
        GTRecord mergeEnd = ranges.get(0).pkEnd;
        for (int index = 1; index < ranges.size(); index++) {
            GTScanRange range = ranges.get(index);

            // if overlap, swallow it
            if (rangeStartEndComparator.compare(range.pkStart, mergeEnd) <= 0) {
                mergeEnd = rangeEndComparator.max(mergeEnd, range.pkEnd);
                continue;
            }

            // not overlap, split here
            GTScanRange mergedRange = mergeKeyRange(ranges.subList(mergeBeginIndex, index));
            mergedRanges.add(mergedRange);

            // start new split
            mergeBeginIndex = index;
            mergeEnd = range.pkEnd;
        }

        // don't miss the last range
        GTScanRange mergedRange = mergeKeyRange(ranges.subList(mergeBeginIndex, ranges.size()));
        mergedRanges.add(mergedRange);

        return mergedRanges;
    }

    private GTScanRange mergeKeyRange(List<GTScanRange> ranges) {
        GTScanRange first = ranges.get(0);
        if (ranges.size() == 1)
            return first;

        GTRecord start = first.pkStart;
        GTRecord end = first.pkEnd;
        List<GTRecord> newFuzzyKeys = new ArrayList<GTRecord>();

        boolean hasNonFuzzyRange = false;
        for (GTScanRange range : ranges) {
            hasNonFuzzyRange = hasNonFuzzyRange || range.fuzzyKeys.isEmpty();
            newFuzzyKeys.addAll(range.fuzzyKeys);
            end = rangeEndComparator.max(end, range.pkEnd);
        }

        // if any range is non-fuzzy, then all fuzzy keys must be cleared
        // also too many fuzzy keys will slow down HBase scan
        if (hasNonFuzzyRange || newFuzzyKeys.size() > maxFuzzyKeys) {
            newFuzzyKeys.clear();
        }

        return new GTScanRange(start, end, newFuzzyKeys);
    }

    protected List<GTScanRange> mergeTooManyRanges(List<GTScanRange> ranges, int maxRanges) {
        if (ranges.size() <= maxRanges) {
            return ranges;
        }

        // TODO: check the distance between range and merge the large distance range
        // From KYLIN
        List<GTScanRange> result = new ArrayList<GTScanRange>(1);
        GTScanRange mergedRange = mergeKeyRange(ranges);
        result.add(mergedRange);
        return result;
    }

    public int getMaxScanRanges() {
        return maxScanRanges;
    }

    public void setMaxScanRanges(int maxScanRanges) {
        this.maxScanRanges = maxScanRanges;
    }

}
