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

package io.kyligence.kap.cube.raw.gridtable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.gridtable.RecordComparators;
import org.apache.kylin.cube.gridtable.ScanRangePlannerBase;
import org.apache.kylin.cube.gridtable.SegmentGTStartAndEnd;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.gridtable.GTUtilExd;
import io.kyligence.kap.cube.raw.RawTableSegment;

public class RawTableScanRangePlanner extends ScanRangePlannerBase {

    private static final Logger logger = LoggerFactory.getLogger(RawTableScanRangePlanner.class);

    //non-GT
    protected RawTableSegment rawSegment;
    protected StorageContext context;

    public RawTableScanRangePlanner(RawTableSegment rawSegment, TupleFilter filter, Set<TblColRef> dimensions,
            Set<TblColRef> groupbyDims, //
            Collection<FunctionDesc> metrics, StorageContext contex) {
        this.context = contex;

        this.rawSegment = rawSegment;

        Set<TblColRef> filterDims = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterDims);

        this.gtInfo = RawTableGridTable.newGTInfoInOriginOrder(this.rawSegment.getRawTableInstance().getRawTableDesc());
        RawToGridTableMapping mapping = this.rawSegment.getRawTableInstance().getRawTableDesc()
                .getRawToGridTableMapping();

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        //replace the constant values in filter to dictionary codes 

        this.gtFilter = GTUtilExd.convertFilterColumnsAndConstantsForRawTable(filter, gtInfo,
                mapping.getOriginColumns(), true, groupbyDims);

        this.gtDimensions = mapping.makeGridTableColumns(dimensions);
        this.gtAggrGroups = mapping.makeGridTableColumns(groupbyDims);
        this.gtAggrMetrics = mapping.makeGridTableColumns(metrics);
        this.gtAggrFuncs = mapping.makeAggrFuncs(metrics);

        if (rawSegment.getModel().getPartitionDesc().isPartitioned()) {
            int index = mapping.getOriginIndexOf(rawSegment.getModel().getPartitionDesc().getPartitionDateColumnRef());
            if (index >= 0) {
                SegmentGTStartAndEnd segmentGTStartAndEnd = new SegmentGTStartAndEnd(rawSegment, gtInfo);
                this.isPartitionColUsingDatetimeEncoding = segmentGTStartAndEnd.isUsingDatetimeEncoding(index);
                this.gtStartAndEnd = segmentGTStartAndEnd.getSegmentStartAndEnd(index);
                this.gtPartitionCol = gtInfo.colRef(index);
            }
        }
    }

    public GTScanRequest planScanRequest() {
        GTScanRequest scanRequest;
        boolean shouldSkip = this.shouldSkipSegment();
        if (!shouldSkip) {
            scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null).setDimensions(gtDimensions).//
                    setAggrGroupBy(gtAggrGroups).setAggrMetrics(gtAggrMetrics).setAggrMetricsFuncs(gtAggrFuncs)
                    .setFilterPushDown(gtFilter).//
                    setAllowStorageAggregation(false)
                    .setAggCacheMemThreshold(rawSegment.getCubeSegment().getConfig().getQueryCoprocessorMemGB()).//
                    setStoragePushDownLimit(context.getFinalPushDownLimit())
                    .setStorageLimitLevel(context.getStorageLimitLevel()).createGTScanRequest();
        } else {
            scanRequest = null;
        }
        return scanRequest;
    }

    private boolean shouldSkipSegment() {
        TupleFilter flatFilter = flattenToOrAndFilter(gtFilter);

        List<Collection<ScanRangePlannerBase.ColumnRange>> orAndDimRanges = translateToOrAndDimRanges(flatFilter);

        boolean shouldSkipSegment = true;
        for (Collection<ColumnRange> andDimRanges : orAndDimRanges) {
            shouldSkipSegment = shouldSkipSegment && shouldSkipSegment(andDimRanges);
        }

        return shouldSkipSegment;
    }

    protected boolean shouldSkipSegment(Collection<ColumnRange> andDimRanges) {

        for (ColumnRange range : andDimRanges) {
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
                    return true;
                }
            }
        }
        return false;
    }
}
