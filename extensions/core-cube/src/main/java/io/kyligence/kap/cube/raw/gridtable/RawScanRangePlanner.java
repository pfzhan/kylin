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

package io.kyligence.kap.cube.raw.gridtable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.gridtable.RecordComparators;
import org.apache.kylin.cube.gridtable.ScanRangePlannerBase;
import org.apache.kylin.cube.gridtable.SegmentGTStartAndEnd;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTScanRequestBuilder;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTComparator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.raw.RawTableSegment;

public class RawScanRangePlanner extends ScanRangePlannerBase {

    private static final Logger logger = LoggerFactory.getLogger(RawScanRangePlanner.class);

    //non-GT
    protected RawTableSegment rawSegment;

    public RawScanRangePlanner(RawTableSegment rawSegment, TupleFilter filter, Set<TblColRef> dimensions, Set<TblColRef> groupbyDims, //
            Collection<FunctionDesc> metrics) {

        this.rawSegment = rawSegment;

        Set<TblColRef> filterDims = Sets.newHashSet();
        TupleFilter.collectColumns(filter, filterDims);

        this.gtInfo = RawGridTable.newGTInfo(this.rawSegment.getRawTableInstance());
        RawToGridTableMapping mapping = new RawToGridTableMapping(this.rawSegment.getRawTableInstance());

        IGTComparator comp = gtInfo.getCodeSystem().getComparator();
        //start key GTRecord compare to start key GTRecord
        this.rangeStartComparator = RecordComparators.getRangeStartComparator(comp);
        //stop key GTRecord compare to stop key GTRecord
        this.rangeEndComparator = RecordComparators.getRangeEndComparator(comp);
        //start key GTRecord compare to stop key GTRecord
        this.rangeStartEndComparator = RecordComparators.getRangeStartEndComparator(comp);

        //replace the constant values in filter to dictionary codes 
        this.gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, gtInfo, mapping.getGtOrderColumns(), groupbyDims);

        this.gtDimensions = mapping.makeGridTableColumns(dimensions);
        this.gtAggrGroups = mapping.makeGridTableColumns(groupbyDims);
        this.gtAggrMetrics = mapping.makeGridTableColumns(metrics);
        this.gtAggrFuncs = mapping.makeAggrFuncs(metrics);

        if (rawSegment.getModel().getPartitionDesc().isPartitioned()) {
            int index = mapping.getIndexOf(rawSegment.getModel().getPartitionDesc().getPartitionDateColumnRef());
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
            scanRequest = new GTScanRequestBuilder().setInfo(gtInfo).setRanges(null).setDimensions(gtDimensions).setAggrGroupBy(gtAggrGroups).setAggrMetrics(gtAggrMetrics).setAggrMetricsFuncs(gtAggrFuncs).setFilterPushDown(gtFilter).createGTScanRequest();
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

                if ((isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare < 0) || (!isPartitionColUsingDatetimeEncoding && endCompare <= 0 && beginCompare <= 0)) {
                    //segment range is [Closed,Open), but segmentStartAndEnd.getSecond() might be rounded when using dict encoding, so use <= when has equals in condition. 
                } else {
                    logger.debug("Pre-check partition col filter failed, partitionColRef {}, segment start {}, segment end {}, range begin {}, range end {}", //
                            gtPartitionCol, makeReadable(gtStartAndEnd.getFirst()), makeReadable(gtStartAndEnd.getSecond()), makeReadable(range.begin), makeReadable(range.end));
                    return true;
                }
            }
        }
        return false;
    }
}
