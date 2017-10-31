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

package io.kyligence.kap.storage.parquet.cube.raw;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.storage.gtrecord.RawTableSegmentScanner;
import io.kyligence.kap.storage.gtrecord.SequentialRawTableTupleIterator;

public class RawTableStorageQuery implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(RawTableStorageQuery.class);

    private RawTableInstance rawTableInstance;

    public RawTableStorageQuery(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
    }

    private void hackSelectStar(SQLDigest sqlDigest) {
        // this is no longer needed thanks to SqlToRelConverter.hackSelectStar()
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        hackSelectStar(sqlDigest);

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        enableStorageLimitIfPossible(sqlDigest, sqlDigest.filter, context);
        // set query deadline
        context.setDeadline(rawTableInstance);

        List<RawTableSegmentScanner> scanners = Lists.newArrayList();
        for (RawTableSegment rawTableSegment : rawTableInstance.getSegments(SegmentStatusEnum.READY)) {
            RawTableSegmentScanner scanner;
            if (rawTableSegment.getCubeSegment().getInputRecords() == 0) {
                if (!skipZeroInputSegment(rawTableSegment)) {
                    logger.warn(
                            "raw segment {} input record is 0, " + "it may caused by kylin failed to the job counter "
                                    + "as the hadoop history server wasn't running",
                            rawTableSegment);
                } else {
                    logger.warn("raw segment {} input record is 0, skip it ", rawTableSegment);
                    continue;
                }
            }

            Set<TblColRef> groups = new HashSet<>();
            scanner = new RawTableSegmentScanner(rawTableSegment, dimensions, groups,
                    Collections.<FunctionDesc> emptySet(), sqlDigest.filter, context);
            scanners.add(scanner);
        }
        return new SequentialRawTableTupleIterator(scanners, rawTableInstance, dimensions, metrics, returnTupleInfo,
                context);
    }

    private void enableStorageLimitIfPossible(SQLDigest sqlDigest, TupleFilter filter, StorageContext context) {
        StorageLimitLevel storageLimitLevel = StorageLimitLevel.LIMIT_ON_SCAN;

        boolean isRaw = sqlDigest.isRawQuery;
        if (!isRaw && !sqlDigest.limitPrecedesAggr) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because it's after aggregation");
        }

        boolean goodFilter = filter == null || TupleFilter.isEvaluableRecursively(filter);
        if (!goodFilter) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because the filter is unevaluatable");
        }

        boolean goodSort = !context.hasSort();
        if (!goodSort) {
            storageLimitLevel = StorageLimitLevel.NO_LIMIT;
            logger.info("storageLimitLevel set to NO_LIMIT because the query has order by");
        }

        context.applyLimitPushDown(rawTableInstance, storageLimitLevel);
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions,
            Collection<FunctionDesc> metrics) {
        for (TblColRef column : sqlDigest.allColumns) {
            dimensions.add(column);
        }
    }

    protected boolean skipZeroInputSegment(RawTableSegment segment) {
        return segment.getConfig().isSkippingEmptySegments();
    }

}
