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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableSegment;
import io.kyligence.kap.gtrecord.RawTableSegmentScanner;
import io.kyligence.kap.gtrecord.SequentialRawTableTupleIterator;
import kap.google.common.collect.Sets;

public class RawTableStorageQuery implements IStorageQuery {

    private static final Logger logger = LoggerFactory.getLogger(RawTableStorageQuery.class);

    private RawTableInstance rawTableInstance;
    private RawTableDesc rawTableDesc;

    public RawTableStorageQuery(RawTableInstance rawTableInstance) {
        this.rawTableInstance = rawTableInstance;
        this.rawTableDesc = rawTableInstance.getRawTableDesc();
    }

    private void hackSelectStar(SQLDigest sqlDigest) {
        if (!sqlDigest.isRawQuery) {
            return;
        }

        // If it's select * from ...,
        // We need to retrieve cube to manually add columns into sqlDigest, so that we have full-columns results as output.
        boolean isSelectAll = sqlDigest.allColumns.isEmpty() || sqlDigest.allColumns.equals(sqlDigest.filterColumns);

        if (!isSelectAll)
            return;

        for (TblColRef col : this.rawTableDesc.getColumns()) {
            if (col.getTable().equals(sqlDigest.factTable)) {
                sqlDigest.allColumns.add(col);
            }
        }
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {

        //deal with participant columns in subquery join
        sqlDigest.includeSubqueryJoinParticipants();

        hackSelectStar(sqlDigest);

        // build dimension & metrics
        Set<TblColRef> dimensions = new LinkedHashSet<TblColRef>();
        Set<FunctionDesc> metrics = new LinkedHashSet<FunctionDesc>();
        buildDimensionsAndMetrics(sqlDigest, dimensions, metrics);

        enableStorageLimitIfPossible(sqlDigest, sqlDigest.filter, context);
        context.setFinalPushDownLimit(rawTableInstance);

        List<RawTableSegmentScanner> scanners = Lists.newArrayList();
        for (RawTableSegment rawTableSegment : rawTableInstance.getSegments(SegmentStatusEnum.READY)) {
            RawTableSegmentScanner scanner;
            if (rawTableSegment.getCubeSegment().getInputRecords() == 0) {
                if (!skipZeroInputSegment(rawTableSegment)) {
                    logger.warn("raw segment {} input record is 0, " + "it may caused by kylin failed to the job counter " + "as the hadoop history server wasn't running", rawTableSegment);
                } else {
                    logger.warn("raw segment {} input record is 0, skip it ", rawTableSegment);
                    continue;
                }
            }

            Set<TblColRef> groups = Sets.newHashSet();
            scanner = new RawTableSegmentScanner(rawTableSegment, dimensions, groups, Collections.<FunctionDesc> emptySet(), sqlDigest.filter, context);
            scanners.add(scanner);
        }
        return new SequentialRawTableTupleIterator(scanners, rawTableInstance, dimensions, metrics, returnTupleInfo, context);
    }

    private void enableStorageLimitIfPossible(SQLDigest sqlDigest, TupleFilter filter, StorageContext context) {
        boolean possible = true;

        boolean isRaw = sqlDigest.isRawQuery;
        if (!isRaw) {
            possible = false;
            logger.info("Storage limit push down it's a non-raw query");
        }

        boolean goodFilter = filter == null || TupleFilter.isEvaluableRecursively(filter);
        if (!goodFilter) {
            possible = false;
            logger.info("Storage limit push down is impossible because the filter is unevaluatable");
        }

        boolean goodSort = !context.hasSort();
        if (!goodSort) {
            possible = false;
            logger.info("Storage limit push down is impossible because the query has order by");
        }

        if (possible) {
            logger.info("Enable limit " + context.getLimit());
            context.enableLimit();
        }
    }

    private void buildDimensionsAndMetrics(SQLDigest sqlDigest, Collection<TblColRef> dimensions, Collection<FunctionDesc> metrics) {
        for (TblColRef column : sqlDigest.allColumns) {
            dimensions.add(column);
        }
    }

    protected boolean skipZeroInputSegment(RawTableSegment segment) {
        return segment.getConfig().isSkippingEmptySegments();
    }

}
