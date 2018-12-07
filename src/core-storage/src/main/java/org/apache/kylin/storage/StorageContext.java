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

package org.apache.kylin.storage;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;

import io.kyligence.kap.cube.cuboid.NLayoutCandidate;
import lombok.Getter;
import lombok.Setter;

/**
 * @author xjiang
 */
public class StorageContext {
    private static final Logger logger = LoggerFactory.getLogger(StorageContext.class);
    @Getter
    private int ctxId;
    private StorageURL connUrl;
    private int limit = Integer.MAX_VALUE;
    private boolean overlookOuterLimit = false;
    private int offset = 0;
    private int finalPushDownLimit = Integer.MAX_VALUE;
    private StorageLimitLevel storageLimitLevel = StorageLimitLevel.NO_LIMIT;
    private boolean hasSort = false;
    private boolean acceptPartialResult = false;
    private long deadline;

    private boolean exactAggregation = false;
    private boolean needStorageAggregation = false;
    private boolean enableCoprocessor = false;
    private boolean enableStreamAggregate = false;

    private IStorageQuery storageQuery;
    private Long cuboidId;

    private AtomicLong processedRowCount = new AtomicLong();
    private boolean partialResultReturned = false;

    private Range<Long> reusedPeriod;

    private NLayoutCandidate candidate;
    private TupleFilter filter;
    private Set<TblColRef> dimensions;
    private Set<FunctionDesc> metrics;
    @Getter @Setter
    private boolean useSnapshot = false;

    public StorageContext() {
    }

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
    }

    public StorageURL getConnUrl() {
        return connUrl;
    }

    public void setConnUrl(StorageURL connUrl) {
        this.connUrl = connUrl;
    }

    //the limit here correspond to the limit concept in SQL
    //also take into consideration Statement.setMaxRows in JDBC
    private int getLimit() {
        if (overlookOuterLimit || BackdoorToggles.getStatementMaxRows() == null
                || BackdoorToggles.getStatementMaxRows() == 0) {
            return limit;
        } else {
            return Math.min(limit, BackdoorToggles.getStatementMaxRows());
        }
    }

    public void setLimit(int l) {
        if (limit != Integer.MAX_VALUE) {
            logger.warn("Setting limit to {} but in current olap context, the limit is already {}, won't apply", l,
                    limit);
        } else {
            limit = l;
        }
    }

    //outer limit is sth like Statement.setMaxRows in JDBC
    public void setOverlookOuterLimit() {
        this.overlookOuterLimit = true;
    }

    //the offset here correspond to the offset concept in SQL
    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Long getCuboidId() {
        return cuboidId;
    }

    public void setCuboidId(Long cuboidId) {
        this.cuboidId = cuboidId;
    }

    public NLayoutCandidate getCandidate() {
        return candidate;
    }

    public void setCandidate(NLayoutCandidate candidate) {
        this.candidate = candidate;
    }

    public TupleFilter getFilter() {
        return filter;
    }

    public void setFilter(TupleFilter filter) {
        this.filter = filter;
    }

    public Set<TblColRef> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Set<TblColRef> dimensions) {
        this.dimensions = dimensions;
    }

    public Set<FunctionDesc> getMetrics() {
        return metrics;
    }

    public void setMetrics(Set<FunctionDesc> metrics) {
        this.metrics = metrics;
    }

    /**
     * in contrast to the limit in SQL concept, "limit push down" means
     * whether the limit is effective in storage level. Some queries are not possible
     * to leverage limit clause, checkout
     */
    public boolean isLimitPushDownEnabled() {
        return isValidPushDownLimit(finalPushDownLimit);
    }

    public static boolean isValidPushDownLimit(long finalPushDownLimit) {
        return finalPushDownLimit < Integer.MAX_VALUE && finalPushDownLimit > 0;
    }

    public int getFinalPushDownLimit() {
        return finalPushDownLimit;
    }

    public StorageLimitLevel getStorageLimitLevel() {
        return storageLimitLevel;
    }

    public void applyLimitPushDown(IRealization realization, StorageLimitLevel storageLimitLevel) {

        if (storageLimitLevel == StorageLimitLevel.NO_LIMIT) {
            return;
        }

        if (!realization.supportsLimitPushDown()) {
            logger.warn("Not enabling limit push down because cube storage type not supported");
            return;
        }

        int temp = this.getOffset() + this.getLimit();

        if (!isValidPushDownLimit(temp)) {
            logger.warn("Not enabling limit push down because current limit is invalid: {}", this.getLimit());
            return;
        }

        this.finalPushDownLimit = temp;
        this.storageLimitLevel = storageLimitLevel;
        logger.info("Enabling limit push down: {} at level: {}", temp, storageLimitLevel);
    }

    public boolean mergeSortPartitionResults() {
        return mergeSortPartitionResults(finalPushDownLimit);
    }

    public static boolean mergeSortPartitionResults(int finalPushDownLimit) {
        return isValidPushDownLimit(finalPushDownLimit);
    }

    public long getDeadline() {
        return this.deadline;
    }

    public void setDeadline(IRealization realization) {
        int timeout = realization.getConfig().getQueryTimeoutSeconds() * 1000;
        if (timeout == 0) {
            this.deadline = Long.MAX_VALUE;
        } else {
            this.deadline = timeout + System.currentTimeMillis();
        }
    }

    public void markSort() {
        this.hasSort = true;
    }

    public boolean hasSort() {
        return this.hasSort;
    }

    public long getProcessedRowCount() {
        return processedRowCount.get();
    }

    public long increaseProcessedRowCount(long count) {
        return processedRowCount.addAndGet(count);
    }

    public boolean isAcceptPartialResult() {
        return acceptPartialResult;
    }

    public void setAcceptPartialResult(boolean acceptPartialResult) {
        this.acceptPartialResult = acceptPartialResult;
    }

    public boolean isPartialResultReturned() {
        return partialResultReturned;
    }

    public void setPartialResultReturned(boolean partialResultReturned) {
        this.partialResultReturned = partialResultReturned;
    }

    public boolean isNeedStorageAggregation() {
        return needStorageAggregation;
    }

    public void setNeedStorageAggregation(boolean needStorageAggregation) {
        this.needStorageAggregation = needStorageAggregation;
    }

    public void setExactAggregation(boolean isExactAggregation) {
        this.exactAggregation = isExactAggregation;
    }

    public boolean isExactAggregation() {
        return this.exactAggregation;
    }

    public void enableCoprocessor() {
        this.enableCoprocessor = true;
    }

    public boolean isCoprocessorEnabled() {
        return this.enableCoprocessor;
    }

    public Range<Long> getReusedPeriod() {
        return reusedPeriod;
    }

    public void setReusedPeriod(Range<Long> reusedPeriod) {
        this.reusedPeriod = reusedPeriod;
    }

    public IStorageQuery getStorageQuery() {
        return storageQuery;
    }

    public void setStorageQuery(IStorageQuery storageQuery) {
        this.storageQuery = storageQuery;
    }

    public boolean isStreamAggregateEnabled() {
        return enableStreamAggregate;
    }

    public void enableStreamAggregate() {
        this.enableStreamAggregate = true;
    }
}
