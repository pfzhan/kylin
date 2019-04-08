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
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;

import com.google.common.collect.Range;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xjiang
 */
@Slf4j
public class StorageContext {
    @Getter
    private int ctxId;
    @Getter
    @Setter
    private StorageURL connUrl;
    private int limit = Integer.MAX_VALUE;
    private boolean overlookOuterLimit = false;

    @Getter
    @Setter
    private int offset = 0; //the offset here correspond to the offset concept in SQL

    @Getter
    private int finalPushDownLimit = Integer.MAX_VALUE;
    @Getter
    private StorageLimitLevel storageLimitLevel = StorageLimitLevel.NO_LIMIT;
    private boolean hasSort = false;
    @Getter
    @Setter
    private boolean acceptPartialResult = false;

    @Getter
    private long deadline;

    @Getter
    @Setter
    private boolean exactAggregation = false;

    @Getter
    @Setter
    private boolean needStorageAggregation = false;
    @Getter
    private boolean enableCoprocessor = false;
    private boolean enableStreamAggregate = false;

    @Getter
    @Setter
    private IStorageQuery storageQuery;

    @Getter
    @Setter
    private Long cuboidLayoutId = -1L;

    private AtomicLong processedRowCount = new AtomicLong();

    @Getter
    @Setter
    private boolean partialResultReturned = false;

    @Getter
    @Setter
    private Range<Long> reusedPeriod;

    @Getter
    @Setter
    private NLayoutCandidate candidate;

    @Getter
    @Setter
    private TupleFilter filter;

    @Getter
    @Setter
    private Set<TblColRef> dimensions;

    @Getter
    @Setter
    private Set<FunctionDesc> metrics;

    @Getter
    @Setter
    private boolean useSnapshot = false;

    public StorageContext() {
    }

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
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
            log.warn("Setting limit to {} but in current olap context, the limit is already {}, won't apply", l, limit);
        } else {
            limit = l;
        }
    }

    //outer limit is sth like Statement.setMaxRows in JDBC
    public void setOverlookOuterLimit() {
        this.overlookOuterLimit = true;
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

    public void applyLimitPushDown(IRealization realization, StorageLimitLevel storageLimitLevel) {

        if (storageLimitLevel == StorageLimitLevel.NO_LIMIT) {
            return;
        }

        if (!realization.supportsLimitPushDown()) {
            log.warn("Not enabling limit push down because cube storage type not supported");
            return;
        }

        int temp = this.getOffset() + this.getLimit();

        if (!isValidPushDownLimit(temp)) {
            log.warn("Not enabling limit push down because current limit is invalid: {}", this.getLimit());
            return;
        }

        this.finalPushDownLimit = temp;
        this.storageLimitLevel = storageLimitLevel;
        log.info("Enabling limit push down: {} at level: {}", temp, storageLimitLevel);
    }

    public boolean mergeSortPartitionResults() {
        return mergeSortPartitionResults(finalPushDownLimit);
    }

    public static boolean mergeSortPartitionResults(int finalPushDownLimit) {
        return isValidPushDownLimit(finalPushDownLimit);
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

    public void enableCoprocessor() {
        this.enableCoprocessor = true;
    }

    public boolean isStreamAggregateEnabled() {
        return enableStreamAggregate;
    }

    public void enableStreamAggregate() {
        this.enableStreamAggregate = true;
    }
}
