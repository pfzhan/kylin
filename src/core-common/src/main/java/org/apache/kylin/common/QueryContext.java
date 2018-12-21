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

package org.apache.kylin.common;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds per query information and statistics.
 */
public class QueryContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryContext.class);

    public static final String PUSHDOWN_RDBMS = "RDBMS";
    public static final String PUSHDOWN_HIVE = "HIVE";
    public static final String PUSHDOWN_MOCKUP = "MOCKUP";

    private static final ThreadLocal<QueryContext> contexts = new ThreadLocal<QueryContext>() {
        @Override
        protected QueryContext initialValue() {
            return new QueryContext();
        }
    };

    // ============================================================================

    private String queryId;
    private String username;
    private Set<String> groups;
    private AtomicLong scannedRows = new AtomicLong();
    private AtomicLong scannedBytes = new AtomicLong();
    private AtomicLong sourceScanBytes = new AtomicLong();
    private String sql;
    private boolean isTimeout;
    private String project;
    private Object calcitePlan;
    private boolean hasRuntimeAgg;
    private boolean hasLike;

    private long queryStartMillis;
    private boolean isSparderUsed;

    private ThreadLocal<Boolean> isAsyncQuery = new ThreadLocal<Boolean>(){
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };


    private Throwable errorCause;
    private String pushdownEngine;

    private String correctedSql;

    private QueryContext() {
        // use QueryContext.current() instead
        queryStartMillis = System.currentTimeMillis();
        queryId = UUID.randomUUID().toString();
    }

    public static QueryContext current() {
        return contexts.get();
    }

    public long getQueryStartMillis() {
        return queryStartMillis;
    }

    public static void reset() {
        contexts.remove();
    }

    public String getQueryId() {
        return queryId == null ? "" : queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public void setHasLike(boolean hasLike){
        this.hasLike = hasLike;
    }

    public long getScannedRows() {
        return scannedRows.get();
    }

    public long getScannedBytes() {
        return scannedBytes.get();
    }

    public long getSourceScanBytes() {
        return sourceScanBytes.get();
    }

    public long addAndGetSourceScanBytes(long bytes) {
        return sourceScanBytes.addAndGet(bytes);
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isTimeout() {
        return isTimeout;
    }

    public void setTimeout(boolean timeout) {
        isTimeout = timeout;
    }

    public void setIsSparderUsed(boolean isSparderUsed) {
        this.isSparderUsed = isSparderUsed;
    }

    public void setIsAsyncQuery(){
        isAsyncQuery.set(true);
    }

    public Boolean isAsyncQuery(){
       return isAsyncQuery.get();
    }

    public Object getCalcitePlan() {
        return calcitePlan;
    }

    public void setCalcitePlan(Object calcitePlan) {
        this.calcitePlan = calcitePlan;
    }

    public String getPushdownEngine() {
        return pushdownEngine;
    }

    public void setPushdownEngine(String pushdownEngine) {
        this.pushdownEngine = pushdownEngine;
    }

    public long addAndGetScannedRows(long deltaRows) {
        return scannedRows.addAndGet(deltaRows);
    }

    public Throwable getErrorCause() {
        return errorCause;
    }

    public void setErrorCause(Throwable errorCause) {
        this.errorCause = errorCause;
    }

    public boolean hasRuntimeAgg() {
        return hasRuntimeAgg;
    }

    public void setHasRuntimeAgg(Boolean hasRuntimeAgg) {
        this.hasRuntimeAgg = hasRuntimeAgg;
    }
/*

    public Set<Future> getAllRunningTasks() {
        return allRunningTasks;
    }

    public void addRunningTasks(Future task) {
        this.allRunningTasks.add(task);
    }

    public void removeRunningTask(Future task) {
        this.allRunningTasks.remove(task);
    }

    public boolean isHighPriorityQuery() {
        return isHighPriorityQuery;
    }

    public void markHighPriorityQuery() {
        isHighPriorityQuery = true;
    }

    public long getQueryStartMillis() {
        return queryStartMillis;
    }

    public void checkMillisBeforeDeadline() {
        if (Thread.interrupted()) {
            throw new KylinTimeoutException("Query timeout");
        }
    }

    public int getScannedShards() {
        return scannedShards.get();
    }

    public void addScannedShards(int deltaFiles) {
        scannedShards.addAndGet(deltaFiles);
    }

    public long addAndGetScannedRows(long deltaRows) {
        return scannedRows.addAndGet(deltaRows);
    }

    public long addAndGetScannedBytes(long deltaBytes) {
        return scannedBytes.addAndGet(deltaBytes);
    }

    public Object getCalcitePlan() {
        return calcitePlan;
    }

    public void setCalcitePlan(Object calcitePlan) {
        this.calcitePlan = calcitePlan;
    }


    public boolean isSparderAppliable() {
        return isSparderAppliable;
    }

    public void setSparderAppliable(boolean isSparderAppliable) {
        this.isSparderAppliable = isSparderAppliable;
    }

    public boolean isSparderUsed() {
        return isSparderUsed;
    }

    public boolean isLateDecodeEnabled() {
        return isLateDecodeEnabled;
    }

    public void setLateDecodeEnabled(boolean lateDecodeEnabled) {
        isLateDecodeEnabled = lateDecodeEnabled;
    }

    public boolean isHasLike() {
        return hasLike;
    }

    public void setHasLike(boolean hasLike) {
        this.hasLike = hasLike;
    }

    public boolean isHasAdvance() {
        return hasAdvance;
    }

    public void setHasAdvance(boolean hasAdvance) {
        this.hasAdvance = hasAdvance;
    }
*/

    public String getCorrectedSql() {
        return correctedSql;
    }

    public void setCorrectedSql(String correctedSql) {
        this.correctedSql = correctedSql;
    }
}
