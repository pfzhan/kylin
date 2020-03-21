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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

/**
 * Holds per query information and statistics.
 */
public class QueryContext {

    private static final Logger logger = LoggerFactory.getLogger(QueryContext.class);

    public static final String PUSHDOWN_RDBMS = "RDBMS";
    public static final String PUSHDOWN_HIVE = "HIVE";
    public static final String PUSHDOWN_MOCKUP = "MOCKUP";
    public static final String PUSHDOWN_FILE = "FILE";

    public static final long DEFAULT_NULL_SCANNED_DATA = -1L;

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
    private AtomicLong sourceScanBytes = new AtomicLong();
    private AtomicLong sourceScanRows = new AtomicLong();
    private String userSQL;
    private boolean isTimeout;
    private String project;
    private Object calcitePlan;
    private boolean hasRuntimeAgg;
    private boolean hasLike;

    private long queryStartMillis;
    private long recordMillis;

    private boolean isSparderUsed;
    private boolean isTableIndex;

    private boolean isHighPriorityQuery = false;

    private ThreadLocal<Boolean> isAsyncQuery = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private Throwable finalCause;
    private Throwable olapCause;
    private String pushdownEngine;
    @Getter
    @Setter
    private boolean withoutSyntaxError;

    private String correctedSql;

    @Getter
    @Setter
    private int shufflePartitions;

    @Getter
    @Setter
    // Spark execution ID
    private String executionID = "";

    @Getter
    @Setter
    private List<Long> scanRows;

    @Getter
    @Setter
    private List<Long> scanBytes;

    @Getter
    @Setter
    private long scannedRows = DEFAULT_NULL_SCANNED_DATA;

    @Getter
    @Setter
    private long scannedBytes = DEFAULT_NULL_SCANNED_DATA;

    @Getter
    @Setter
    private boolean hasAdminPermission;

    private QueryContext() {
        // use QueryContext.current() instead
        queryStartMillis = System.currentTimeMillis();
        queryId = UUID.randomUUID().toString();
        recordMillis = queryStartMillis;
    }

    public static QueryContext current() {
        return contexts.get();
    }

    public long getQueryStartMillis() {
        return queryStartMillis;
    }

    public void setQueryStartMillis(long queryStartMillis) {
        this.queryStartMillis = queryStartMillis;
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

    public void setHasLike(boolean hasLike) {
        this.hasLike = hasLike;
    }

    public long getSourceScanBytes() {
        return sourceScanBytes.get();
    }

    public long addAndGetSourceScanBytes(long bytes) {
        return sourceScanBytes.addAndGet(bytes);
    }
    public long getSourceScanRows() {
        return sourceScanRows.get();
    }

    public long addAndGetSourceScanRows(long rows) {
        return sourceScanRows.addAndGet(rows);
    }

    public String getUserSQL() {
        return userSQL;
    }

    public void setUserSQL(String sql) {
        this.userSQL = sql;
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

    public boolean isTableIndex() {
        return isTableIndex;
    }

    public void setTableIndex(boolean tableIndex) {
        isTableIndex = tableIndex;
    }

    public void setIsAsyncQuery() {
        isAsyncQuery.set(true);
    }

    public Boolean isAsyncQuery() {
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

    public Throwable getFinalCause() {
        return finalCause;
    }

    public void setFinalCause(Throwable finalCause) {
        this.finalCause = finalCause;
    }

    public Throwable getOlapCause() {
        return olapCause;
    }

    public void setOlapCause(Throwable olapCause) {
        this.olapCause = olapCause;
    }

    public boolean hasRuntimeAgg() {
        return hasRuntimeAgg;
    }

    public void setHasRuntimeAgg(Boolean hasRuntimeAgg) {
        this.hasRuntimeAgg = hasRuntimeAgg;
    }

    public boolean isHighPriorityQuery() {
        return isHighPriorityQuery;
    }

    public void markHighPriorityQuery() {
        isHighPriorityQuery = true;
    }

    LinkedHashMap<String, String> queryRecord = new LinkedHashMap<>();

    public String getSchema() {
        return String.join(",", queryRecord.keySet());
    }

    public String getTimeLine() {
        return String.join(",", queryRecord.values());
    }

    public void record(String message) {
        long current = System.currentTimeMillis();
        long takeTime = current - recordMillis;
        queryRecord.put(message, takeTime + "");
        recordMillis = current;
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

    /**
     * update scanRows and calculate scannedRows
     * @param scanRows
     */
    public void updateAndCalScanRows(List<Long> scanRows) {
        setScanRows(scanRows);
        setScannedRows(calScannedValueWithDefault(scanRows));
    }

    /**
     * update scanBytes and calculate scannedBytes
     * @param scanBytes
     */
    public void updateAndCalScanBytes(List<Long> scanBytes) {
        setScanBytes(scanBytes);
        setScannedBytes(calScannedValueWithDefault(scanBytes));
    }

    /**
     * @param scanList
     * @return if scanList == null return default -1, else return sum of list
     */
    public static long calScannedValueWithDefault(List<Long> scanList) {
        if (Objects.isNull(scanList)) {
            return DEFAULT_NULL_SCANNED_DATA;
        } else {
            return scanList.stream().mapToLong(Long::longValue).sum();
        }
    }

}
