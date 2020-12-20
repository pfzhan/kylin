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

import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.ttl.TransmittableThreadLocal;

import lombok.Getter;
import lombok.Setter;

/**
 * Holds per query information and statistics.
 */
public class QueryContext implements Closeable {

    public static final String PUSHDOWN_RDBMS = "RDBMS";
    public static final String PUSHDOWN_HIVE = "HIVE";
    public static final String PUSHDOWN_MOCKUP = "MOCKUP";
    public static final String PUSHDOWN_OBJECT_STORAGE = "OBJECT STORAGE";

    public static final long DEFAULT_NULL_SCANNED_DATA = -1L;

    public static final List<Long> DEFAULT_SCANNED_DATA = Collections.emptyList();

    private static final TransmittableThreadLocal<QueryContext> contexts = new TransmittableThreadLocal<QueryContext>() {
        @Override
        protected QueryContext initialValue() {
            return new QueryContext();
        }
    };

    @Setter
    private String queryId;
    @Getter
    @Setter
    private String project;
    private long recordMillis;
    @Getter
    @Setter
    private Object calcitePlan;
    @Getter
    @Setter
    private String pushdownEngine;
    @Getter
    @Setter
    private int shufflePartitions;
    @Getter
    @Setter
    // Spark execution ID
    private String executionID = "";
    @Getter
    @Setter
    private String userSQL;

    @Getter
    @Setter
    private String[] modelPriorities = new String[0];

    @Getter
    private QueryTrace queryTrace = new QueryTrace();

    private QueryContext() {
        // use QueryContext.current() instead
        queryId = UUID.randomUUID().toString();
        recordMillis = System.currentTimeMillis();
    }

    public static QueryContext current() {
        return contexts.get();
    }

    public static QueryTrace currentTrace() {
        return contexts.get().getQueryTrace();
    }

    public static void reset() {
        contexts.remove();
    }

    public String getQueryId() {
        return queryId == null ? "" : queryId;
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

    @Override
    public void close() {
        reset();
    }

    // ============================================================================
    @Getter
    @Setter
    private AclInfo aclInfo;

    @Getter
    @Setter
    public static class AclInfo {

        private String username;
        private Set<String> groups;
        private boolean hasAdminPermission;

        public AclInfo(String username, Set<String> groups, boolean hasAdminPermission) {
            this.username = username;
            this.groups = groups;
            this.hasAdminPermission = hasAdminPermission;
        }
    }

    // ============================================================================
    /**
     * query metrics
     */
    @Getter
    @Setter
    private Metrics metrics = new Metrics();

    @Getter
    @Setter
    public class Metrics {
        private String correctedSql;
        private Throwable finalCause;
        private Throwable olapCause;
        private boolean exactlyMatch;
        private int segCount;

        private AtomicLong sourceScanBytes = new AtomicLong();
        private AtomicLong sourceScanRows = new AtomicLong();
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

        /**
         * update scanRows and calculate scannedRows
         *
         * @param scanRows
         */
        public void updateAndCalScanRows(List<Long> scanRows) {
            setScanRows(scanRows);
            setScannedRows(calScannedValueWithDefault(scanRows));
        }

        /**
         * update scanBytes and calculate scannedBytes
         *
         * @param scanBytes
         */
        public void updateAndCalScanBytes(List<Long> scanBytes) {
            setScanBytes(scanBytes);
            setScannedBytes(calScannedValueWithDefault(scanBytes));
        }
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

    public static void fillEmptyResultSetMetrics() {
        QueryContext.current().getMetrics().updateAndCalScanRows(QueryContext.DEFAULT_SCANNED_DATA);
        QueryContext.current().getMetrics().updateAndCalScanBytes(QueryContext.DEFAULT_SCANNED_DATA);
    }

    // ============================================================================
    /**
     * queryTagInfo
     */
    @Getter
    @Setter
    private QueryTagInfo queryTagInfo = new QueryTagInfo();

    @Getter
    @Setter
    public class QueryTagInfo {

        private boolean isTimeout;
        private boolean hasRuntimeAgg;
        private boolean hasLike;
        private boolean isSparderUsed;
        private boolean isTableIndex;
        private boolean isHighPriorityQuery = false;
        private boolean withoutSyntaxError;
        private boolean isAsyncQuery;
        private String fileFormat;
        private String fileEncode;
    }

}
