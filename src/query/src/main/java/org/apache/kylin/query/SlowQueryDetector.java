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

package org.apache.kylin.query;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.Setter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class SlowQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger("query");

    @Getter
    private static final ConcurrentHashMap<Thread, QueryEntry> runningQueries = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CanceledSlowQueryStatus> canceledSlowQueriesStatus = Maps
            .newConcurrentMap();
    private final int detectionIntervalMs;
    private int queryTimeoutMs;

    public SlowQueryDetector() {
        super("SlowQueryDetector");
        this.setDaemon(true);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        this.detectionIntervalMs = kylinConfig.getSlowQueryDefaultDetectIntervalSeconds() * 1000;
        this.queryTimeoutMs = kylinConfig.getQueryTimeoutSeconds() * 1000;
    }

    // just for test.
    public SlowQueryDetector(int detectionIntervalMs, int queryTimeoutMs) {
        super("SlowQueryDetector");
        this.setDaemon(true);
        this.detectionIntervalMs = detectionIntervalMs;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    public void queryStart(String stopId) {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        runningQueries.put(currentThread(), new QueryEntry(System.currentTimeMillis(), currentThread(),
                QueryContext.current().getQueryId(), QueryContext.current().getUserSQL(), stopId, false));
    }

    public void queryEnd() {
        if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
            return;
        }
        QueryEntry entry = runningQueries.remove(currentThread());
        if (null != entry && null != canceledSlowQueriesStatus.get(entry.queryId)) {
            canceledSlowQueriesStatus.remove(entry.queryId);
            logger.debug("Remove query [{}] from canceledSlowQueriesStatus", entry.queryId);
        }
    }

    @Override
    public void run() {
        while (true) {
            checkStopByUser();
            checkTimeout();
            try {
                Thread.sleep(detectionIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // stop detection and exit
                return;
            }
        }
    }

    public static ConcurrentMap<String, CanceledSlowQueryStatus> getCanceledSlowQueriesStatus() {
        return canceledSlowQueriesStatus;
    }

    @VisibleForTesting
    public static void addCanceledSlowQueriesStatus(ConcurrentMap<String, CanceledSlowQueryStatus> slowQueriesStatus) {
        canceledSlowQueriesStatus.putAll(slowQueriesStatus);
    }

    @VisibleForTesting
    public static void clearCanceledSlowQueriesStatus() {
        canceledSlowQueriesStatus.clear();
    }

    private void checkStopByUser() {
        // interrupt query thread if Stop By User but running
        for (QueryEntry e : runningQueries.values()) {
            if (e.isStopByUser) {
                e.getThread().interrupt();
                logger.error("Trying to cancel query: {}", e.getThread().getName());
            }
        }
    }

    private void checkTimeout() {
        // interrupt query thread if timeout
        for (QueryEntry e : runningQueries.values()) {
            if (!e.setInterruptIfTimeout()) {
                continue;
            }

            try {
                CanceledSlowQueryStatus canceledSlowQueryStatus = canceledSlowQueriesStatus.get(e.getQueryId());
                if (null == canceledSlowQueryStatus) {
                    canceledSlowQueriesStatus.putIfAbsent(e.getQueryId(), new CanceledSlowQueryStatus(e.getQueryId(), 1,
                            System.currentTimeMillis(), e.getRunningTime()));
                    logger.debug("Query [{}] has been canceled 1 times, put to canceledSlowQueriesStatus", e.queryId);
                } else {
                    int canceledTimes = canceledSlowQueryStatus.getCanceledTimes() + 1;
                    canceledSlowQueriesStatus.put(e.getQueryId(), new CanceledSlowQueryStatus(e.getQueryId(),
                            canceledTimes, System.currentTimeMillis(), e.getRunningTime()));
                    logger.debug("Query [{}] has been canceled {} times", e.getQueryId(), canceledTimes);
                }
            } catch (Exception ex) {
                logger.error("Record slow query status failed!", ex);
            }
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public class QueryEntry {
        final long startTime;
        final Thread thread;
        final String queryId;
        final String sql;
        final String stopId;
        boolean isStopByUser;

        public long getRunningTime() {
            return (System.currentTimeMillis() - startTime) / 1000;
        }

        private boolean setInterruptIfTimeout() {
            long runningMs = System.currentTimeMillis() - startTime;
            if (runningMs >= queryTimeoutMs) {
                thread.interrupt();
                logger.error("Trying to cancel query: {}", thread.getName());
                return true;
            }

            return false;
        }
    }

    @Getter
    @AllArgsConstructor
    public static class CanceledSlowQueryStatus {
        public final String queryId;
        public final int canceledTimes;
        public final long lastCanceledTime;
        public final float queryDurationTime;
    }
}
