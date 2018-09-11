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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;

import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.rest.request.SQLRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class SlowQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(SlowQueryDetector.class);
    public static final int ONE_MB = 1024 * 1024;

    private final ConcurrentMap<Thread, Entry> runningQueries = Maps.newConcurrentMap();
    private final long detectionInterval;
    private final int alertMB;
    private final int alertRunningSec;
    private KylinConfig kylinConfig;
    private ArrayList<Notifier> notifiers = new ArrayList<>();
    private int queryTimeoutSeconds;

    public SlowQueryDetector() {
        super("SlowQueryDetector");
        this.setDaemon(true);
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        //todo
        this.detectionInterval = kylinConfig.getSlowQueryDefaultDetectIntervalSeconds() * 1000L;
        this.alertMB = 100;
        this.alertRunningSec = kylinConfig.getSlowQueryDefaultAlertingSeconds();
        this.queryTimeoutSeconds = kylinConfig.getQueryTimeoutSeconds();

        initNotifiers();
    }

    public SlowQueryDetector(long detectionInterval, int alertMB, int alertRunningSec, int queryTimeoutSeconds) {
        super("SlowQueryDetector");
        this.setDaemon(true);
        this.detectionInterval = detectionInterval;
        this.alertMB = alertMB;
        this.alertRunningSec = alertRunningSec;
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.queryTimeoutSeconds = queryTimeoutSeconds;

        initNotifiers();
    }

    public static long getSystemAvailBytes() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory(); // current heap allocated to the VM process
        long freeMemory = runtime.freeMemory(); // out of the current heap, how much is free
        long maxMemory = runtime.maxMemory(); // Max heap VM can use e.g. Xmx setting
        long usedMemory = totalMemory - freeMemory; // how much of the current heap the VM is using
        long availableMemory = maxMemory - usedMemory; // available memory i.e. Maximum heap size minus the current amount used
        return availableMemory;
    }

    public static int getSystemAvailMB() {
        return (int) (getSystemAvailBytes() / ONE_MB);
    }

    private void initNotifiers() {
        this.notifiers.add(new LoggerNotifier());
        this.notifiers.add(new PersistenceNotifier());
    }

    public void registerNotifier(Notifier notifier) {
        notifiers.add(notifier);
    }

    private void notify(Entry e) {
        float runningSec = (float) (System.currentTimeMillis() - e.startTime) / 1000;

        for (Notifier notifier : notifiers) {
            try {
                notifier.slowQueryFound(runningSec, //
                        e.startTime, e.sqlRequest.getProject(), e.sqlRequest.getSql(), e.user, e.thread);
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    public void queryStart(Thread thread, SQLRequest sqlRequest, String user, long startTime) {
        runningQueries.put(thread, new Entry(sqlRequest, user, thread, startTime));
    }

    public void queryEnd(Thread thread) {
        runningQueries.remove(thread);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(detectionInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // stop detection and exit
                return;
            }

            try {
                detectSlowQuery();
            } catch (Exception ex) {
                logger.error("", ex);
            }
        }
    }

    private void detectSlowQuery() {
        long now = System.currentTimeMillis();
        ArrayList<Entry> entries = new ArrayList<Entry>(runningQueries.values());
        Collections.sort(entries);

        // report if query running long
        for (Entry e : entries) {
            float runningSec = (float) (now - e.startTime) / 1000;
            setQueryThreadInterrupted(e, runningSec);

            if (runningSec >= alertRunningSec) {
                notify(e);
                dumpStackTrace(e.thread);
            } else {
                break; // entries are sorted by startTime
            }
        }

        // report if low memory
        if (getSystemAvailMB() < alertMB) {
            logger.info("System free memory less than " + alertMB + " MB. " + entries.size() + " queries running.");
        }
    }

    private void setQueryThreadInterrupted(Entry e, float runningSec) {
        if (queryTimeoutSeconds != 0 && runningSec >= queryTimeoutSeconds) {
            e.thread.interrupt();
            logger.error("Trying to cancel query:" + e.thread.getName());
        }
    }

    // log the stack trace of slow query thread for further analysis
    private void dumpStackTrace(Thread t) {
        int maxStackTraceDepth = kylinConfig.getSlowQueryStackTraceDepth();
        int current = 0;

        StackTraceElement[] stackTrace = t.getStackTrace();
        StringBuilder buf = new StringBuilder("Problematic thread 0x" + Long.toHexString(t.getId()));
        buf.append("\n");
        for (StackTraceElement e : stackTrace) {
            if (++current > maxStackTraceDepth) {
                break;
            }
            buf.append("\t").append("at ").append(e.toString()).append("\n");
        }
        logger.info(buf.toString());
    }

    public interface Notifier {
        void slowQueryFound(float runningSec, long startTime, String project, String sql, String user,
                Thread t) throws IOException;
    }

    private class LoggerNotifier implements Notifier {
        @Override
        public void slowQueryFound(float runningSec, long startTime, String project, String sql,
                String user, Thread t) {
            logger.info("{} query has been running {} seconds (project:{}, thread: 0x{}, user:{}) -- {}", QueryHistory.ADJ_SLOW,
                    runningSec, project, Long.toHexString(t.getId()), user, sql);
        }
    }

    private class PersistenceNotifier implements Notifier {
        String serverHostname;

        public PersistenceNotifier() {
            try {
                serverHostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                serverHostname = "Unknow";
                logger.warn("Error in get current hostname.", e);
            }
        }

        @Override
        public void slowQueryFound(float runningSec, long startTime, String project, String sql, String user, Thread t) throws IOException {
                QueryHistory entry = new QueryHistory(QueryContext.current().getQueryId(), sql, startTime, runningSec,
                        serverHostname, t.getName(), user);
                entry.setQueryStatus(QueryHistoryStatusEnum.FAILED);
                QueryHistoryManager.getInstance(kylinConfig, project).save(entry);
        }
    }

    private class Entry implements Comparable<Entry> {
        final SQLRequest sqlRequest;
        final long startTime;
        final Thread thread;
        final String user;

        Entry(SQLRequest sqlRequest, String user, Thread thread, long startTime) {
            this.sqlRequest = sqlRequest;
            this.startTime = startTime;
            this.thread = thread;
            this.user = user;
        }

        @Override
        public int compareTo(Entry o) {
            return (int) (this.startTime - o.startTime);
        }
    }

}
