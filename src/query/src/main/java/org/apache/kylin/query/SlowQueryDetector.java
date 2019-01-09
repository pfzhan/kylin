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

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueryDetector extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(SlowQueryDetector.class);

    private final ConcurrentHashMap<Thread, QueryEntry> runningQueries = new ConcurrentHashMap<>();
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
    SlowQueryDetector(int detectionIntervalMs, int queryTimeoutMs) {
        super("SlowQueryDetector");
        this.setDaemon(true);
        this.detectionIntervalMs = detectionIntervalMs;
        this.queryTimeoutMs = queryTimeoutMs;
    }

    public void queryStart() {
        runningQueries.put(currentThread(), new QueryEntry(currentThread(), System.currentTimeMillis()));
    }

    public void queryEnd() {
        runningQueries.remove(currentThread());
    }

    @Override
    public void run() {
        while (true) {
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

    private void checkTimeout() {
        // interrupt query thread if timeout
        for (QueryEntry e : runningQueries.values()) {
            e.setInterruptIfTimeout();
        }
    }

    private class QueryEntry {
        final long startTime;
        final Thread thread;

        QueryEntry(Thread thread, long startTime) {
            this.startTime = startTime;
            this.thread = thread;
        }

        private void setInterruptIfTimeout() {
            long runningMs = System.currentTimeMillis() - startTime;
            if (runningMs >= queryTimeoutMs) {
                thread.interrupt();
                logger.error("Trying to cancel query:" + thread.getName());
            }
        }
    }

}
