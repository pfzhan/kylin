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

package org.apache.kylin.rest.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.kyligence.kap.metadata.project.NProjectManager;

public class QueryRequestLimits implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(QueryRequestLimits.class);

    private static LoadingCache<String, AtomicInteger> runningStats = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<String, AtomicInteger>() {
                @Override
                public void onRemoval(RemovalNotification<String, AtomicInteger> notification) {
                    logger.info("Current running query number " + notification.getValue().get() + " for project "
                            + notification.getKey() + " is removed due to " + notification.getCause());
                }
            }).expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, AtomicInteger>() {
                @Override
                public AtomicInteger load(String s) throws Exception {
                    return new AtomicInteger(0);
                }
            });

    static boolean openQueryRequest(String project, int maxConcurrentQuery) {
        if (maxConcurrentQuery == 0) {
            return true;
        }
        try {
            AtomicInteger nRunningQueries = runningStats.get(project);
            for (;;) {
                int nRunning = nRunningQueries.get();
                if (nRunning < maxConcurrentQuery) {
                    if (nRunningQueries.compareAndSet(nRunning, nRunning + 1)) {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    static void closeQueryRequest(String project, int maxConcurrentQuery) {
        if (maxConcurrentQuery == 0) {
            return;
        }
        AtomicInteger nRunningQueries = runningStats.getIfPresent(project);
        if (nRunningQueries != null) {
            nRunningQueries.decrementAndGet();
        }
    }

    public static Integer getCurrentRunningQuery(String project) {
        AtomicInteger nRunningQueries = runningStats.getIfPresent(project);
        if (nRunningQueries != null) {
            return nRunningQueries.get();
        } else {
            return null;
        }
    }

    // ============================================================================

    final private String project;
    final private int maxConcurrentQuery;

    public QueryRequestLimits(String project) {
        this.project = project;

        NProjectManager mgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance prj = mgr.getProject(project);
        this.maxConcurrentQuery = prj.getConfig().getQueryConcurrentRunningThresholdForProject();

        boolean ok = openQueryRequest(project, maxConcurrentQuery);
        if (!ok) {
            Message msg = MsgPicker.getMsg();
            logger.warn("Directly return exception as too many concurrent query requests for project:" + project);
            throw new KylinException("KE-1005", msg.getQUERY_TOO_MANY_RUNNING());
        }
    }

    @Override
    public void close() {
        closeQueryRequest(project, maxConcurrentQuery);
    }
}
