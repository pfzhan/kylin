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
package org.apache.kylin.query.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.exception.BusyQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class QueryLimiter {
    private static final Logger logger = LoggerFactory.getLogger(QueryLimiter.class);

    private static volatile boolean isDowngrading;
    private static final ThreadLocal<Boolean> downgradeState = new ThreadLocal<>();

    private static final Semaphore semaphore;
    static {
        semaphore = new Semaphore(KylinConfig.getInstanceFromEnv().getDowngradeParallelQueryThreshold(), true);
    }

    private QueryLimiter() {
    }

    @VisibleForTesting
    public static Semaphore getSemaphore() {
        return semaphore;
    }

    public static synchronized void downgrade() {
        if (!isDowngrading) {
            isDowngrading = true;
            logger.info("Query server state changed to downgrade");
        } else {
            logger.info("Query server is already in downgrading state");
        }
    }

    public static synchronized void recover() {
        if (isDowngrading) {
            isDowngrading = false;
            logger.info("Query server state changed to normal");
        } else {
            logger.info("Query server is already in normal state");
        }
    }

    public static void tryAcquire() {
        logger.info("query: {} try to get acquire, current server state: {}", QueryContext.current().getQueryId(),
                isDowngrading);
        downgradeState.set(isDowngrading);
        if (!isDowngrading) {
            return;
        }

        if (!semaphore.tryAcquire()) {
            // finally {} skip release lock
            downgradeState.set(false);

            logger.info("query: {} failed to get acquire", QueryContext.current().getQueryId());
            throw new BusyQueryException("Query rejected. Caused by query server is too busy");
        }

        logger.info("query: {} success to get acquire", QueryContext.current().getQueryId());
    }

    public static void release() {
        logger.info("query: {} try to release acquire, current server state: {}", QueryContext.current().getQueryId(),
                downgradeState.get());
        if (!downgradeState.get()) {
            return;
        }

        semaphore.release();
    }

    public static boolean getStatus() {
        return isDowngrading;
    }
}
