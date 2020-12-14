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

import static org.awaitility.Awaitility.await;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.query.exception.BusyQueryException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class QueryLimiterTest extends NLocalFileMetadataTestCase {
    private volatile boolean queryRejected = false;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        overwriteSystemProp("kylin.guardian.downgrade-mode-parallel-query-threshold", "3");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testDowngrade() {
        concurrenceQuery(10);

        Assert.assertFalse(queryRejected);

        QueryLimiter.downgrade();

        concurrenceQuery(10);

        Assert.assertTrue(queryRejected);
        QueryLimiter.recover();
    }

    @Test
    public void testDetail() throws InterruptedException {
        int times = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(times * 2);
        final CountDownLatch tasks = new CountDownLatch(times);
        for (int i = 0; i < times; i++) {
            int time = (i + 1) * 1000;
            executorService.submit(() -> {
                query(time);
                tasks.countDown();
            });
        }
        // at least finished 3 query
        await().atMost(5, TimeUnit.SECONDS).until(() -> tasks.getCount() <= 7);

        // have running query, downgrade state is false
        Assert.assertTrue(tasks.getCount() > 0);

        QueryLimiter.downgrade();

        final CountDownLatch tasks1 = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            executorService.submit(() -> {
                query(5000);
                tasks1.countDown();
            });
        }

        Thread.sleep(1000);

        // failed
        try {
            query(1000);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof BusyQueryException);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> tasks1.getCount() == 0);

        // success
        query(1000);

        Semaphore semaphore = QueryLimiter.getSemaphore();

        Assert.assertEquals(3, semaphore.availablePermits());
        QueryLimiter.recover();
    }

    private void concurrenceQuery(int times) {
        ExecutorService executorService = Executors.newFixedThreadPool(times);
        final CountDownLatch tasks = new CountDownLatch(times);

        for (int i = 0; i < times; i++) {
            executorService.submit(() -> {
                try {
                    query(1000);
                } catch (BusyQueryException e) {
                    queryRejected = true;
                }

                tasks.countDown();
            });
        }

        await().atMost(15, TimeUnit.SECONDS).until(() -> tasks.getCount() == 0);
        executorService.shutdown();
    }

    private void query(long millis) {
        QueryLimiter.tryAcquire();

        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            //
        } finally {
            QueryLimiter.release();
        }
    }
}
