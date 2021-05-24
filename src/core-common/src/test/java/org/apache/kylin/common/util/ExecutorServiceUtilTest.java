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
package org.apache.kylin.common.util;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.util.LogOutputTestCase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutorServiceUtilTest extends LogOutputTestCase {

    @Test
    @Ignore
    public void testShutdownGracefully() throws Exception {
        ExecutorService pool = Executors.newScheduledThreadPool(1);
        pool.execute(() -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.error("thread interrupted.");
            }
        });
        Thread.sleep(2000);
        ExecutorServiceUtil.shutdownGracefully(pool, 1);
        Assert.assertTrue(containsLog("thread interrupted."));
    }

    @Test
    public void testForceShutdown() {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        Future task = pool.schedule(() -> log.info("thread execute"), 10, TimeUnit.SECONDS);
        ExecutorServiceUtil.forceShutdown(pool);
        try {
            //Avoid getting stuck
            task.get(60, TimeUnit.SECONDS);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof CancellationException);
        }
        Assert.assertFalse(containsLog("thread execute"));
    }

}
