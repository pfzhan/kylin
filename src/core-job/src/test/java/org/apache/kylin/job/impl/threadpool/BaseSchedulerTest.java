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

package org.apache.kylin.job.impl.threadpool;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.lock.MockJobLock;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public abstract class BaseSchedulerTest extends NLocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(BaseSchedulerTest.class);

    protected NDefaultScheduler scheduler;

    protected static NExecutableManager executableManager;

    protected String project;

    public BaseSchedulerTest() {

    }

    public BaseSchedulerTest(String project) {
        this.project = project;
    }

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.metadata.mq-url", "topic@mock");
        staticCreateTestMetadata();
        executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        startScheduler();

        val clazz = MessageQueue.class;
        val field = clazz.getDeclaredField("MQ_PROVIDERS");
        field.setAccessible(true);
        val providers = (Map<String, String>) field.get(null);
        providers.put("mock", "org.apache.kylin.job.impl.threadpool.MockMQ2");
    }

    void startScheduler() throws SchedulerException {
        scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        System.clearProperty("kylin.metadata.mq-url");
    }

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }

    protected void waitForJobFinish(String jobId) {
        waitForJobFinish(jobId, 60000);
    }

    protected void waitForJobFinish(String jobId, int maxWaitTime) {
        int error = 0;
        long start = System.currentTimeMillis();
        final int errorLimit = 3;
        while (error < errorLimit && (System.currentTimeMillis() - start < maxWaitTime)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                AbstractExecutable job = executableManager.getJob(jobId);
                ExecutableState status = job.getStatus();
                if (status == ExecutableState.SUCCEED || status == ExecutableState.ERROR
                        || status == ExecutableState.STOPPED || status == ExecutableState.DISCARDED) {
                    break;
                }
            } catch (Exception ex) {
                logger.error("", ex);
                error++;
            }
        }

        if (error >= errorLimit) {
            throw new RuntimeException("too many exceptions");
        }

        if (System.currentTimeMillis() - start >= maxWaitTime) {
            throw new RuntimeException("too long wait time");
        }
    }

    protected void waitForJobStatus(String jobId, ExecutableState state, long interval) {
        while (true) {
            AbstractExecutable job = executableManager.getJob(jobId);
            if (job.getStatus() == state) {
                break;
            } else {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    logger.error("waitForJobStatus error : " + e.getMessage(), e);
                }
            }
        }
    }

    protected void runningJobToError(String jobId) {
        while (true) {
            try {
                AbstractExecutable job = executableManager.getJob(jobId);
                ExecutableState status = job.getStatus();
                if (status == ExecutableState.RUNNING || status.isFinalState()) {
                    //                    scheduler.fetchFailed = true;
                    break;
                }
                Thread.sleep(2000);
            } catch (Exception ex) {
                logger.error("runningJobToError :" + ex.getMessage(), ex);
            }
        }
    }

}
