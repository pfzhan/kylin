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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.NExecutableManager;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public abstract class BaseSchedulerTest extends NLocalFileMetadataTestCase {

    //TODO need to be rewritten
    // protected NDefaultScheduler scheduler;

    protected static NExecutableManager executableManager;
    protected static NExecutableDao executableDao;

    protected String project;

    protected AtomicInteger killProcessCount;

    public BaseSchedulerTest(String project) {
        this.project = project;
    }

    //TODO need to be rewritten
    /*
    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        killProcessCount = new AtomicInteger();
        val originExecutableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager = Mockito.spy(originExecutableManager);
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            killProcessCount.incrementAndGet();
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());
        executableDao = NExecutableDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
        startScheduler();
    }

    void startScheduler() {
        scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
    }

    protected void waitForJobFinish(String jobId) {
        waitForJobFinish(jobId, 120000);
    }

    protected void waitForJobFinish(String jobId, int maxWaitTime) {
        waitForJobByStatus(jobId, maxWaitTime, null, executableManager);
    }

    protected void waitForJobByStatus(String jobId, int maxWaitMilliseconds, final ExecutableState state,
            final NExecutableManager executableManager) {
        getConditionFactory(maxWaitMilliseconds).until(() -> {
            AbstractExecutable job = executableManager.getJob(jobId);
            ExecutableState status = job.getStatus();
            if (state != null) {
                return status == state;
            }
            return status == ExecutableState.SUCCEED || status == ExecutableState.ERROR
                    || status == ExecutableState.PAUSED || status == ExecutableState.DISCARDED
                    || status == ExecutableState.SUICIDAL;
        });
    }

    private ConditionFactory getConditionFactory(long maxWaitMilliseconds) {
        return with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(maxWaitMilliseconds, TimeUnit.MILLISECONDS);
    }

    protected final ConditionFactory getConditionFactory() {
        return getConditionFactory(60000);
    }
     */

}
