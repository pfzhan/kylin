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

package io.kyligence.kap.rest.service.task;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHistoryTaskSchedulerRunnerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private QueryHistoryTaskScheduler qhAccelerateScheduler;

    @Before
    public void setUp() {
        createTestMetadata();
        new SpringContext().setApplicationContext(null);
        qhAccelerateScheduler = Mockito.spy(new QueryHistoryTaskScheduler(PROJECT));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testQueryHistoryAccelerateRunner() {

        long startTime = System.currentTimeMillis();
        val internalExecute = Lists.<Long> newArrayList();

        val mockSleepTimeSecs = 1;
        val mockSchedulerDelay = 2;

        val queryHistoryAccelerateRunnerMock = qhAccelerateScheduler.new QueryHistoryAccelerateRunner(false) {
            @Override
            public void work() {
                try {
                    TimeUnit.SECONDS.sleep(mockSleepTimeSecs);
                    internalExecute.add((System.currentTimeMillis() - startTime) / 1000);

                    //mock exception
                    throw new RuntimeException("test for exception");
                } catch (InterruptedException e) {
                    log.error("queryHistoryAccelerateRunnerMock is interrupted", e);
                }
            }

        };

        val queryHistoryMetaUpdateRunnerMock = qhAccelerateScheduler.new QueryHistoryMetaUpdateRunner() {
            @Override
            public void work() {
                try {
                    TimeUnit.SECONDS.sleep(mockSleepTimeSecs);
                } catch (InterruptedException e) {
                    log.error("queryHistoryMetaUpdateRunner is interrupted", e);
                }
            }

        };

        ReflectionTestUtils.setField(qhAccelerateScheduler, "taskScheduler", Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("QueryHistoryWorker(project:" + PROJECT + ")")));

        try {
            val schedulerService = (ScheduledExecutorService) ReflectionTestUtils.getField(qhAccelerateScheduler,
                    "taskScheduler");

            schedulerService.scheduleWithFixedDelay(queryHistoryAccelerateRunnerMock, 0, mockSchedulerDelay,
                    TimeUnit.SECONDS);
            schedulerService.scheduleWithFixedDelay(queryHistoryMetaUpdateRunnerMock, 0, mockSchedulerDelay,
                    TimeUnit.SECONDS);

            val schedulerNum = 10;

            TimeUnit.SECONDS.sleep(schedulerNum);

            Assert.assertEquals(internalExecute.size(), schedulerNum / (mockSchedulerDelay + mockSleepTimeSecs));

            for (int i = 0; i < internalExecute.size(); i++) {
                Assert.assertEquals(internalExecute.get(i), i * mockSchedulerDelay + mockSleepTimeSecs * (i + 1), 1);
            }
        } catch (Exception e) {
            log.error("test qhAccelerateScheduler error :", e);
        } finally {
            QueryHistoryTaskScheduler.shutdownByProject(PROJECT);
        }
    }

}