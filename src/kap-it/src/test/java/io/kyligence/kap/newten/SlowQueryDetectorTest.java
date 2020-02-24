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

package io.kyligence.kap.newten;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.query.pushdown.SparkSqlClient;
import java.util.UUID;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.spark.InfoHelper;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.status.api.v1.JobData;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowQueryDetectorTest extends NLocalWithSparkSessionTest {
    private SlowQueryDetector slowQueryDetector = null;

    private static final Logger logger = LoggerFactory.getLogger(SlowQueryDetectorTest.class);

    @Before
    public void setup() {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        slowQueryDetector = new SlowQueryDetector(100, 5 * 1000);
        slowQueryDetector.start();
    }

    @Override
    public String getProject() {
        return "match";
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
        slowQueryDetector.interrupt();
    }

    @Test
    public void testSetInterrupt() {
        slowQueryDetector.queryStart();
        try {
            Thread.sleep(6 * 1000);
            Assert.fail();
        } catch (InterruptedException e) {
            Assert.assertEquals("sleep interrupted", e.getMessage());
        }

        slowQueryDetector.queryEnd();
    }

    @Test
    public void testSparderTimeoutCancelJob() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        fullBuildCube("073198da-ce0e-4a0c-af38-cc27ae31cc0e", getProject());
        SparkSession ss = SparderEnv.getSparkSession();
        populateSSWithCSVData(config, getProject(), ss);

        System.setProperty("kap.query.engine.spark-sql-shuffle-partitions", "10000");
        slowQueryDetector.queryStart();
        try {
            SparderEnv.cleanCompute();
            long t = System.currentTimeMillis();
            NExecAndComp.queryCube(getProject(), "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME");
            String error = "TestSparderTimeoutCancelJob fail, query cost:" + (System.currentTimeMillis() - t)
                    + " ms, need compute:" + SparderEnv.needCompute();
            logger.error(error);
            Assert.fail(error);
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().isTimeout());
            Throwable cause = e.getCause();
            Assert.assertTrue(cause instanceof KylinTimeoutException);
            Assert.assertTrue(cause.getMessage().contains("Query timeout after:"));

            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
        System.clearProperty("kap.query.engine.spark-sql-shuffle-partitions");

        Thread.sleep(1000);
        JobData jobData = new InfoHelper(ss).getJobsByGroupId(Thread.currentThread().getName()).apply(0);
        Assert.assertEquals(1, jobData.numFailedStages());
    }

    @Test
    public void testPushdownTimeoutCancelJob() throws InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try {
            SparkSession ss = SparderEnv.getSparkSession();
            ss.sessionState().conf().setLocalProperty("spark.sql.shuffle.partitions", "10000");
            KylinConfig conf = KylinConfig.getInstanceFromEnv();
            conf.setProperty("kylin.query.pushdown.auto-set-shuffle-partitions-enabled", "false");
            populateSSWithCSVData(config, getProject(), ss);

            slowQueryDetector.queryStart();
            try {
                String sql = "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";
                SparkSqlClient.executeSql(ss, sql, UUID.randomUUID());
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(QueryContext.current().isTimeout());
                Assert.assertTrue(e instanceof KylinTimeoutException);
                Assert.assertTrue(e.getMessage().contains("Query timeout after:"));

                // reset query thread's interrupt state.
                Thread.interrupted();
            }
            slowQueryDetector.queryEnd();

            Thread.sleep(1000);
            JobData jobData = new InfoHelper(ss).getJobsByGroupId(Thread.currentThread().getName()).apply(0);
            Assert.assertEquals(1, jobData.numFailedStages());

        } finally {
            config.setProperty("kylin.query.pushdown.auto-set-shuffle-partitions-enabled", "true");
        }
    }
}
