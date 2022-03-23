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



package io.kyligence.kap.newten;

import java.io.File;

import io.kyligence.kap.util.ExecAndComp;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.util.QueryParams;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.pushdown.SparkSqlClient;
import io.kyligence.kap.query.runtime.plan.ResultPlan;
import io.kyligence.kap.query.util.KapQueryUtil;
import lombok.val;

public class SlowQueryDetectorTest extends NLocalWithSparkSessionTest {
    private SlowQueryDetector slowQueryDetector = null;

    private static final Logger logger = LoggerFactory.getLogger(SlowQueryDetectorTest.class);
    private static final int TIMEOUT_MS = 5 * 1000;

    @Before
    public void setup() {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        slowQueryDetector = new SlowQueryDetector(100, TIMEOUT_MS);
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
        slowQueryDetector.interrupt();
    }

    @Test
    public void testSetInterrupt() {
        slowQueryDetector.queryStart("");
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
        val df = SparderEnv.getSparkSession().emptyDataFrame();
        val mockDf = Mockito.spy(df);
        Mockito.doAnswer((p) -> {
            Thread.sleep(TIMEOUT_MS * 3);
            return null;
        }).when(mockDf).toIterator();
        slowQueryDetector.queryStart("");
        try {
            SparderEnv.cleanCompute();
            long t = System.currentTimeMillis();
            ResultPlan.getResult(mockDf, null);
            ExecAndComp.queryModel(getProject(), "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME");
            String error = "TestSparderTimeoutCancelJob fail, query cost:" + (System.currentTimeMillis() - t)
                    + " ms, need compute:" + SparderEnv.needCompute();
            logger.error(error);
            Assert.fail(error);
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertEquals(
                    "The query exceeds the set time limit of 300s. Current step: Collecting dataset for sparder. ",
                    e.getMessage());
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Test
    public void testPushdownTimeoutCancelJob() throws InterruptedException {
        val df = SparderEnv.getSparkSession().emptyDataFrame();
        val mockDf = Mockito.spy(df);
        Mockito.doAnswer((p) -> {
            Thread.sleep(TIMEOUT_MS * 3);
            return null;
        }).when(mockDf).toIterator();
        slowQueryDetector.queryStart("");
        try {
            String sql = "select sum(price) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME";
            SparkSqlClient.dfToList(ss, "", mockDf);
            SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), getProject());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertEquals(
                    "The query exceeds the set time limit of 300s. Current step: Collecting dataset for push-down. ",
                    e.getMessage()
            );
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Ignore("not timeout, need another sql")
    @Test
    public void testSQLMassageTimeoutCancelJob() throws Exception {
        slowQueryDetector.queryStart("");
        try {
            SparderEnv.cleanCompute();
            long t = System.currentTimeMillis();
            String sql = FileUtils
                    .readFileToString(new File("src/test/resources/query/sql_timeout/query03.sql"), "UTF-8").trim();
            QueryParams queryParams = new QueryParams(KapQueryUtil.getKylinConfig(getProject()), sql, getProject(), 0, 0,
                    "DEFAULT", true);
            KapQueryUtil.massageSql(queryParams);
            String error = "TestSQLMassageTimeoutCancelJob fail, query cost:" + (System.currentTimeMillis() - t)
                    + " ms, need compute:" + SparderEnv.needCompute();
            logger.error(error);
            Assert.fail(error);
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e instanceof KylinTimeoutException);
            Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("QueryUtil"));
            // reset query thread's interrupt state.
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }

    @Ignore
    @Test
    public void testRealizationChooserTimeout() {
        slowQueryDetector.queryStart("");
        try {
            long t = System.currentTimeMillis();
            Thread.sleep(TIMEOUT_MS - 10);
            val queryExec = new QueryExec("default", getTestConfig());
            queryExec.executeQuery("select cal_dt,sum(price) from test_kylin_fact group by "
                    + "cal_dt union all select cal_dt,sum(price) from test_kylin_fact group by cal_dt");
            Assert.fail("testRealizationChooserTimeout fail, query cost:" + (System.currentTimeMillis() - t) + " ms");
        } catch (Exception e) {
            Assert.assertTrue(QueryContext.current().getQueryTagInfo().isTimeout());
            Assert.assertTrue(e.getCause() instanceof KylinTimeoutException);
            Assert.assertEquals("KE-000000002", ((KylinTimeoutException) e.getCause()).getErrorCode().getCodeString());
            Assert.assertEquals("The query exceeds the set time limit of 300s. Current step: Realization chooser. ",
                    e.getCause().getMessage());
            Thread.interrupted();
        }
        slowQueryDetector.queryEnd();
    }
}
