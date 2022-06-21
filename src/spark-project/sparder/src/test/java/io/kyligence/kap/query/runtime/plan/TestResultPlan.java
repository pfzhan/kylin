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

package io.kyligence.kap.query.runtime.plan;

import io.kyligence.kap.common.state.StateSwitchConstant;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.state.QueryShareStateManager;
import io.kyligence.kap.query.MockContext;
import lombok.val;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.NewQueryRefuseException;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestResultPlan extends NLocalFileMetadataTestCase {

    private SlowQueryDetector slowQueryDetector = null;
    SparkSession ss;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        getTestConfig().setProperty("kylin.query.share-state-switch-implement", "jdbc");
        ss = SparkSession.builder().appName("local").master("local[1]")
                .getOrCreate();
        SparderEnv.setSparkSession(ss);
        StructType schema = new StructType();
        schema = schema.add("TRANS_ID", DataTypes.LongType, false);
        schema = schema.add("ORDER_ID", DataTypes.LongType, false);
        schema = schema.add("CAL_DT", DataTypes.DateType, false);
        schema = schema.add("LSTG_FORMAT_NAME", DataTypes.StringType, false);
        schema = schema.add("LEAF_CATEG_ID", DataTypes.LongType, false);
        schema = schema.add("LSTG_SITE_ID", DataTypes.IntegerType, false);
        schema = schema.add("SLR_SEGMENT_CD", DataTypes.FloatType, false);
        schema = schema.add("SELLER_ID", DataTypes.LongType, false);
        schema = schema.add("PRICE", DataTypes.createDecimalType(19, 4), false);
        schema = schema.add("ITEM_COUNT", DataTypes.DoubleType, false);
        schema = schema.add("TEST_COUNT_DISTINCT_BITMAP", DataTypes.StringType, false);
        ss.read().schema(schema).csv("../../examples/test_case_data/localmeta/data/DEFAULT.TEST_KYLIN_FACT.csv")
                .createOrReplaceTempView("TEST_KYLIN_FACT");
        slowQueryDetector = new SlowQueryDetector(100000, 5 * 1000);
        slowQueryDetector.start();
    }

    @After
    public void after() throws Exception {
        slowQueryDetector.interrupt();
        ss.stop();
        cleanupTestMetadata();
        SparderEnv.clean();
    }

    @Test
    public void testRefuseNewBigQuery() {
        QueryShareStateManager.getInstance().setState(Collections.singletonList(AddressUtil.concatInstanceName()),
                StateSwitchConstant.QUERY_LIMIT_STATE, "true");
        QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics()
                .addAccumSourceScanRows(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
        String sql = "select * from TEST_KYLIN_FACT";
        try {
            ResultPlan.getResult(ss.sql(sql), null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NewQueryRefuseException);
        }
    }

    @Test
    public void testCancelQuery() throws InterruptedException {
        AtomicReference<SparkListenerJobEnd> sparkJobEnd = new AtomicReference<>();
        CountDownLatch isJobEnd = new CountDownLatch(1);
        ss.sparkContext().addSparkListener(new SparkListener() {
            @Override
            public void onTaskStart(SparkListenerTaskStart taskStart) {
                for (SlowQueryDetector.QueryEntry e : SlowQueryDetector.getRunningQueries().values()) {
                    e.setStopByUser(true);
                    e.getThread().interrupt();
                    return;
                }
                Assert.fail("no running query is found");
            }

            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                sparkJobEnd.set(jobEnd);
                isJobEnd.countDown();
            }
        });
        Thread queryThread = new Thread(() -> {
            try {
                slowQueryDetector.queryStart("foo");
                QueryShareStateManager.getInstance().setState(
                        Collections.singletonList(AddressUtil.concatInstanceName()),
                        StateSwitchConstant.QUERY_LIMIT_STATE, "false");
                QueryContext.current().getMetrics()
                        .addAccumSourceScanRows(KapConfig.getInstanceFromEnv().getBigQuerySourceScanRowsThreshold() + 1);
                String sql = "select * from TEST_KYLIN_FACT";
                ResultPlan.getResult(ss.sql(sql), null);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof UserStopQueryException);
            } finally {
                slowQueryDetector.queryEnd();
            }
        });
        queryThread.start();
        queryThread.join();
        isJobEnd.await(10, TimeUnit.SECONDS);
        Assert.assertTrue(sparkJobEnd.get().jobResult() instanceof JobFailed);
        Assert.assertTrue(((JobFailed)sparkJobEnd.get().jobResult()).exception().getMessage().contains("cancelled part of cancelled job group"));
    }

    @Test
    public void testSetQueryFairSchedulerPool() {
        long fakeScanRows = 10000;
        int fakePartitionNum = 5;
        String pool;

        SparkConf sparkConf = new SparkConf();
        QueryContext queryContext = QueryContext.current();

        queryContext.getQueryTagInfo().setHighPriorityQuery(true);
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
        Assert.assertEquals("vip_tasks", pool);
        queryContext.getQueryTagInfo().setHighPriorityQuery(false);

        queryContext.getQueryTagInfo().setTableIndex(true);
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
        Assert.assertEquals("extreme_heavy_tasks", pool);
        queryContext.getQueryTagInfo().setTableIndex(false);

        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
        Assert.assertEquals("heavy_tasks", pool);
        fakePartitionNum = SparderEnv.getTotalCore() - 1;
        pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
        Assert.assertEquals("lightweight_tasks", pool);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.query-limit-enabled", "true");
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
            Assert.assertEquals("lightweight_tasks", pool);

            sparkConf.set("spark.dynamicAllocation.enabled", "true");
            sparkConf.set("spark.dynamicAllocation.maxExecutors", "1");
            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", String.valueOf(fakeScanRows + 1));
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
            Assert.assertEquals("lightweight_tasks", pool);

            config.setProperty("kylin.query.big-query-source-scan-rows-threshold", String.valueOf(fakeScanRows - 1));
            pool = ResultPlan.getQueryFairSchedulerPool(sparkConf, queryContext, fakeScanRows, fakePartitionNum);
            Assert.assertEquals("heavy_tasks", pool);
        }
    }

    @Test
    public void testAsyncQueryWriteParquet() {
        QueryContext queryContext = QueryContext.current();
        queryContext.getQueryTagInfo().setAsyncQuery(true);
        queryContext.getQueryTagInfo().setFileFormat("parquet");
        queryContext.getQueryTagInfo().setFileEncode("utf-8");
        String sql = "select * from TEST_KYLIN_FACT";
        val resultType = MockContext.current().getRelDataType();
        ResultPlan.getResult(ss.sql(sql), resultType);
    }

}
