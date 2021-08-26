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

package io.kyligence.kap.query.pushdown;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.exception.UserStopQueryException;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class PushdownJobCancelTest extends NLocalFileMetadataTestCase {

    private SlowQueryDetector slowQueryDetector = null;
    SparkSession ss;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
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
    public void testCancelPushdownJob() throws InterruptedException {
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
                String sql = "select * from TEST_KYLIN_FACT";
                SparkSqlClient.executeSql(ss, sql, RandomUtil.randomUUID(), "tpch");
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

}
