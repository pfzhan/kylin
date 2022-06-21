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

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.query.runtime.plan.ResultPlan;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ExtractLimitInfoTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        if (ss != null && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
        sparkConf = new SparkConf().setAppName(UUID.randomUUID().toString()).setMaster("local[4]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set("spark.sql.adaptive.enabled", "false");
        sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "1");
        ss = SparkSession.builder().config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/join_opt");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
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

    @Test
    public void testExtractLimitInfo() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");
        fullBuild("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        SparkPlan sparkPlan;
        AtomicLong accumRowsCounter;

        val sql1 = "select * from "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) lside left join "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) rside "
                + "on lside.TRANS_ID = rside.TRANS_ID limit 10";
        sparkPlan = getSparkExecutedPlan(sql1);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10010, accumRowsCounter.get());

        val sql2 = "select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME limit 10";
        sparkPlan = getSparkExecutedPlan(sql2);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10, accumRowsCounter.get());

        val sql3 = "select count(*) from (select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME limit 10)";
        sparkPlan = getSparkExecutedPlan(sql3);
        accumRowsCounter = new AtomicLong();
        ResultPlan.extractEachStageLimitRows(sparkPlan, -1, accumRowsCounter);
        Assert.assertEquals(10, accumRowsCounter.get());
    }

    private SparkPlan getSparkExecutedPlan(String sql) throws SQLException {
        return ExecAndComp.queryModel(getProject(), sql).queryExecution().executedPlan();
    }

    @Override
    public String getProject() {
        return "join_opt";
    }

}
