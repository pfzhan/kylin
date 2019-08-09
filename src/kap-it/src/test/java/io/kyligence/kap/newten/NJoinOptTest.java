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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SortExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.exchange.Exchange;
import org.apache.spark.sql.execution.joins.SortMergeJoinExec;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import scala.Option;
import scala.runtime.AbstractFunction1;

public class NJoinOptTest extends NLocalWithSparkSessionTest {

    @BeforeClass
    public static void initSpark() {
        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
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

        System.out.println("Check spark sql config [spark.sql.catalogImplementation = "
                + ss.conf().get("spark.sql.catalogImplementation") + "]");
    }

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/join_opt");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
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
    }

    @Test
    public void testShardJoinInOneSeg() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "100");
        try {
            fullBuildCube("8c670664-8d05-466a-802f-83c023b56c77", getProject());
            populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
            // calcite will transform this "in" to join
            val sql1 = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
            val sql2 = "select count(*) from TEST_KYLIN_FACT where LSTG_FORMAT_NAME in (select LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME)";
            List<String> query = new ArrayList<>();
            query.add(sql1);
            query.add(sql2);
            NExecAndComp.execAndCompareQueryList(query, getProject(), NExecAndComp.CompareLevel.SAME, "default");

            basicScenario(sql1);
            testExchangePruningAfterAgg(sql2);
        } finally {
            System.clearProperty("kap.storage.columnar.shard-rowcount");
        }
    }

    private void testExchangePruningAfterAgg(String sql) throws SQLException {
        val plan = NExecAndComp.queryCube(getProject(), sql).queryExecution().executedPlan();
        val joinExec = (SortMergeJoinExec) findSpecPlan(plan, SortMergeJoinExec.class).get();
        // assert no exchange
        Assert.assertFalse(findSpecPlan(joinExec, Exchange.class).isDefined());

        // data after agg will lost its sorting characteristics
        Assert.assertTrue(findSpecPlan(joinExec, SortExec.class).isDefined());
    }

    private void basicScenario(String sql) throws SQLException {
        val plan = NExecAndComp.queryCube(getProject(), sql).queryExecution().executedPlan();
        val joinExec = (SortMergeJoinExec) findSpecPlan(plan, SortMergeJoinExec.class).get();
        // assert no exchange
        Assert.assertFalse(findSpecPlan(joinExec, Exchange.class).isDefined());

        // assert no sort
        Assert.assertFalse(findSpecPlan(joinExec, SortExec.class).isDefined());
    }

    @Test
    public void testShardJoinInMultiSeg() throws Exception {
        System.setProperty("kap.storage.columnar.shard-rowcount", "100");
        try {
            buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");
            populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
            // calcite will transform this "in" to join
            val sql = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
            List<String> query = new ArrayList<>();
            query.add(sql);
            NExecAndComp.execAndCompareQueryList(query, getProject(), NExecAndComp.CompareLevel.SAME, "default");

            val plan = NExecAndComp.queryCube(getProject(), sql).queryExecution().executedPlan();
            val joinExec = (SortMergeJoinExec) findSpecPlan(plan, SortMergeJoinExec.class).get();
            // assert exists exchange
            Assert.assertTrue(findSpecPlan(joinExec, Exchange.class).isDefined());

            // assert exists sort
            Assert.assertTrue(findSpecPlan(joinExec, SortExec.class).isDefined());
        } finally {
            System.clearProperty("kap.storage.columnar.shard-rowcount");
        }
    }

    @Test
    public void testShardJoinInMultiSegWithFixedShardNum() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);

        Map<String, String> overrideKylinProps = new HashMap<>();
        overrideKylinProps.put("kylin.engine.shard-num-json", "{\"DEFAULT.TEST_KYLIN_FACT.SELLER_ID\":\"10\",\"c\":\"200\",\"e\":\"300\"}");
        projectManager.updateProject(getProject(), copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });

        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        // calcite will transform this "in" to join
        val sql = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
        List<String> query = new ArrayList<>();
        query.add(sql);
        NExecAndComp.execAndCompareQueryList(query, getProject(), NExecAndComp.CompareLevel.SAME, "default");

        val plan = NExecAndComp.queryCube(getProject(), sql).queryExecution().executedPlan();
        val joinExec = (SortMergeJoinExec) findSpecPlan(plan, SortMergeJoinExec.class).get();
        // assert no exchange, cuz we unified the num of shards in different segments.
        Assert.assertFalse(findSpecPlan(joinExec, Exchange.class).isDefined());

        // assert exists sort
        Assert.assertTrue(findSpecPlan(joinExec, SortExec.class).isDefined());
    }

    private Option<SparkPlan> findSpecPlan(SparkPlan plan, Class<?> cls) {
        return plan.find(new AbstractFunction1<SparkPlan, Object>() {
            @Override
            public Object apply(SparkPlan v1) {
                return cls.isInstance(v1);
            }
        });
    }

    @Override
    public String getProject() {
        return "join_opt";
    }
}
