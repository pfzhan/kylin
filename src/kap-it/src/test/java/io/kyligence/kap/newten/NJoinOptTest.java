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
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;
import scala.Option;
import scala.runtime.AbstractFunction1;

public class NJoinOptTest extends NLocalWithSparkSessionTest {

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

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Ignore("KE-30387")
    @Test
    public void testShardJoinInOneSeg() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");
        fullBuild("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        // calcite will transform this "in" to join
        val sql1 = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
        val sql2 = "select count(*) from TEST_KYLIN_FACT where LSTG_FORMAT_NAME in (select LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME)";
        val sql3 = "select count(*) from TEST_KYLIN_FACT t1 join "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) t2 "
                + "on t1.TRANS_ID = t2.TRANS_ID and t1.LSTG_FORMAT_NAME = t2.LSTG_FORMAT_NAME";
        List<String> query = new ArrayList<>();
        query.add(sql1);
        query.add(sql2);
        query.add(sql3);
        ExecAndComp.execAndCompareQueryList(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");

        basicScenario(sql1);
        testExchangePruningAfterAgg(sql2);
        testMultiShards(sql3);
    }

    private void testMultiShards(String sql) throws SQLException {
        // assert no exchange
        // assert no sort
        assertPlan(sql, false, false);
    }

    private void testExchangePruningAfterAgg(String sql) throws SQLException {
        // assert no exchange
        // data after agg will lost its sorting characteristics
        assertPlan(sql, false, true);
    }

    private void basicScenario(String sql) throws SQLException {
        // assert no exchange
        // assert no sort
        assertPlan(sql, false, false);
    }

    @Test
    public void testShardJoinInMultiSeg() throws Exception {
        overwriteSystemProp("kylin.storage.columnar.shard-rowcount", "100");
        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        // calcite will transform this "in" to join
        val sql = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
        List<String> query = new ArrayList<>();
        query.add(sql);
        ExecAndComp.execAndCompareQueryList(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");

        // assert exists exchange
        // assert exists sort
        assertPlan(sql, true, true);
    }

    @Ignore("KE-30387")
    @Test
    public void testShardJoinInMultiSegWithFixedShardNum() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);

        Map<String, String> overrideKylinProps = new HashMap<>();
        overrideKylinProps.put("kylin.engine.shard-num-json",
                "{\"DEFAULT.TEST_KYLIN_FACT.SELLER_ID\":\"10\",\"DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME,DEFAULT.TEST_KYLIN_FACT.TRANS_ID\":\"15\",\"e\":\"300\"}");
        projectManager.updateProject(getProject(), copyForWrite -> {
            copyForWrite.getOverrideKylinProps().putAll(overrideKylinProps);
        });

        buildMultiSegs("8c670664-8d05-466a-802f-83c023b56c77");
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        // calcite will transform this "in" to join
        val sql1 = "select count(*) from TEST_KYLIN_FACT where SELLER_ID in (select SELLER_ID from TEST_KYLIN_FACT group by SELLER_ID)";
        val sql2 = "select count(*) from TEST_KYLIN_FACT t1 join "
                + "(select TRANS_ID,LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by TRANS_ID,LSTG_FORMAT_NAME) t2 "
                + "on t1.TRANS_ID = t2.TRANS_ID and t1.LSTG_FORMAT_NAME = t2.LSTG_FORMAT_NAME";

        List<String> query = new ArrayList<>();
        query.add(sql1);
        query.add(sql2);
        ExecAndComp.execAndCompareQueryList(query, getProject(), ExecAndComp.CompareLevel.SAME, "default");

        // assert no exchange, cuz we unified the num of shards in different segments.
        // assert exists sort
        assertPlan(sql1, false, true);
        assertPlan(sql2, false, true);
    }

    private void assertPlan(String sql, boolean existsExchange, boolean existsSort) throws SQLException {
        SortMergeJoinExec joinExec = getSortMergeJoinExec(sql);
        Assert.assertEquals(existsExchange, findSpecPlan(joinExec, Exchange.class).isDefined());

        Assert.assertEquals(existsSort, findSpecPlan(joinExec, SortExec.class).isDefined());
    }

    private SortMergeJoinExec getSortMergeJoinExec(String sql) throws SQLException {
        val plan = ExecAndComp.queryModel(getProject(), sql).queryExecution().executedPlan();
        return (SortMergeJoinExec) findSpecPlan(plan, SortMergeJoinExec.class).get();
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
