///*
// * Copyright (C) 2016 Kyligence Inc. All rights reserved.
// *
// * http://kyligence.io
// *
// * This software is the confidential and proprietary information of
// * Kyligence Inc. ("Confidential Information"). You shall not disclose
// * such Confidential Information and shall use it only in accordance
// * with the terms of the license agreement you entered into with
// * Kyligence Inc.
// *
// * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//
//package io.kyligence.kap.newten;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.kylin.common.KylinConfig;
//import org.apache.kylin.common.util.Pair;
//import org.apache.kylin.job.engine.JobEngineConfig;
//import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
//import org.apache.kylin.job.lock.MockJobLock;
//import org.apache.kylin.query.routing.Candidate;
//import org.apache.spark.SparkContext;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import io.kyligence.kap.cube.model.NCubePlan;
//import io.kyligence.kap.cube.model.NCuboidDesc;
//import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
//import io.kyligence.kap.metadata.model.NDataModel;
//import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
//import io.kyligence.kap.smart.NSmartContext.NModelContext;
//import io.kyligence.kap.smart.NSmartMaster;
//import io.kyligence.kap.spark.KapSparkSession;
//
//public class NAutoBuildAndQueryTest extends NLocalWithSparkSessionTest {
//
//    private static final Logger logger = LoggerFactory.getLogger(NAutoBuildAndQueryTest.class);
//    private static KylinConfig kylinConfig;
//    private static KapSparkSession kapSparkSession;
//    private static final String DEFAULT_PROJECT = "newten";
//    private static final String IT_SQL_KYLIN_DIR = "../../kylin/kylin-it/src/test/resources/query";
//    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/query";
//
//    private static String JOIN_TYPE = "default";
//
//    @Before
//    public void setup() throws Exception {
//        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
//        super.setUp();
//        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
//        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
//        if (!scheduler.hasStarted()) {
//            throw new RuntimeException("scheduler has not been started");
//        }
//        kylinConfig = getTestConfig();
//        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
//        kylinConfig.setProperty("kap.storage.columnar.hdfs-dir", kylinConfig.getHdfsWorkingDirectory() + "/parquet/");
//    }
//
//    @After
//    public void after() throws Exception {
//        Candidate.restorePriorities();
//
//        if (kapSparkSession != null)
//            kapSparkSession.close();
//
//        NDefaultScheduler.destroyInstance();
//        super.tearDown();
//        System.clearProperty("kylin.job.scheduler.poll-interval-second");
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testAutoSingleModel() throws Exception {
//
//        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//
//        // 1. Create simple model with one fact table
//        String targetModelName;
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 1, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            Assert.assertEquals(modelContexts.size(), 1);
//            NModelContext modelContext = modelContexts.get(0);
//            NDataModel dataModel = modelContext.getTargetModel();
//            Assert.assertNotNull(dataModel);
//            targetModelName = dataModel.getName();
//            Assert.assertEquals(dataModel.getAllTables().size(), 1);
//            NCubePlan cubePlan = modelContext.getTargetCubePlan();
//            Assert.assertNotNull(cubePlan);
//        }
//
//        // 2. Feed query with left join same fact table, should update same model
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 1, 2, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            Assert.assertEquals(modelContexts.size(), 1);
//            NModelContext modelContext = modelContexts.get(0);
//            NDataModel dataModel = modelContext.getTargetModel();
//            Assert.assertNotNull(dataModel);
//            Assert.assertEquals(targetModelName, dataModel.getName());
//            Assert.assertEquals(dataModel.getAllTables().size(), 2);
//            NCubePlan cubePlan = modelContext.getTargetCubePlan();
//            Assert.assertNotNull(cubePlan);
//        }
//
//        kapSparkSession.close();
//
//        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//
//        // 3. Auto suggested model is able to serve related query
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 3, "default");
//            kapSparkSession.use(DEFAULT_PROJECT);
//            populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
//            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
//        }
//
//        // 4. Feed bad queries
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql_bad", true, 0, 0, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            Assert.assertEquals(modelContexts.size(), 0);
//        }
//
//        // 5. Feed query with inner join same fact table, should create another model
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 3, 4, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            Assert.assertEquals(modelContexts.size(), 1);
//            NModelContext modelContext = modelContexts.get(0);
//            NDataModel dataModel = modelContext.getTargetModel();
//            Assert.assertNotNull(dataModel);
//            Assert.assertNotEquals(targetModelName, dataModel.getName());
//            Assert.assertEquals(dataModel.getAllTables().size(), 2);
//            NCubePlan cubePlan = modelContext.getTargetCubePlan();
//            Assert.assertNotNull(cubePlan);
//        }
//
//        kapSparkSession.close();
//
//        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//
//        // 6. Final run of all queries
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 4, "default");
//            kapSparkSession.use(DEFAULT_PROJECT);
//            populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
//            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
//            kapSparkSession.close();
//        }
//
//        kapSparkSession.close();
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testAutoMultipleModel() throws Exception {
//
//        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//
//        Map<String, NCubePlan> cubePlanOfParts = new HashMap<>();
//        Map<String, NCubePlan> cubePlanOfAll = new HashMap<>();
//
//        // 1. Feed queries part1
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 2, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            for (NModelContext nModelContext : modelContexts) {
//                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
//                cubePlanOfParts.put(cubePlan.getId(), cubePlan);
//            }
//        }
//
//        // 2. Feed queried part2
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 2, 4, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            for (NModelContext nModelContext : modelContexts) {
//                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
//                cubePlanOfParts.put(cubePlan.getId(), cubePlan);
//            }
//        }
//
//        // 3. Retry all queries
//        {
//            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 4, "default");
//            List<String> sqlList = new ArrayList<>();
//            for (Pair<String, String> queryPair : queries) {
//                sqlList.add(queryPair.getSecond());
//            }
//
//            NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
//            master.runAll();
//            kapSparkSession.use(DEFAULT_PROJECT);
//            kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);
//
//            List<NModelContext> modelContexts = master.getContext().getModelContexts();
//            for (NModelContext nModelContext : modelContexts) {
//                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
//                cubePlanOfAll.put(cubePlan.getId(), cubePlan);
//            }
//        }
//
//        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
//        {
//            Assert.assertEquals(cubePlanOfParts.size(), cubePlanOfAll.size());
//            for (NCubePlan actual : cubePlanOfAll.values()) {
//                NCubePlan expected = cubePlanOfParts.get(actual.getId());
//                Assert.assertNotNull(expected);
//                // compare cuboids
//                Assert.assertEquals(expected.getCuboids().size(), actual.getCuboids().size());
//                Assert.assertEquals(expected.getAllCuboidLayouts().size(), actual.getAllCuboidLayouts().size());
//                for (NCuboidDesc actualCuboid : actual.getCuboids()) {
//                    NCuboidDesc expectedCuboid = expected.getCuboidDesc(actualCuboid.getId());
//                    Assert.assertArrayEquals(expectedCuboid.getDimensions(), actualCuboid.getDimensions());
//                    Assert.assertArrayEquals(expectedCuboid.getMeasures(), actualCuboid.getMeasures());
//                }
//            }
//        }
//
//        kapSparkSession.close();
//    }
//
//    /***************
//     * Test Kylin test queries with auto modeling
//     */
//
//    @Ignore("passed")
//    @Test
//    public void testCommonQuery() throws Exception {
//        testScenario("sql", CompareLevel.SAME);
//    }
//
//    @Ignore("auto model will suggest lookup table as fact, not fit in this case")
//    @Test
//    public void testLookupQuery() throws Exception {
//        testScenario("sql_lookup", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testCasewhen() throws Exception {
//        testScenario("sql_casewhen", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testLikeQuery() throws Exception {
//        testScenario("sql_like", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testCachedQuery() throws Exception {
//        testScenario("sql_cache", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testDerived() throws Exception {
//        testScenario("sql_derived", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testDatetime() throws Exception {
//        testScenario("sql_datetime", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    // different results due to limited disorder, just check result size
//    @Test
//    public void testTableau() throws Exception {
//        testScenario("sql_tableau", CompareLevel.SAME_ROWCOUNT);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testDistinct() throws Exception {
//        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
//        testScenario("sql_distinct", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testDistinctDim() throws Exception {
//        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
//        testScenario("sql_distinct_dim", CompareLevel.SAME, 0, 2);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testDistinctPrecisely() throws Exception {
//        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
//        testScenario("sql_distinct_precisely", CompareLevel.SAME, "left");
//    }
//
//    @Ignore("no query found")
//    @Test
//    public void testTimestamp() throws Exception {
//        testScenario("sql_timestamp", CompareLevel.NONE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testMultiModel() throws Exception {
//        testScenario("sql_multi_model", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testOrderBy() throws Exception {
//        testScenario("sql_orderby", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testSnowFlake() throws Exception {
//        testScenario("sql_snowflake", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testTopn() throws Exception {
//        testScenario("sql_topn", CompareLevel.SAME, "left");
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testJoin() throws Exception {
//        testScenario("sql_join", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testUnion() throws Exception {
//        testScenario("sql_union", CompareLevel.SAME);
//    }
//
//    @Ignore("not storage query, skip")
//    @Test
//    public void testTableauProbing() throws Exception {
//        testScenario("tableau_probing", CompareLevel.NONE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testWindow() throws Exception {
//        testScenario("sql_window", CompareLevel.NONE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testH2Uncapable() throws Exception {
//        testScenario("sql_h2_uncapable", CompareLevel.NONE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testGrouping() throws Exception {
//        testScenario("sql_grouping", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testHive() throws Exception {
//        testScenario("sql_hive", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testIntersectCount() throws Exception {
//        testScenario("sql_intersect_count", CompareLevel.NONE, "left");
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testPercentile() throws Exception {
//        testScenario("sql_percentile", CompareLevel.NONE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testPowerBiQuery() throws Exception {
//        testScenario("sql_powerbi", CompareLevel.SAME, true);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testLimitCorrectness() throws Exception {
//        List<Pair<String, String>> queries = fetchPartialQueries("sql", false, 0, 0, JOIN_TYPE);
//        buildCube(queries);
//        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//        kapSparkSession.use(DEFAULT_PROJECT);
//        populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
//        NExecAndComp.execLimitAndValidate(queries, kapSparkSession, JOIN_TYPE);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testRaw() throws Exception {
//        testScenario("sql_raw", CompareLevel.SAME);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testRawTable() throws Exception {
//        testScenario("sql_rawtable", CompareLevel.SAME, true);
//    }
//
//    @Ignore("passed")
//    @Test
//    public void testSubQuery() throws Exception {
//        //System.setProperty("calcite.debug", "true");
//        testScenario("sql_subquery", CompareLevel.SAME);
//    }
//
//    /****************
//     * Following cased are not in Newten M1 scope
//     */
//
//    @Ignore
//    @Test
//    public void testDynamic() throws Exception {
//        testScenario("sql_dynamic", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testExtendedColumn() throws Exception {
//        testScenario("sql_extended_column", CompareLevel.SAME);
//    }
//
//    @Ignore
//    @Test
//    public void testInvalid() throws Exception {
//        testScenario("sql_invalid", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testLimit() throws Exception {
//        testScenario("sql_limit", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testMassin() throws Exception {
//        testScenario("sql_massin", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testMassinDistinct() throws Exception {
//        testScenario("sql_massin_distinct", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testStreaming() throws Exception {
//        testScenario("sql_streaming", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testTimeout() throws Exception {
//        testScenario("sql_timeout", CompareLevel.NONE);
//    }
//
//    @Ignore
//    @Test
//    public void testVerifyContent() throws Exception {
//        testScenario("sql_verifyContent", CompareLevel.SAME);
//    }
//
//    @Ignore
//    @Test
//    public void testVerifyCount() throws Exception {
//        testScenario("sql_verifyCount", CompareLevel.SAME_ROWCOUNT);
//    }
//
//    @Ignore
//    @Test
//    public void testTimeStampAdd() throws Exception {
//        testScenario("sql_current_date", CompareLevel.SAME, true);
//    }
//
//    @Ignore
//    @Test
//    public void testPercentileQuery() throws Exception {
//        testScenario("sql_percentile", CompareLevel.SAME, false);
//    }
//
//    private void testScenario(String name, CompareLevel compareLevel) throws Exception {
//        testScenario(name, compareLevel, false, 0, 0, "default");
//    }
//
//    private void testScenario(String name, CompareLevel compareLevel, boolean isKapTest) throws Exception {
//        testScenario(name, compareLevel, isKapTest, 0, 0, "default");
//    }
//
//    @SuppressWarnings("unused")
//    private void testScenario(String name, CompareLevel compareLevel, int start, int end) throws Exception {
//        testScenario(name, compareLevel, false, start, end, "default");
//    }
//
//    private void testScenario(String name, CompareLevel compareLevel, String joinType) throws Exception {
//        testScenario(name, compareLevel, false, 0, 0, joinType);
//    }
//
//    private void testScenario(String name, CompareLevel compareLevel, boolean isKapTest, int start, int end,
//            String joinType) throws Exception {
//
//        List<Pair<String, String>> queries = fetchPartialQueries(name, isKapTest, start, end, joinType);
//        buildCube(queries);
//
//        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//        kapSparkSession.use(DEFAULT_PROJECT);
//
//        // Validate results between sparksql and cube
//        populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
//        NExecAndComp.execAndCompare(queries, kapSparkSession, compareLevel, joinType);
//        kapSparkSession.close();
//    }
//
//    private void buildCube(List<Pair<String, String>> queries) throws Exception {
//        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
//        kapSparkSession.use(DEFAULT_PROJECT);
//        for (Pair<String, String> query : queries) {
//            kapSparkSession.collectQueries(query.getSecond());
//        }
//        kapSparkSession.speedUp();
//        kapSparkSession.close();
//    }
//
//    private List<Pair<String, String>> fetchPartialQueries(String subFolder, boolean isKapTest, int start, int end,
//            String joinType) throws IOException {
//        String folder = (isKapTest ? IT_SQL_KAP_DIR : IT_SQL_KYLIN_DIR) + File.separator + subFolder;
//        List<Pair<String, String>> partials = start < end ? NExecAndComp.fetchPartialQueries(folder, start, end)
//                : NExecAndComp.fetchQueries(folder);
//        for (Pair<String, String> pair : partials) {
//            String sql = pair.getSecond();
//            pair.setSecond(NExecAndComp.changeJoinType(sql, joinType));
//        }
//        return partials;
//    }
//}
