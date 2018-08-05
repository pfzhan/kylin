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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.SparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.spark.KapSparkSession;

@SuppressWarnings("serial")
public class NAutoBuildAndQueryTest extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NAutoBuildAndQueryTest.class);
    private static KylinConfig kylinConfig;
    private static final String DEFAULT_PROJECT = "newten";
    private static final String IT_SQL_KYLIN_DIR = "../../kylin/kylin-it/src/test/resources/query";
    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/query";

    private static String JOIN_TYPE = "default";

    @Before
    public void setup() throws Exception {
        super.init();

        kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.storage.provider.0", "io.kyligence.kap.storage.NDataStorage");
        kylinConfig.setProperty("kap.storage.columnar.hdfs-dir", kylinConfig.getHdfsWorkingDirectory() + "/parquet/");
        kylinConfig.setProperty("kap.smart.conf.model.inner-join.exactly-match", "true");
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");

        Candidate.restorePriorities();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    @Ignore
    @Test
    public void test_auto_build_basic() throws Exception{
        testAutoSingleModel(kylinConfig);
        testAutoMultipleModel(kylinConfig);
    }

    public void testAutoSingleModel(KylinConfig kylinConfig) throws Exception {

        //1. create single segement
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        List<NModelContext> modelContextsPart1 =
                build(kapSparkSession, kylinConfig, "auto/sql", 0, 1);
        validate(modelContextsPart1, 1, 1);

        List<NModelContext> modelContextsPart2 =
                build(kapSparkSession, kylinConfig, "auto/sql", 1, 2);
        validate(modelContextsPart2, 1, 2);
        Assert.assertEquals(modelContextsPart1.get(0).getTargetModel().getName(),
                modelContextsPart2.get(0).getTargetModel().getName());
        kapSparkSession.close();

        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        // 3. Auto suggested model is able to serve related query
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 3, "default");
            kapSparkSession.use(DEFAULT_PROJECT);
            populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
        }

        // 4. Feed bad queries
        {
            List<NModelContext> modelContexts = build(kapSparkSession, kylinConfig, "auto/sql_bad", 0, 0);
            Assert.assertEquals(modelContexts.size(), 0);
        }

        // 5. Feed query with inner join same fact table, should create another model
        {
            List<NModelContext> modelContexts = build(kapSparkSession, kylinConfig, "auto/sql", 3, 4);
            validate(modelContexts, 1, 2);
        }

        kapSparkSession.close();

        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        // 6. Final run of all queries
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", true, 0, 4, "default");
            kapSparkSession.use(DEFAULT_PROJECT);
            populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
        }

        kapSparkSession.close();
    }

    private List<NModelContext> build(KapSparkSession kapSparkSession, KylinConfig kylinConfig, String subFolder, int start, int end) throws Exception{
        List<Pair<String, String>> queries = fetchPartialQueries(subFolder, true, start, end, "default");
        List<String> sqlList = new ArrayList<>();
        for (Pair<String, String> queryPair : queries) {
            sqlList.add(queryPair.getSecond());
        }

        NSmartMaster master = new NSmartMaster(kylinConfig, DEFAULT_PROJECT, sqlList.toArray(new String[0]));
        master.runAll();
        kapSparkSession.use(DEFAULT_PROJECT);
        kapSparkSession.buildAllCubes(kylinConfig, DEFAULT_PROJECT);

        return master.getContext().getModelContexts();
    }

    private void validate(List<NModelContext> modelContexts, int exceptedSize, int exceptedTableSize){
        Assert.assertEquals(modelContexts.size(), exceptedSize);
        NModelContext modelContext = modelContexts.get(0);
        NDataModel dataModel = modelContext.getTargetModel();
        Assert.assertNotNull(dataModel);
        Assert.assertEquals(dataModel.getAllTables().size(), exceptedTableSize);
        NCubePlan cubePlan = modelContext.getTargetCubePlan();
        Assert.assertNotNull(cubePlan);
    }

    public void testAutoMultipleModel(KylinConfig kylinConfig) throws Exception {

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        Map<String, NCubePlan> cubePlanOfParts = new HashMap<>();
        Map<String, NCubePlan> cubePlanOfAll = new HashMap<>();

        Map<String, String> buildMap = new HashMap<String, String>();
        buildMap.put("part", "0,2");
        buildMap.put("part", "2,4");
        buildMap.put("all", "0,4");
        for(Map.Entry<String, String> entry: buildMap.entrySet()){
            String type = entry.getKey();
            String during = entry.getValue();
            int start = Integer.valueOf(during.split(",")[0]);
            int end = Integer.valueOf(during.split(",")[1]);
            List<NModelContext> modelContexts = build(kapSparkSession, kylinConfig, "auto/sql", start, end);
            for (NModelContext nModelContext : modelContexts) {
                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
                if("part".equals(type)){
                    cubePlanOfParts.put(cubePlan.getId(), cubePlan);
                }
                if("all".equals(type)){
                    cubePlanOfAll.put(cubePlan.getId(), cubePlan);
                }
            }
        }
        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
        {
            Assert.assertEquals(cubePlanOfParts.size(), cubePlanOfAll.size());
            for (NCubePlan actual : cubePlanOfAll.values()) {
                NCubePlan expected = cubePlanOfParts.get(actual.getId());
                Assert.assertNotNull(expected);
                // compare cuboids
                Assert.assertEquals(expected.getCuboids().size(), actual.getCuboids().size());
                Assert.assertEquals(expected.getAllCuboidLayouts().size(), actual.getAllCuboidLayouts().size());
                for (NCuboidDesc actualCuboid : actual.getCuboids()) {
                    NCuboidDesc expectedCuboid = expected.getCuboidDesc(actualCuboid.getId());
                    Assert.assertArrayEquals(expectedCuboid.getDimensions(), actualCuboid.getDimensions());
                    Assert.assertArrayEquals(expectedCuboid.getMeasures(), actualCuboid.getMeasures());
                }
            }
        }

        kapSparkSession.close();
    }

    /***************
     * Test Kylin test queries with auto modeling
     */
    @Test
    public void test_query_basic() throws Exception{
        String[] sameSql = new String[]{ "sql", "sql_lookup", "sql_casewhen", "sql_like", "sql_cache", "sql_derived", "sql_subquery",
                "sql_datetime", "sql_distinct", "sql_multi_model", "sql_orderby", "sql_snowflake", "sql_union", "sql_grouping", "sql_hive", "sql_raw"};
        String[] rowcountSql = new String[]{ "sql_tableau" };
        String[] noneSql = new String[]{ "sql_window", "sql_h2_uncapable", "sql_percentile" };
        String[] leftJoinSql = new String[]{ "sql_distinct_precisely", "sql_topn" };
        String[] noneAndLeftSql = new String[]{ "sql_intersect_count" };
        String[] specialSql = new String[]{ "sql_distinct_dim", "sql_rawtable"};

        CustomThreadPoolExecutor service = new CustomThreadPoolExecutor();
        int countdownNum = sameSql.length + rowcountSql.length + leftJoinSql.length +
                noneSql.length + noneAndLeftSql.length + specialSql.length;

        CountDownLatch latch = new CountDownLatch(countdownNum);
        Map<CompareLevel, String[]> queriesMap = new HashMap<CompareLevel, String[]>();
        queriesMap.put(CompareLevel.SAME, sameSql);
        queriesMap.put(CompareLevel.SAME_ROWCOUNT, rowcountSql);
        queriesMap.put(CompareLevel.NONE, noneSql);
        for (Map.Entry<CompareLevel, String[]> entry: queriesMap.entrySet()){
            CompareLevel compareLevel = entry.getKey();
            String[] values = entry.getValue();
            for (String value: values){
                Runnable runnable = new QueryRunnable(value, false, 0 ,
                        0, JOIN_TYPE, compareLevel, latch);
                service.submit(runnable);
            }
        }
        for(String value: leftJoinSql){
            Runnable runnable = new QueryRunnable(value, false, 0 , 0, "left", CompareLevel.SAME, latch);
            service.submit(runnable);
        }
        for(String value: noneAndLeftSql){
            Runnable runnable = new QueryRunnable(value, false, 0 , 0, "left", CompareLevel.NONE, latch);
            service.submit(runnable);
        }
        Runnable runnable = new QueryRunnable("sql_distinct_dim", false, 0, 2, JOIN_TYPE, CompareLevel.SAME, latch);
        service.submit(runnable);
        Runnable runnable1 = new QueryRunnable("sql_rawtable", true, 0, 0, JOIN_TYPE, CompareLevel.SAME, latch);
        service.submit(runnable1);

        try {
            latch.await();
        } finally {
            service.shutdown();
        }
        
        if (!service.reportError())
            Assert.fail();
    }

    class CustomThreadPoolExecutor extends ThreadPoolExecutor {
        private List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());

        public CustomThreadPoolExecutor() {
            super(9, 9, 1, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(100));
        }

        public boolean reportError() {
            if (exceptions.isEmpty())
                return true;
            
            logger.error("There were exceptions in CustomThreadPoolExecutor");
            for (Throwable ex : exceptions) {
                logger.error("", ex);
            }
            return false;
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            
            if (t != null) {
                exceptions.add(t);
            }
        }
    }

    class QueryRunnable implements Runnable{

        private String subFolder;
        private boolean isKapTest;
        private int start;
        private int end;
        private String joinType;
        private CompareLevel compareLevel;
        private CountDownLatch latch;

        public QueryRunnable(String subFolder, boolean isKapTest, int start, int end,
                             String joinType, CompareLevel compareLevel, CountDownLatch latch){
            this.subFolder = subFolder;
            this.isKapTest = isKapTest;
            this.start = start;
            this.end = end;
            this.joinType = joinType;
            this.compareLevel = compareLevel;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                List<Pair<String, String>> queries = fetchPartialQueries(subFolder, isKapTest, start, end, joinType);
                buildCube(queries);
                KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
                kapSparkSession.use(DEFAULT_PROJECT);
                populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
                NExecAndComp.execAndCompare(queries, kapSparkSession, compareLevel, JOIN_TYPE);
                kapSparkSession.close();
            } catch (Exception e) {
                throw new RuntimeException("query error in sub-folder: " + subFolder, e);
            } finally {
                latch.countDown();
            }
        }
    }

    private void buildCube(List<Pair<String, String>> queries) throws Exception {
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        for (Pair<String, String> query : queries) {
            kapSparkSession.collectQueries(query.getSecond());
        }
        kapSparkSession.speedUp();
        kapSparkSession.close();
    }

    private List<Pair<String, String>> fetchPartialQueries(String subFolder, boolean isKapTest, int start, int end,
                                                           String joinType) throws IOException {
        String folder = (isKapTest ? IT_SQL_KAP_DIR : IT_SQL_KYLIN_DIR) + File.separator + subFolder;
        List<Pair<String, String>> partials = start < end ? NExecAndComp.fetchPartialQueries(folder, start, end)
                : NExecAndComp.fetchQueries(folder);
        for (Pair<String, String> pair : partials) {
            String sql = pair.getSecond();
            pair.setSecond(NExecAndComp.changeJoinType(sql, joinType));
        }
        return partials;
    }

    /**
     * test which can't pass
     */
    /*
    @Ignore("auto model will suggest lookup table as fact, not fit in this case")
    @Test
    public void testLookupQuery() throws Exception {
        new TestScenario("sql_lookup", CompareLevel.SAME).execute();
    }

    @Ignore("no query found")
    @Test
    public void testTimestamp() throws Exception {
        new TestScenario("sql_timestamp", CompareLevel.NONE).execute();
    }

    @Ignore("not storage query, skip")
    @Test
    public void testTableauProbing() throws Exception {
        new TestScenario("tableau_probing", CompareLevel.NONE).execute();
    }

    @Ignore("Fail at (SortedIteratorMergerWithLimit.java:132), conflict with SortedIteratorMergerWithLimitTest")
    @Test
    public void testLimitCorrectness() throws Exception {
        List<Pair<String, String>> queries = fetchPartialQueries("sql", false, 0, 0, JOIN_TYPE);
        buildCube(queries);
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(DEFAULT_PROJECT);
        populateSSWithCSVData(kylinConfig, DEFAULT_PROJECT, kapSparkSession);
        NExecAndComp.execLimitAndValidate(queries, kapSparkSession, JOIN_TYPE);
    }
*/

    /****************
     * Following cased are not in Newten M1 scope
     */
    /*
    private static String[] sqlNotIncludeInNewtenM1 = new String[]{
            "sql_dynamic", "sql_extended_column", "sql_invalid", "sql_limit", "sql_massin",
            "sql_massin_distinct", "sql_streaming", "sql_timeout", "sql_verifyContent", "sql_verifyCount",
            "sql_current_date", "sql_percentile"
    };

    @Ignore
    @Test
    public void testDynamic() throws Exception {
        new TestScenario("sql_dynamic", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testExtendedColumn() throws Exception {
        new TestScenario("sql_extended_column", CompareLevel.SAME).execute();
    }

    @Ignore
    @Test
    public void testInvalid() throws Exception {
        new TestScenario("sql_invalid", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testLimit() throws Exception {
        new TestScenario("sql_limit", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testMassin() throws Exception {
        new TestScenario("sql_massin", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testMassinDistinct() throws Exception {
        new TestScenario("sql_massin_distinct", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testStreaming() throws Exception {
        new TestScenario("sql_streaming", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testTimeout() throws Exception {
        new TestScenario("sql_timeout", CompareLevel.NONE).execute();
    }

    @Ignore
    @Test
    public void testVerifyContent() throws Exception {
        new TestScenario("sql_verifyContent", CompareLevel.SAME).execute();
    }

    @Ignore
    @Test
    public void testVerifyCount() throws Exception {
        new TestScenario("sql_verifyCount", CompareLevel.SAME_ROWCOUNT).execute();
    }

    @Ignore
    @Test
    public void testTimeStampAdd() throws Exception {
        new TestScenario("sql_current_date", CompareLevel.SAME, true).execute();
    }

    @Ignore
    @Test
    public void testPercentileQuery() throws Exception {
        new TestScenario("sql_percentile", CompareLevel.SAME, false).execute();
    }
    */
}
