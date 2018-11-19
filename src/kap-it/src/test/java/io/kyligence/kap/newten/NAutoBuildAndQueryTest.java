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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkContext;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.spark.KapSparkSession;

@SuppressWarnings("serial")
public class NAutoBuildAndQueryTest extends NAutoTestBase {

    private static String JOIN_TYPE = "default";

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testAutoSingleModel() throws Exception {

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        // 1. Create simple model with one fact table
        String targetModelName;
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 0, 1, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            targetModelName = dataModel.getName();
            Assert.assertEquals(1, dataModel.getAllTables().size());
            NCubePlan cubePlan = modelContext.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
        }

        // 2. Feed query with left join using same fact table, should update same model
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 1, 2, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(targetModelName, dataModel.getName());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            NCubePlan cubePlan = modelContext.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
        }

        kapSparkSession.close();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        // 3. Auto suggested model is able to serve related query
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 0, 3, "default");
            kapSparkSession.use(getProject());
            populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
        }

        // 4. Feed bad queries
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql_bad", 0, 0, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(0, modelContexts.size());
        }

        // 5. Feed query with inner join using same fact table, should create another model
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 3, 4, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertNotEquals(targetModelName, dataModel.getName());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            NCubePlan cubePlan = modelContext.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
        }

        kapSparkSession.close();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        // 6. Finally, run all queries
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 0, 4, "default");
            kapSparkSession.use(getProject());
            populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
            NExecAndComp.execAndCompare(queries, kapSparkSession, CompareLevel.SAME, "default");
        }

        kapSparkSession.close();
    }

    @Test
    public void testAutoMultipleModel() throws Exception {

        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));

        Map<String, NCubePlan> cubePlanOfParts = new HashMap<>();
        Map<String, NCubePlan> cubePlanOfAll = new HashMap<>();

        // 1. Feed queries part1
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 0, 2, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NModelContext nModelContext : modelContexts) {
                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
                cubePlanOfParts.put(cubePlan.getId(), cubePlan);
            }
        }

        // 2. Feed queries part2
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 2, 4, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NModelContext nModelContext : modelContexts) {
                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
                cubePlanOfParts.put(cubePlan.getId(), cubePlan);
            }
        }

        // 3. Retry all queries
        {
            List<Pair<String, String>> queries = fetchPartialQueries("auto/sql", 0, 4, "default");
            NSmartMaster master = proposeCubeWithSmartMaster(queries);
            kapSparkSession.use(getProject());
            kapSparkSession.buildAllCubes(kylinConfig, getProject());

            List<NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NModelContext nModelContext : modelContexts) {
                NCubePlan cubePlan = nModelContext.getTargetCubePlan();
                cubePlanOfAll.put(cubePlan.getId(), cubePlan);
            }
        }

        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
        {
            Assert.assertEquals(cubePlanOfParts.size(), cubePlanOfAll.size());
            for (NCubePlan actual : cubePlanOfAll.values()) {
                NCubePlan expected = cubePlanOfParts.get(actual.getId());
                Assert.assertNotNull(expected);
                // compare cuboids
                Assert.assertEquals(expected.getAllCuboids().size(), actual.getAllCuboids().size());
                Assert.assertEquals(expected.getAllCuboidLayouts().size(), actual.getAllCuboidLayouts().size());
                for (NCuboidDesc actualCuboid : actual.getAllCuboids()) {
                    NCuboidDesc expectedCuboid = expected.getCuboidDesc(actualCuboid.getId());
                    Assert.assertThat(expectedCuboid.getDimensions(), CoreMatchers.is(actualCuboid.getDimensions()));
                    Assert.assertThat(expectedCuboid.getMeasures(), CoreMatchers.is(actualCuboid.getMeasures()));
                }
            }
        }

        kapSparkSession.close();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testAllQueries() throws Exception {
        executeTestScenario(
//                new TestScenario("sql", CompareLevel.SAME),
                new TestScenario("sql_lookup", CompareLevel.SAME),
                new TestScenario("sql_casewhen", CompareLevel.SAME),
                new TestScenario("sql_like", CompareLevel.SAME),
                new TestScenario("sql_cache", CompareLevel.SAME),
                new TestScenario("sql_derived", CompareLevel.SAME),
                new TestScenario("sql_datetime", CompareLevel.SAME),
                new TestScenario("sql_tableau", CompareLevel.SAME_ROWCOUNT),
//                                new TestScenario("sql_distinct", CompareLevel.SAME),
//                                new TestScenario("sql_distinct_dim", CompareLevel.SAME),
//                                new TestScenario("sql_distinct_precisely", CompareLevel.SAME, "left"),
                new TestScenario("sql_timestamp", CompareLevel.NONE),
                new TestScenario("sql_multi_model", CompareLevel.SAME),
                new TestScenario("sql_orderby", CompareLevel.SAME),
                new TestScenario("sql_snowflake", CompareLevel.SAME),
                new TestScenario("sql_topn", CompareLevel.SAME, "left"),
                new TestScenario("sql_join", CompareLevel.SAME),
                new TestScenario("sql_union", CompareLevel.SAME),
                new TestScenario("sql_window", CompareLevel.NONE),
                new TestScenario("sql_h2_uncapable", CompareLevel.NONE),
                new TestScenario("sql_grouping", CompareLevel.SAME),
                new TestScenario("sql_hive", CompareLevel.SAME),
        // FIXME  https://github.com/Kyligence/KAP/issues/8090   percentile and sql_intersect_count do not support
        //                new TestScenario("sql_intersect_count", CompareLevel.NONE, "left")
        //                new TestScenario("sql_percentile", CompareLevel.NONE)//,
                new TestScenario("sql_powerbi", CompareLevel.SAME),
                new TestScenario("sql_raw", CompareLevel.SAME),
                new TestScenario("sql_rawtable", CompareLevel.SAME),
                new TestScenario("sql_subquery", CompareLevel.SAME)
        );
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        String[] exclusionList = new String[] {};
        new TestScenario("temp", CompareLevel.SAME, exclusionList).execute();
    }

    /***************
     * Test Kylin test queries with auto modeling
     */

    // FIXME query02 will be fixed in #7257
    @Test
    public void testCommonQuery() throws Exception {
        String[] exclusionList = new String[] { "query02.sql" };
        new TestScenario("sql", CompareLevel.SAME, exclusionList).execute();
    }

    @Test
    public void testDistinct() throws Exception {
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
        new TestScenario("sql_distinct", CompareLevel.SAME).execute();
    }

    @Test
    public void testDistinctDim() throws Exception {
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
        new TestScenario("sql_distinct_dim", CompareLevel.SAME).execute();
    }

    @Test
    public void testDistinctPrecisely() throws Exception {
        kylinConfig.setProperty("kap.smart.conf.measure.count-distinct.return-type", "bitmap");
        new TestScenario("sql_distinct_precisely", CompareLevel.SAME, "left").execute();
    }

    @Ignore("not storage query, skip")
    @Test
    public void testTableauProbing() throws Exception {
        new TestScenario("tableau_probing", CompareLevel.NONE).execute();
    }

    @Test
    public void testLimitCorrectness() throws Exception {
        List<Pair<String, String>> queries = fetchPartialQueries("sql", 0, 0, JOIN_TYPE);
        buildCubeWithSparkSession(queries);
        KapSparkSession kapSparkSession = new KapSparkSession(SparkContext.getOrCreate(sparkConf));
        kapSparkSession.use(getProject());
        populateSSWithCSVData(kylinConfig, getProject(), kapSparkSession);
        NExecAndComp.execLimitAndValidate(queries, kapSparkSession, JOIN_TYPE);
    }

    /****************
     * Following cased are not in Newten M1 scope
     */

    @Ignore("not in Newten M1 scope")
    @Test
    public void testDynamic() throws Exception {
        new TestScenario("sql_dynamic", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testExtendedColumn() throws Exception {
        new TestScenario("sql_extended_column", CompareLevel.SAME).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testInvalid() throws Exception {
        new TestScenario("sql_invalid", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testLimit() throws Exception {
        new TestScenario("sql_limit", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testMassin() throws Exception {
        new TestScenario("sql_massin", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testMassinDistinct() throws Exception {
        new TestScenario("sql_massin_distinct", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testStreaming() throws Exception {
        new TestScenario("sql_streaming", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testTimeout() throws Exception {
        new TestScenario("sql_timeout", CompareLevel.NONE).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testVerifyContent() throws Exception {
        new TestScenario("sql_verifyContent", CompareLevel.SAME).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testVerifyCount() throws Exception {
        new TestScenario("sql_verifyCount", CompareLevel.SAME_ROWCOUNT).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testTimeStampAdd() throws Exception {
        new TestScenario("sql_current_date", CompareLevel.SAME).execute();
    }

    @Ignore("not in Newten M1 scope")
    @Test
    public void testPercentileQuery() throws Exception {
        new TestScenario("sql_percentile", CompareLevel.SAME).execute();
    }

    @Override
    protected void executeTestScenario(TestScenario... tests) throws Exception {
        if ("true".equals(System.getProperty("skipAutoModelingCI"))) { // -DskipAutoModelingCI=true
            return;
        }
        super.executeTestScenario(tests);
    }
}
