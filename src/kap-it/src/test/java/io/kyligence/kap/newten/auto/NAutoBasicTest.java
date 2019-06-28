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

package io.kyligence.kap.newten.auto;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.sql.SparderEnv;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;

public class NAutoBasicTest extends NAutoTestBase {

    @Test
    public void testAutoSingleModel() throws Exception {

        // 1. Create simple model with one fact table
        String targetModelId;
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 1);
            NSmartMaster master = proposeWithSmartMaster(queries);
            buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            targetModelId = dataModel.getUuid();
            Assert.assertEquals(1, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 2. Feed query with left join using same fact table, should update same model
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 1, 2);
            NSmartMaster master = proposeWithSmartMaster(queries);
            buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertEquals(targetModelId, dataModel.getUuid());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        //FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));

        // 3. Auto suggested model is able to serve related query
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 3);
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            NExecAndComp.execAndCompare(queries, getProject(), NExecAndComp.CompareLevel.SAME, "default");
        }

        // 4. Feed bad queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql_bad", 0, 0);
            NSmartMaster master = proposeWithSmartMaster(queries);
            buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(0, modelContexts.size());
        }

        // 5. Feed query with inner join using same fact table, should create another model
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 3, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);
            buildAllCubes(kylinConfig, getProject());

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            Assert.assertEquals(1, modelContexts.size());
            NSmartContext.NModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertNotEquals(targetModelId, dataModel.getUuid());
            Assert.assertEquals(2, dataModel.getAllTables().size());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }

        // 6. Finally, run all queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 4);
            populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
            NExecAndComp.execAndCompare(queries, getProject(), NExecAndComp.CompareLevel.SAME, "default");
        }

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testAutoMultipleModel() throws Exception {

        Map<String, IndexPlan> indexPlanOfParts = new HashMap<>();
        Map<String, IndexPlan> indexPlanOfAll = new HashMap<>();

        // 1. Feed queries part1
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 2);
            NSmartMaster master = proposeWithSmartMaster(queries);

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 2. Feed queries part2
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 2, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfParts.put(indexPlan.getId(), indexPlan);
            }
        }

        // 3. Retry all queries
        {
            List<Pair<String, String>> queries = fetchQueries("sql_for_automodeling/sql", 0, 4);
            NSmartMaster master = proposeWithSmartMaster(queries);

            List<NSmartContext.NModelContext> modelContexts = master.getContext().getModelContexts();
            for (NSmartContext.NModelContext nModelContext : modelContexts) {
                IndexPlan indexPlan = nModelContext.getTargetIndexPlan();
                indexPlanOfAll.put(indexPlan.getId(), indexPlan);
            }
        }

        // 4. Suggested cuboids should be consistent no matter modeling with partial or full queries
        {
            Assert.assertEquals(indexPlanOfParts.size(), indexPlanOfAll.size());
            for (IndexPlan actual : indexPlanOfAll.values()) {
                IndexPlan expected = indexPlanOfParts.get(actual.getId());
                Assert.assertNotNull(expected);
                // compare cuboids
                Assert.assertEquals(expected.getAllIndexes().size(), actual.getAllIndexes().size());
                Assert.assertEquals(expected.getAllLayouts().size(), actual.getAllLayouts().size());
                for (IndexEntity actualCuboid : actual.getAllIndexes()) {
                    IndexEntity expectedCuboid = expected.getIndexEntity(actualCuboid.getId());
                    Assert.assertThat(expectedCuboid.getDimensions(), CoreMatchers.is(actualCuboid.getDimensions()));
                    Assert.assertThat(expectedCuboid.getMeasures(), CoreMatchers.is(actualCuboid.getMeasures()));
                }
            }
        }

        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    /**
     * Test a query only only with count(*), can build and query from IndexPlan,
     * don't move it.
     */
    @Test
    public void testCountStar() throws Exception {
        new TestScenario(NExecAndComp.CompareLevel.SAME, "sql_for_automodeling/sql_count_star").execute();
    }

    @Test
    public void testLimitCorrectness() throws Exception {
        new TestScenario(NExecAndComp.CompareLevel.SAME, true, "query/sql").execute();
    }

    /**
     * (auto-modeling) one sql generates many OLAPContexts but it failed to accelerate.
     * The second OLAPContext failed to propose cc when proposing target model.
     */
    @Test
    public void testPartialFailedWhenProposingWhenOneSqlAccelerating() {
        KylinConfig kylinConfig = getTestConfig();
        final String project = "newten";
        String sql = "select l.cal_dt, sum(left_join_gvm) as left_join_sum, sum(inner_join_gvm) as inner_join_sum\n" //
                + "from (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact " //
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id " //
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n" //
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) l inner join (\n" //
                + "    select t2.cal_dt, SUM(PRICE_TOTAL + 1) as inner_join_gvm\n" //
                + "    from (select price*item_count as price_total, cal_dt, leaf_categ_id, lstg_site_id from test_kylin_fact) t2 \n" //
                + "        inner JOIN edw.test_cal_dt as test_cal_dt ON t2.cal_dt = test_cal_dt.cal_dt\n" //
                + "        inner JOIN test_category_groupings ON t2.leaf_categ_id = test_category_groupings.leaf_categ_id " //
                + "          AND t2.lstg_site_id = test_category_groupings.site_id\n" //
                + "    group by t2.cal_dt\n" //
                + "  ) i on l.cal_dt = i.cal_dt\n" //
                + "group by l.cal_dt";

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, project, new String[] { sql });
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();

        // assert everything is ok after select model
        val accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(sql).getRelatedLayouts().isEmpty());
        smartMaster.optimizeModel();

        // assert it failed in the step of optimize model
        final List<NSmartContext.NModelContext> modelContexts = smartMaster.getContext().getModelContexts();
        val accelerateInfoMapAfterOpt = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(2, modelContexts.size());
        Assert.assertTrue(accelerateInfoMapAfterOpt.get(sql).isNotSucceed());
        Assert.assertTrue(accelerateInfoMapAfterOpt.get(sql).getRelatedLayouts().isEmpty());
    }

    /**
     * (manual maintain type) one sql generates many OLAPContexts but it failed to accelerate.
     * The second OLAPContext failed to reuse an existing model when proposing layouts.
     */
    @Test
    public void testPartialFailedWhenProposingWhenOneSqlAcceleratingWithManualMaintainType() {
        KylinConfig kylinConfig = getTestConfig();
        final String project = "newten";
        String sql = "select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact "
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, project, new String[] { sql });
        smartMaster.runAll();

        // confirm auto-modeling is ok
        val accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertFalse(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(1, modelContexts.size());

        //set maintain model type to manual
        final NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        final ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(project));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);

        // propose model under the scene of manual maintain type
        sql = "select l.cal_dt, sum(left_join_gvm) as left_join_sum, sum(inner_join_gvm) as inner_join_sum\n"
                + "from (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price) as left_join_gvm\n" //
                + "    from test_kylin_fact "
                + "       left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" //
                + "       left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "         AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) l inner join (\n" //
                + "    select test_kylin_fact.cal_dt, sum(price+1) as inner_join_gvm\n" //
                + "    from test_kylin_fact\n" //
                + "        left JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "        left JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                + "          AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "    group by test_kylin_fact.cal_dt\n" //
                + "  ) i on l.cal_dt = i.cal_dt\n" //
                + "group by l.cal_dt";
        smartMaster = new NSmartMaster(kylinConfig, project, new String[] { sql });
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();

        // assert everything is ok after optimize model
        val accelerationMapAfterOptModel = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationMapAfterOptModel.get(sql).isNotSucceed());
        Assert.assertTrue(accelerationMapAfterOptModel.get(sql).getRelatedLayouts().isEmpty());

        // assert everything is ok after select index plan
        smartMaster.selectIndexPlan();
        val accelerationMapAfterSelectIndexPlan = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationMapAfterSelectIndexPlan.get(sql).isNotSucceed());
        Assert.assertTrue(accelerationMapAfterSelectIndexPlan.get(sql).getRelatedLayouts().isEmpty());

        // assert it failed at optimize index plan
        smartMaster.optimizeIndexPlan();
        val accelerateInfoAfterOptIndexPlan = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoAfterOptIndexPlan.get(sql).isNotSucceed());
        Assert.assertTrue(accelerateInfoAfterOptIndexPlan.get(sql).getRelatedLayouts().isEmpty());
    }

    private NSmartMaster proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        NSmartMaster master = new NSmartMaster(kylinConfig, getProject(), sqls);
        master.runAll();
        return master;
    }
}
