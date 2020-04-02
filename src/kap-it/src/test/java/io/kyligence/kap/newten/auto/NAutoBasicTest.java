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
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.sql.SparderEnv;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
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
    public void testUsedColumnsIsTomb() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select lstg_format_name from test_kylin_fact group by lstg_format_name",
                "select sum(price * item_count) from test_kylin_fact" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]).isFailed());
        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls[1]).isFailed());
        NDataModel dataModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, dataModel.getComputedColumnDescs().size());

        // delete computed column add a existing column
        dataModel.getAllNamedColumns().forEach(column -> {
            if (column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.lstg_format_name")) {
                column.setStatus(NDataModel.ColumnStatus.TOMB);
            }
            if (column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.cc_auto_1")) {
                column.setName("modified_cc_column");
                column.setStatus(NDataModel.ColumnStatus.TOMB);
            }
        });
        dataModel.getAllNamedColumns().get(dataModel.getAllNamedColumns().size() - 1)
                .setStatus(NDataModel.ColumnStatus.TOMB);
        dataModel.getComputedColumnDescs().clear();
        dataModel.getAllMeasures().forEach(measure -> {
            if (measure.getId() == 100001) {
                measure.setTomb(true);
            }
        });
        NDataModelManager.getInstance(kylinConfig, getProject()).updateDataModelDesc(dataModel);

        // verify update success
        NDataModel updatedModel = NDataModelManager.getInstance(kylinConfig, getProject())
                .getDataModelDesc(dataModel.getUuid());
        Assert.assertTrue(updatedModel.getComputedColumnDescs().isEmpty());
        List<NDataModel.NamedColumn> targetColumns = updatedModel.getAllNamedColumns().stream()
                .filter(column -> column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.lstg_format_name")
                        || column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.cc_auto_1"))
                .collect(Collectors.toList());
        Assert.assertEquals(2, targetColumns.size());
        targetColumns.forEach(column -> {
            Assert.assertFalse(column.isExist());
            if (column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.cc_auto_1")) {
                Assert.assertEquals("modified_cc_column", column.getName());
            }
        });
        Assert.assertTrue(updatedModel.getAllMeasures().get(1).isTomb());

        // update model to expert mode
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(getProject());
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sqls[0]).getPendingMsg()
                .contains("but the dimension [TEST_KYLIN_FACT.LSTG_FORMAT_NAME] is missing. "
                        + "Please add the above dimension before attempting to accelerate this query."));
        Assert.assertTrue(accelerateInfoMap.get(sqls[1]).getPendingMsg()
                .contains("Please add the above measure before attempting to accelerate this query."));

        // update model to semi-auto-mode
        kylinConfig.setProperty("kylin.metadata.semi-automatic-mode", "true");
        smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runOptRecommendation(null);
        accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(sqls[1]).isNotSucceed());
        NDataModel model = OptimizeRecommendationManager.getInstance(getTestConfig(), getProject())
                .applyModel(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        NDataModel.Measure newMeasure = model.getAllMeasures().stream().filter(measure -> measure.getId() == 10100001L)
                .findFirst().orElse(null);
        Assert.assertNotNull(newMeasure);
        Assert.assertFalse(newMeasure.isTomb());
        NDataModel.NamedColumn namedColumn = model.getAllNamedColumns().stream()
                .filter(column -> column.getId() >= OptimizeRecommendationManager.ID_OFFSET)
                .filter(column -> column.getAliasDotColumn().equalsIgnoreCase("test_kylin_fact.cc_auto_1")) //
                .findFirst().orElse(null);
        Assert.assertNotNull(namedColumn);
        Assert.assertEquals(NDataModel.ColumnStatus.EXIST, namedColumn.getStatus());
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
    public void testSelectTimestamp() throws Exception {
        new TestScenario(NExecAndComp.CompareLevel.SAME, "sql_for_automodeling/sql_timestamp").execute();
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
        Assert.assertFalse(accelerateInfoMapAfterOpt.get(sql).isNotSucceed());
        //        Assert.assertTrue(accelerateInfoMapAfterOpt.get(sql).getRelatedLayouts().isEmpty());
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

    @Test
    public void testIndexReducer() {
        // use smart-model to prepare a model
        KylinConfig kylinConfig = getTestConfig();
        String project = getProject();

        String[] sqls = {
                "select LSTG_FORMAT_NAME,slr_segment_cd ,sum(price) as GMV from test_kylin_fact\n"
                        + " group by LSTG_FORMAT_NAME ,slr_segment_cd",
                "select LSTG_FORMAT_NAME,slr_segment_cd ,sum(price) as GMV, min(price) as MMV from test_kylin_fact\n"
                        + " group by LSTG_FORMAT_NAME ,slr_segment_cd" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, project, sqls);
        smartMaster.runAll();

        NSmartContext smartContext = smartMaster.getContext();
        Map<String, AccelerateInfo> accelerationInfoMap = smartContext.getAccelerateInfoMap();
        val relatedLayoutsForSql0 = accelerationInfoMap.get(sqls[0]).getRelatedLayouts();
        val relatedLayoutsForSql1 = accelerationInfoMap.get(sqls[1]).getRelatedLayouts();
        long layoutForSql0 = relatedLayoutsForSql0.iterator().next().getLayoutId();
        long layoutForSql1 = relatedLayoutsForSql1.iterator().next().getLayoutId();
        Assert.assertNotEquals(layoutForSql0, layoutForSql1);

        // set to semi-auto to check tailoring layouts
        overwriteSystemProp("kylin.metadata.semi-automatic-mode", "true");
        final ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        NProjectManager.getInstance(kylinConfig).updateProject(projectInstance);
        NSmartContext.NModelContext modelContext = smartContext.getModelContexts().get(0);
        NDataModel targetModel = modelContext.getTargetModel();
        NIndexPlanManager.getInstance(kylinConfig, project).updateIndexPlan(targetModel.getUuid(), copyForWrite -> {
            copyForWrite.setIndexes(Lists.newArrayList());
        });

        smartMaster = new NSmartMaster(kylinConfig, project, sqls);
        smartMaster.runOptRecommendation(null);

        smartContext = smartMaster.getContext();
        accelerationInfoMap = smartContext.getAccelerateInfoMap();
        val relatedLayoutsSemiForSql0 = accelerationInfoMap.get(sqls[0]).getRelatedLayouts();
        val relatedLayoutsSemiForSql1 = accelerationInfoMap.get(sqls[1]).getRelatedLayouts();
        long layoutSemiForSql0 = relatedLayoutsSemiForSql0.iterator().next().getLayoutId();
        long layoutSemiForSql1 = relatedLayoutsSemiForSql1.iterator().next().getLayoutId();
        Assert.assertEquals(layoutSemiForSql0, layoutSemiForSql1);
    }

    private NSmartMaster proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        NSmartMaster master = new NSmartMaster(kylinConfig, getProject(), sqls);
        master.runAll();
        return master;
    }
}
