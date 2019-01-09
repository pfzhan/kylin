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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.cube.model.IndexEntity;
import io.kyligence.kap.cube.model.IndexPlan;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;

/**
 * This class is used to test corrupted metadata. Not run in IT test!
 */
@Ignore
public class NCorruptMetadataTest extends NAutoTestBase {

    @Test
    public void testIndexMissingDimension() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "index_missing_dimension", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMeassage = "layout 1's dimension is illegal";
            assertWithException(e, expectedMessage, expectedCauseMeassage);
        }
    }

    @Test
    public void testIndexMissingMeasure() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "index_missing_measure", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "layout 1's measure is illegal";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    @Test
    public void testCorruptColOrder() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "corrupt_colOrder", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "layout 1's measure is illegal";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    /**
     * Column missing in the IndexPlan's json file, program ends normally,
     */
    @Test
    public void testIndexPlanMissingColumnEncoding() {

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "index_plan_missing_col_encoding", sqls);
        smartMaster.runAll();
        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);

        Assert.assertEquals(5, modelContext.getOrigIndexPlan().getIndexPlanOverrideEncodings().size());
        Assert.assertEquals(11, modelContext.getTargetIndexPlan().getIndexPlanOverrideEncodings().size());
        Assert.assertEquals(1, modelContext.getTargetIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContext.getTargetIndexPlan().getAllLayouts().size());
    }

    @Test
    public void testIndexPlanMissingColumnEncodingManually() {
        setModelMaintainTypeToManual(kylinConfig, "index_plan_missing_col_encoding");
        testIndexPlanMissingColumnEncoding();
    }

    /**
     * The result of auto-modeling will recommend two indexes, but only one index exists
     * in the currently provided model json file.
     */
    @Test
    public void testMissingOneIndex() {
        String[] sqls = new String[] { "select a.*, test_kylin_fact.lstg_format_name as lstg_format_name \n"
                + "from ( select cal_dt, sum(price) as sum_price from test_kylin_fact\n"
                + "         where cal_dt > '2010-01-01' group by cal_dt) a \n"
                + "join test_kylin_fact on a.cal_dt = test_kylin_fact.cal_dt \n"
                + "group by lstg_format_name, a.cal_dt, a.sum_price" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "index_missing", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOrigIndexPlan().getAllIndexes().size());

        final List<IndexEntity> originalIndexes = modelContext.getOrigIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, originalIndexes.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().size());
        final List<IndexEntity> targetIndexes = modelContext.getTargetIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, targetIndexes.get(0).getId());
        Assert.assertEquals(10000L, targetIndexes.get(1).getId());
    }

    @Test
    public void testMissingOneIndexManually() {
        setModelMaintainTypeToManual(kylinConfig, "index_missing");
        testMissingOneIndex();
    }

    /**
     * The result of auto-modeling will recommend a index with two layouts, but only one layout exists
     * in the currently provided IndexPlan json file.
     */
    @Test
    public void testMissingLayout() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where lstg_format_name = 'xxx' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt",
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where cal_dt > '2012-02-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "index_missing_layout", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOrigIndexPlan().getAllIndexes().get(0).getLayouts().size());

        final List<LayoutEntity> originalLayouts = modelContext.getOrigIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, originalLayouts.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts().size());
        final List<LayoutEntity> layouts = modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, layouts.get(0).getId());
        Assert.assertEquals(3, layouts.get(1).getId());
    }

    @Test
    public void testMissgingLayoutManually() {
        setModelMaintainTypeToManual(kylinConfig, "index_missing_layout");
        testMissingLayout();
    }

    /**
     * There are no indexes in the IndexPlan's json file
     */
    @Test
    public void testLoadEmptyCubePlan() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "index_plan_empty", sqls);
        smartMaster.runAll();
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        List<NSmartContext.NModelContext> modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(0, modelContexts.get(0).getOrigIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContexts.get(0).getTargetIndexPlan().getAllIndexes().size());
    }

    @Test
    public void testLoadEmptyCubePlanManually() {
        setModelMaintainTypeToManual(kylinConfig, "index_plan_empty");
        testLoadEmptyCubePlan();
    }

    /**
     * There are no columns and measures in the model's json file
     */
    @Test
    public void testLoadEmptyModel() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "model_empty", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            assertModelContexts(smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "Error loading NDataModel at /model_empty/model_desc/4aa8bb26-a17c-499f-89ce-eeaae6f9cf01.json";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    @Test
    public void testModelMissingJoinTables() {
        String[] sqls = new String[] { "SELECT test_category_groupings.meta_categ_name, "
                + "test_category_groupings.categ_lvl2_name, test_kylin_fact.lstg_format_name,\n"
                + "\t SUM(test_kylin_fact.price) AS GMV, COUNT(*) AS TRANS_CNT\n"
                + "FROM test_kylin_fact INNER JOIN test_category_groupings\n"
                + "\tON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"
                + "\t\tAND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "WHERE test_kylin_fact.seller_id < 9 OR test_kylin_fact.seller_id > 10000003\n"
                + "GROUP BY test_category_groupings.meta_categ_name, "
                + "test_category_groupings.categ_lvl2_name, test_kylin_fact.lstg_format_name" };

        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "model_missing_join_tables", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            assertModelContexts(smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "Error loading NDataModel at /model_missing_join_tables/model_desc/8ab5e866-51a4-43a7-b24b-c6aa4fccd09e.json";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    @Test
    public void testModelMissingUsedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "model_missing_column", sqls);
            smartMaster.runAll();
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            assertModelContexts(smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "Error loading NDataModel at /model_missing_column/model_desc/193734fa-3ac0-4f27-80a0-6f3071e53202.json";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    @Test
    public void testModelMissingMeasure() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = null;
        try {
            smartMaster = new NSmartMaster(getTestConfig(), "model_missing_measure", sqls);
            smartMaster.runAll();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            assertModelContexts(smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "Error loading NDataModel at /model_missing_measure/model_desc/9b2a8065-37cb-4a9d-bfdb-82e8198b89b6.json";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
    }

    /**
     *  Missing column in the model and no IndexPlan json file.
     */
    @Test
    public void testModelMissingUnusedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "flaw_model", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(6, modelContext.getOrigModel().getEffectiveCols().size());
        Assert.assertEquals(11, modelContext.getTargetModel().getEffectiveCols().size());
    }

    @Test
    public void testModelMissingUnusedColumnManually() {
        setModelMaintainTypeToManual(kylinConfig, "flaw_model");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "flaw_model", sqls);
        smartMaster.runAll();
        assertAccelerationInfoMap(sqls, smartMaster);
        final IndexPlan targetIndexPlan = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(0, targetIndexPlan.getAllIndexes().size());
        String expectedCauseMessage = "In the model designer project, the system is not allowed to modify the semantic layer "
                + "(dimensions, measures, tables, and joins) of the model. Column not found. Please add column ";
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.getBlockingCause().getMessage().startsWith(expectedCauseMessage));
    }

    /**
     * Missing partition in the model's json file, the program ends normally.
     */
    @Test
    public void testModelMissingPartition() {

        preparePartition();

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "model_missing_partition", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext.getOrigModel().getPartitionDesc().getPartitionDateColumn());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT",
                modelContext.getTargetModel().getPartitionDesc().getPartitionDateColumn());
    }

    @Test
    public void testModelMissingPartitionManually() {
        setModelMaintainTypeToManual(getTestConfig(), "model_missing_partition");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "model_missing_partition", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isBlocked());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(modelContext.getOrigModel(), modelContext.getTargetModel());
        Assert.assertNull(modelContext.getOrigModel().getPartitionDesc().getPartitionDateColumn());
        Assert.assertNull(modelContext.getTargetModel().getPartitionDesc().getPartitionDateColumn());
    }

    private void preparePartition() {
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        NDataLoadingRangeManager.getInstance(getTestConfig(), "model_missing_partition")
                .createDataLoadingRange(dataLoadingRange);
    }

    private void assertModelContexts(NSmartMaster smartMaster) {
        if (smartMaster != null) {
            Assert.assertNull(smartMaster.getContext().getModelContexts());
        }
    }

    private void assertAccelerationInfoMap(String[] sqls, NSmartMaster smartMaster) {
        if (smartMaster != null) {
            Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
            AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isBlocked());
            Assert.assertTrue(accelerateInfo.getRelatedLayouts().isEmpty());
        }
    }

    private void assertWithException(Exception e, String expectedMessage, String expectedCauseMessage) {
        Assert.assertTrue(e instanceof TransactionException);
        Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        Assert.assertEquals(expectedMessage, e.getMessage());
        Assert.assertEquals(expectedCauseMessage, e.getCause().getMessage());
    }

    private void setModelMaintainTypeToManual(KylinConfig kylinConfig, String projectName) {
        final NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        MaintainModelType originMaintainType = projectManager.getProject(projectName).getMaintainModelType();
        final ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(projectName));
        if (originMaintainType == MaintainModelType.AUTO_MAINTAIN) {
            originMaintainType = MaintainModelType.MANUAL_MAINTAIN;
        }
        projectUpdate.setMaintainModelType(originMaintainType);
        projectManager.updateProject(projectUpdate);
    }

    @Override
    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/corrupt_metadata");
        kylinConfig = getTestConfig();
    }
}
