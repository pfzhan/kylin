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

import org.hamcrest.core.StringContains;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;

public class NCorruptMetadataTest extends NAutoTestBase {

    @Test
    public void testIndexMissingDimension() {
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price), sum(item_count) from test_kylin_fact where cal_dt < '2012-03-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        SmartMaster smartMaster = null;
        try {
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "index_missing_dimension", sqls);
            smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
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
        SmartMaster smartMaster = null;
        try {
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "index_missing_measure", sqls);
            smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
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
        SmartMaster smartMaster = null;
        try {
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "corrupt_colOrder", sqls);
            smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            Assert.fail();
        } catch (Exception e) {
            assertAccelerationInfoMap(sqls, smartMaster);
            String expectedMessage = "exhausted max retry times, transaction failed due to inconsistent state";
            String expectedCauseMessage = "layout 1's measure is illegal";
            assertWithException(e, expectedMessage, expectedCauseMessage);
        }
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
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "index_missing", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().size());

        final List<IndexEntity> originalIndexes = modelContext.getOriginIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, originalIndexes.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().size());
        final List<IndexEntity> targetIndexes = modelContext.getTargetIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, targetIndexes.get(0).getId());
        Assert.assertEquals(10000L, targetIndexes.get(1).getId());
    }

    @Test
    public void testMissingOneIndexManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "index_missing");
        String[] sqls = new String[] { "select a.*, test_kylin_fact.lstg_format_name as lstg_format_name \n"
                + "from ( select cal_dt, sum(price) as sum_price from test_kylin_fact\n"
                + "         where cal_dt > '2010-01-01' group by cal_dt) a \n"
                + "join test_kylin_fact on a.cal_dt = test_kylin_fact.cal_dt \n"
                + "group by lstg_format_name, a.cal_dt, a.sum_price" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(), "index_missing",
                sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().size());

        final List<IndexEntity> originalIndexes = modelContext.getOriginIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, originalIndexes.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().size());
        final List<IndexEntity> targetIndexes = modelContext.getTargetIndexPlan().getAllIndexes();
        Assert.assertEquals(20000000000L, targetIndexes.get(0).getId());
        Assert.assertEquals(10000L, targetIndexes.get(1).getId());
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
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "index_missing_layout", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().get(0).getLayouts().size());

        final List<LayoutEntity> originalLayouts = modelContext.getOriginIndexPlan().getAllIndexes().get(0)
                .getLayouts();
        Assert.assertEquals(2, originalLayouts.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts().size());
        final List<LayoutEntity> layouts = modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, layouts.get(0).getId());
        Assert.assertEquals(3, layouts.get(1).getId());
    }

    @Test
    public void testMissgingLayoutManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "index_missing_layout");
        String[] sqls = new String[] {
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where lstg_format_name = 'xxx' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt",
                "select lstg_format_name, cal_dt, sum(price) from test_kylin_fact where cal_dt > '2012-02-01' "
                        + "group by lstg_format_name, cal_dt order by lstg_format_name, cal_dt" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(),
                "index_missing_layout", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(1, modelContext.getOriginIndexPlan().getAllIndexes().get(0).getLayouts().size());

        final List<LayoutEntity> originalLayouts = modelContext.getOriginIndexPlan().getAllIndexes().get(0)
                .getLayouts();
        Assert.assertEquals(2, originalLayouts.get(0).getId());

        Assert.assertEquals(2, modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts().size());
        final List<LayoutEntity> layouts = modelContext.getTargetIndexPlan().getAllIndexes().get(0).getLayouts();
        Assert.assertEquals(2, layouts.get(0).getId());
        Assert.assertEquals(3, layouts.get(1).getId());
    }

    /**
     * There are no indexes in the IndexPlan's json file
     */
    @Test
    public void testLoadEmptyCubePlan() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "index_plan_empty", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        List<AbstractContext.ModelContext> modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(0, modelContexts.get(0).getOriginIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContexts.get(0).getTargetIndexPlan().getAllIndexes().size());
    }

    @Test
    public void testLoadEmptyCubePlanManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "index_plan_empty");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(), "index_plan_empty",
                sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        List<AbstractContext.ModelContext> modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(0, modelContexts.get(0).getOriginIndexPlan().getAllIndexes().size());
        Assert.assertEquals(1, modelContexts.get(0).getTargetIndexPlan().getAllIndexes().size());
    }

    /**
     * There are no columns and measures in the model's json file
     */
    @Test
    public void testLoadEmptyModel() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "model_empty", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isFailed());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertNull(modelContext.getOriginIndexPlan());
        Assert.assertNotNull(modelContext.getTargetModel());
        Assert.assertNotNull(modelContext.getTargetIndexPlan());
    }

    @Test
    public void testLoadEmptyModelManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "model_empty");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(), "model_empty", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getOriginModel());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());
    }

    @Test
    public void testModelWithoutIndexPlan() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "model_without_index_plan", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(sqls[0]).isFailed());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertNull(modelContext.getOriginIndexPlan());
        Assert.assertNotNull(modelContext.getTargetModel());
        Assert.assertNotNull(modelContext.getTargetIndexPlan());
    }

    @Test
    public void testModelWithoutIndexPlanManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "model_without_index_plan");
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(),
                "model_without_index_plan", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getOriginModel());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());
    }

    // Losing the join table and measure will produce the same result, no longer shown here
    @Test
    public void testModelMissingUsedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "model_missing_column", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel());
        Assert.assertEquals(11, modelContext.getTargetModel().getEffectiveCols().size());
    }

    @Test
    public void testModelMissingColumnManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "model_missing_column");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(),
                "model_missing_column", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertEquals(expectedMessage, accelerateInfo.getPendingMsg());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getOriginModel());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getOriginIndexPlan());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());
        Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan());
    }

    /**
     *  Missing column in the model and no IndexPlan json file.
     */
    @Test
    public void testModelMissingUnusedColumn() {
        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "flaw_model", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(7, modelContext.getOriginModel().getEffectiveCols().size());
        Assert.assertEquals(11, modelContext.getTargetModel().getEffectiveCols().size());
    }

    @Ignore("Semi-Auto-mode need a new design")
    @Test
    public void testModelMissingUnusedColumnManually() {
        AccelerationContextUtil.transferProjectToPureExpertMode(kylinConfig, "flaw_model");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(), "flaw_model", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertEquals(modelContext.getTargetModel(), modelContext.getOriginModel());
        Assert.assertEquals(7, modelContext.getOriginModel().getEffectiveCols().size());
    }

    /**
     * Missing partition in the model's json file, the program ends normally.
     */
    @Test
    public void testModelMissingPartition() {

        preparePartition();

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "model_missing_partition", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNotNull(modelContext.getOriginModel());
        Assert.assertNull(modelContext.getOriginModel().getPartitionDesc().getPartitionDateColumn());
        Assert.assertNotNull(modelContext.getTargetModel());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT",
                modelContext.getTargetModel().getPartitionDesc().getPartitionDateColumn());
    }

    @Test
    public void testModelMissingPartitionManually() {

        preparePartition();
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "model_missing_partition");

        String[] sqls = new String[] { "select lstg_format_name, sum(price) from test_kylin_fact "
                + "group by lstg_format_name order by lstg_format_name" };
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(getTestConfig(),
                "model_missing_partition", sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        final AbstractContext.ModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext.getOriginModel().getPartitionDesc().getPartitionDateColumn());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT",
                modelContext.getTargetModel().getPartitionDesc().getPartitionDateColumn());
    }

    private void preparePartition() {
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        NDataLoadingRangeManager.getInstance(getTestConfig(), "model_missing_partition")
                .createDataLoadingRange(dataLoadingRange);
    }

    private void assertAccelerationInfoMap(String[] sqls, SmartMaster smartMaster) {
        if (smartMaster != null) {
            Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
            AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
            Assert.assertTrue(accelerateInfo.isFailed());
            Assert.assertTrue(accelerateInfo.getRelatedLayouts().isEmpty());
        }
    }

    private void assertWithException(Exception e, String expectedMessage, String expectedCauseMessage) {
        Assert.assertTrue(e instanceof TransactionException);
        Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        Assert.assertTrue(e.getMessage().startsWith(expectedMessage));
        Assert.assertThat(e.getCause().getMessage(), new StringContains(expectedCauseMessage));
    }

    @Override
    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/corrupt_metadata");
        kylinConfig = getTestConfig();
    }
}
