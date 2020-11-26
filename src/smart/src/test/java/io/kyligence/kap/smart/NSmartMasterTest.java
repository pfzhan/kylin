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

package io.kyligence.kap.smart;

import static io.kyligence.kap.smart.model.GreedyModelTreesBuilderTest.smartUtHook;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class NSmartMasterTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testDoProposeWhenPushDownIsDisabled() {
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), proj);
        // the project does not contain any realization
        Assert.assertEquals(0, modelManager.listAllModels().size());
        NDataModel model1, model2;
        {
            String[] sqlStatements = new String[] {
                    "SELECT f.leaf_categ_id FROM kylin_sales f left join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN'" };
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqlStatements);
            NSmartMaster smartMaster = new NSmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            model1 = modelContext.getTargetModel();
        }
        Assert.assertEquals(1, modelManager.listAllModels().size());
        Assert.assertNotNull(modelManager.getDataModelDesc(model1.getId()));

        {
            String[] sqlStatements = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                    + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                    + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                    + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqlStatements);
            NSmartMaster smartMaster = new NSmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            model2 = modelContext.getTargetModel();
        }
        Assert.assertEquals(3, modelManager.listAllModels().size());
        Assert.assertNotNull(modelManager.getDataModelDesc(model2.getId()));
    }

    @Test
    public void testRenameModel() {
        NDataModel model1, model2, model3;
        {
            String[] sqlStatements = new String[] {
                    "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" };
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqlStatements);
            NSmartMaster smartMaster = new NSmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            model1 = modelContext.getTargetModel();
        }
        {
            String[] sqlStatements = new String[] {
                    "SELECT f.leaf_categ_id FROM kylin_sales f left join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN'" };
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqlStatements);
            NSmartMaster smartMaster = new NSmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            model2 = modelContext.getTargetModel();
        }
        String model1Alias = model1.getAlias();
        String model2Alias = model2.getAlias();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_1", model1Alias);
        Assert.assertEquals(model1Alias, model2Alias);
        {
            String[] sqlStatements = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                    + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                    + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                    + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
            val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqlStatements);
            NSmartMaster smartMaster = new NSmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            model3 = modelContext.getTargetModel();
        }
        String model3Alias = model3.getAlias();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_2", model3Alias);
    }

    @Test
    public void testLoadingModelCannotOffline() {
        String[] sqls = { "select item_count, sum(price) from kylin_sales group by item_count" };
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);

        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        // set model maintain type to semi-auto
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), proj);

        // update existing model to offline
        NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(getTestConfig(), proj);
        NDataflow dataflow = dataflowMgr.getDataflow(targetModel.getUuid());
        NDataflowUpdate copiedDataFlow = new NDataflowUpdate(dataflow.getUuid());
        copiedDataFlow.setStatus(RealizationStatusEnum.OFFLINE);
        dataflowMgr.updateDataflow(copiedDataFlow);

        // propose in semi-auto-mode
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), proj, sqls);
        smartMaster = new NSmartMaster(context2);
        smartMaster.executePropose();

        String expectedPendingMsg = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertEquals(expectedPendingMsg, accelerateInfo.getPendingMsg());
    }

    @Test
    public void testCountDistinctTwoParamColumn() {
        /*
         * case 1:
         */
        String[] sqls = new String[] { "SELECT part_dt, SUM(price) AS GMV, COUNT(1) AS TRANS_CNT,\n"
                + "COUNT(DISTINCT lstg_format_name), COUNT(DISTINCT seller_id, lstg_format_name) AS DIST_SELLER_FORMAT\n"
                + "FROM kylin_sales GROUP BY part_dt" };

        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext ctx = smartMaster.getContext();
            final List<NDataModel.Measure> allMeasures = ctx.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(4, allMeasures.size());

            final NDataModel.Measure measure3 = allMeasures.get(2);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_SALES_LSTG_FORMAT_NAME", measure3.getName());
            Assert.assertEquals(1, measure3.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure3.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure4 = allMeasures.get(3);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_SALES_SELLER_ID_KYLIN_SALES_LSTG_FORMAT_NAME",
                    measure4.getName());
            Assert.assertEquals(2, measure4.getFunction().getParameterCount());
            Assert.assertEquals("hllc", measure4.getFunction().getReturnDataType().getName());
        }

        /*
         * case 2:
         */
        sqls = new String[] { "SELECT COUNT(DISTINCT META_CATEG_NAME) AS CNT, MAX(META_CATEG_NAME) AS max_name\n"
                + "FROM kylin_category_groupings" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        smartMaster = new NSmartMaster(context2);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext ctx = smartMaster.getContext();
            final List<NDataModel.Measure> allMeasures = ctx.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(3, allMeasures.size());

            final NDataModel.Measure measure1 = allMeasures.get(1);
            Assert.assertEquals("COUNT_DISTINCT_KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME", measure1.getName());
            Assert.assertEquals(1, measure1.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure1.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure2 = allMeasures.get(2);
            Assert.assertEquals("MAX_KYLIN_CATEGORY_GROUPINGS_META_CATEG_NAME", measure2.getName());
            Assert.assertEquals(1, measure2.getFunction().getParameterCount());
            Assert.assertEquals("varchar", measure2.getFunction().getReturnDataType().getName());
        }
    }

    @Test
    public void testOneSqlToManyLayouts() {
        String[] sqls = new String[] { "select a.*, kylin_sales.lstg_format_name as lstg_format_name \n"
                + "from ( select part_dt, sum(price) as sum_price from kylin_sales\n"
                + "         where part_dt > '2010-01-01' group by part_dt) a \n"
                + "join kylin_sales on a.part_dt = kylin_sales.part_dt \n"
                + "group by lstg_format_name, a.part_dt, a.sum_price" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(1, accelerateInfoMap.size());
        Set<AccelerateInfo.QueryLayoutRelation> relatedLayouts = accelerateInfoMap.get(sqls[0]).getRelatedLayouts();
        Assert.assertEquals(2, relatedLayouts.size());
    }

    @Test
    public void testMaintainModelTypeWithNoInitialModel() {
        // set to manual model type
        AccelerationContextUtil.transferProjectToPureExpertMode(getTestConfig(), proj);

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), lstg_format_name, sum(price) from kylin_sales \n"
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name"

        };
        val context = AccelerationContextUtil.newModelReuseContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        final AbstractContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        // null validation
        final NDataModel originalModel = modelContext.getOriginModel();
        final IndexPlan originalIndexPlan = modelContext.getOriginIndexPlan();
        Assert.assertNull(originalModel);
        Assert.assertNull(originalIndexPlan);

        final NDataModel targetModel = modelContext.getTargetModel();
        final IndexPlan targetIndexPlan = modelContext.getTargetIndexPlan();
        Assert.assertNull(targetModel);
        Assert.assertNull(targetIndexPlan);
    }

    @Test
    public void testManualMaintainType() {
        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        final List<NDataModel.Measure> allMeasures = targetModel.getAllMeasures();
        final List<NDataModel.NamedColumn> allNamedColumns = targetModel.getAllNamedColumns();
        final List<JoinTableDesc> joinTables = targetModel.getJoinTables();
        Assert.assertTrue(CollectionUtils.isEmpty(joinTables));

        // set maintain model type to pure-expert-mode
        AccelerationContextUtil.transferProjectToPureExpertMode(getTestConfig(), proj);

        {
            // -------------- add extra table -----------------------
            sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                    + " left join kylin_cal_dt on cal_dt = part_dt \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            val context1 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), proj, sqls);
            smartMaster = new NSmartMaster(context1);
            smartMaster.runUtWithContext(smartUtHook);

            final Map<String, AccelerateInfo> accelerateInfoMapCase3 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase3.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
            final String pendingMsg3 = accelerateInfoMapCase3.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg3);
            Assert.assertEquals(expectedMessage, pendingMsg3);

            final NDataModel targetModelCase3 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNull(targetModelCase3);
        }

        // set maintain model type to semi-auto-mode
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), proj);
        {
            // -------------- add extra table -----------------------
            sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                    + " left join kylin_cal_dt on cal_dt = part_dt \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), proj, sqls);
            smartMaster = new NSmartMaster(context2);
            smartMaster.runUtWithContext(smartUtHook);

            final Map<String, AccelerateInfo> accelerateInfoMapCase3 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase3.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
            final String pendingMsg3 = accelerateInfoMapCase3.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg3);
            Assert.assertEquals(expectedMessage, pendingMsg3);

            final NDataModel targetModelCase3 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNull(targetModelCase3);
        }
    }

    @Test
    public void testInitTargetModelError() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);

        smartMaster.analyzeSQLs();

        // after cutting context, change "DEFAULT.KYLIN_CAL_DT" to be a incremental load table
        val tableManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        val table = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        table.setIncrementLoading(true);
        tableManager.updateTableDesc(table);

        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.selectIndexPlan();
        smartMaster.optimizeIndexPlan();
        context.saveMetadata();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        final AccelerateInfo accelerateInfo = Lists.newArrayList(accelerateInfoMap.values()).get(0);
        Assert.assertTrue(accelerateInfo.isFailed());
        Assert.assertEquals("Only one incremental loading table can be set in model!",
                accelerateInfo.getFailedCause().getMessage());
    }

    @Test
    public void testWithoutSaveModel() {
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.executePropose();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        Assert.assertFalse(Lists.newArrayList(accelerateInfoMap.values()).get(0).isFailed());
    }

    @Test
    public void testProposeOnExistingRuleBasedIndexPlan() {
        String[] sqls = new String[] { "select CAL_DT from KYLIN_CAL_DT group by CAL_DT limit 10" };

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "rule_based", sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        final List<AbstractContext.NModelContext> modelContexts = context.getModelContexts();
        final AbstractContext.NModelContext modelContext = modelContexts.get(0);
        val originalAllIndexesMap = modelContext.getOriginIndexPlan().getAllIndexesMap();
        val originalWhiteListIndexesMap = modelContext.getOriginIndexPlan().getWhiteListIndexesMap();
        Assert.assertEquals(1, originalAllIndexesMap.size());
        Assert.assertEquals(0, originalWhiteListIndexesMap.size());

        val targetAllIndexesMap = modelContext.getTargetIndexPlan().getAllIndexesMap();
        Assert.assertEquals(originalAllIndexesMap, targetAllIndexesMap);
        final Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();

        final AccelerateInfo accelerateInfo = accelerateInfoMap.get(sqls[0]);
        Assert.assertFalse(accelerateInfo.isFailed());
    }

    @Test
    public void testRenameAllColumns() {
        // test all named columns rename
        String[] sqlStatements = new String[] { "SELECT\n"
                + "BUYER_ACCOUNT.ACCOUNT_COUNTRY, SELLER_ACCOUNT.ACCOUNT_COUNTRY "
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT \n" + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n"
                + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n"
                + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n"
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n"
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n"
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n"
                + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n"
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sqlStatements);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        String[] namedColumns = model.getAllNamedColumns().stream().map(NDataModel.NamedColumn::getAliasDotColumn)
                .sorted().toArray(String[]::new);

        String namedColumn1 = namedColumns[2];
        String namedColumn2 = namedColumns[7];
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_COUNTRY", namedColumn1);
        Assert.assertEquals("TEST_ACCOUNT_1.ACCOUNT_COUNTRY", namedColumn2);
    }

    @Test
    public void testProposeShortNameOfDimensionAndMeasureAreSame() {
        String[] sqls = new String[] {
                "select test_account.account_id, test_account1.account_id, count(test_account.account_country) \n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id, test_account1.account_id",
                "select test_account.account_id, count(test_account.account_country), count(test_account1.account_country)\n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id",
                "select test_kylin_fact.cal_dt, test_cal_dt.cal_dt, count(price) \n"
                        + "from test_kylin_fact inner join edw.test_cal_dt\n"
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                        + "group by test_kylin_fact.cal_dt, test_cal_dt.cal_dt" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sqls);
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerationInfoMap.forEach((key, value) -> Assert.assertFalse(value.isNotSucceed()));
        List<AbstractContext.NModelContext> contexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(2, contexts.size());
        NDataModel model1 = contexts.get(0).getTargetModel();
        List<NDataModel.NamedColumn> dimensionList1 = model1.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        Assert.assertEquals("TEST_CAL_DT_0_DOT_0_CAL_DT", dimensionList1.get(0).getName());
        Assert.assertEquals("TEST_CAL_DT.CAL_DT", dimensionList1.get(0).getAliasDotColumn());
        Assert.assertEquals("TEST_KYLIN_FACT_0_DOT_0_CAL_DT", dimensionList1.get(1).getName());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", dimensionList1.get(1).getAliasDotColumn());
        List<NDataModel.Measure> measureList1 = model1.getAllMeasures();
        Assert.assertEquals("COUNT_ALL", measureList1.get(0).getName());
        Assert.assertEquals("COUNT_TEST_KYLIN_FACT_PRICE", measureList1.get(1).getName());

        NDataModel model2 = contexts.get(1).getTargetModel();
        List<NDataModel.NamedColumn> dimensionList2 = model2.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        Assert.assertEquals("TEST_ACCOUNT_0_DOT_0_ACCOUNT_ID", dimensionList2.get(0).getName());
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_ID", dimensionList2.get(0).getAliasDotColumn());
        Assert.assertEquals("TEST_ACCOUNT1_0_DOT_0_ACCOUNT_ID", dimensionList2.get(1).getName());
        Assert.assertEquals("TEST_ACCOUNT1.ACCOUNT_ID", dimensionList2.get(1).getAliasDotColumn());
        List<NDataModel.Measure> measureList2 = model2.getAllMeasures();
        Assert.assertEquals("COUNT_ALL", measureList2.get(0).getName());
        Assert.assertEquals("COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY", measureList2.get(1).getName());
        Assert.assertEquals("COUNT_TEST_ACCOUNT1_ACCOUNT_COUNTRY", measureList2.get(2).getName());

        // mock error case for dimension and this case should be auto-corrected when inherited from existed model
        dimensionList1.get(0).setName("TEST_KYLIN_FACT_CAL_DT");
        NDataModelManager.getInstance(getTestConfig(), "newten").updateDataModelDesc(model1);
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", new String[] { sqls[2] });
        smartMaster = new NSmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);
        accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[2]).isFailed());

        // mock error case for measure
        measureList2.get(2).setName("COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY");
        NDataModelManager.getInstance(getTestConfig(), "newten").updateDataModelDesc(model2);
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", new String[] { sqls[1] });
        smartMaster = new NSmartMaster(context2);
        smartMaster.runUtWithContext(smartUtHook);
        accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerationInfoMap.get(sqls[1]).isFailed());
        Assert.assertEquals("Duplicate measure name occurs: COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY",
                accelerationInfoMap.get(sqls[1]).getFailedCause().getMessage());
    }

    @Test
    public void testProposeSelectStatementStartsWithParentheses() {
        String sql = "(((SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" <> 'A')) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" = 'A'))) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" > 'A'))) "
                + "UNION ALL SELECT SUM(\"PRICE\") FROM \"TEST_KYLIN_FACT\" WHERE ((\"LSTG_FORMAT_NAME\" = 'A') AND (\"LSTG_SITE_ID\" IN ('A'))))\n";

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", new String[] { sql });
        NSmartMaster smartMaster = new NSmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        Map<String, AccelerateInfo> accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sql).isNotSucceed());
        List<AbstractContext.NModelContext> contexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, contexts.size());
        final NDataModel targetModel = contexts.get(0).getTargetModel();
        final List<NDataModel.NamedColumn> dimensions = targetModel.getAllNamedColumns().stream()
                .filter(NDataModel.NamedColumn::isDimension).collect(Collectors.toList());
        final List<NDataModel.Measure> allMeasures = targetModel.getAllMeasures();
        Assert.assertEquals(2, dimensions.size());
        Assert.assertEquals(2, allMeasures.size());
    }

    @Test
    public void testProposeNewModel_InSemiMode() {

        // set maintain model type to manual
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");

        // normal case 1. create totally new model and index (without rule-based-index), check the model and index_plan
        String[] sqls = new String[] {
                "select test_account.account_id, test_account1.account_id, count(test_account.account_country) \n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id, test_account1.account_id" };
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten", sqls);
        NSmartMaster smartMaster1 = new NSmartMaster(context1);
        smartMaster1.runSuggestModel();
        context1.saveMetadata();
        AccelerationContextUtil.onlineModel(context1);
        Assert.assertFalse(smartMaster1.getContext().getModelContexts().get(0).isTargetModelMissing());
        val newModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, newModel.getJoinTables().size());
        Assert.assertEquals("TEST_ACCOUNT", newModel.getRootFactTable().getTableName());
        Assert.assertEquals(2, newModel.getAllMeasures().size());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(0).getFunction().getExpression());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(1).getFunction().getExpression());
        Assert.assertEquals(10, newModel.getAllNamedColumns().size());
        Assert.assertEquals(2, newModel.getEffectiveDimensions().size());
        val index_plan = smartMaster1.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, index_plan.getAllIndexes().size());
        Assert.assertEquals(0, index_plan.getRuleBaseLayouts().size());

        // case 2. repropose new model and won't reuse the origin model, check the models' info is equaled with origin model
        val context2 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten", sqls);
        NSmartMaster smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();
        val reproposalModel = smartMaster2.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertNotEquals(reproposalModel.getId(), newModel.getId());
        Assert.assertTrue(reproposalModel.getJoinsGraph().match(newModel.getJoinsGraph(), Maps.newHashMap(), false));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllMeasures(), newModel.getAllMeasures()));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllNamedColumns(), newModel.getAllNamedColumns()));
        val reproposalIndexPlan = smartMaster2.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(index_plan.getAllLayouts(), reproposalIndexPlan.getAllLayouts()));

        // case 3. reuse the origin model
        val context3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        NSmartMaster smartMaster3 = new NSmartMaster(context3);
        smartMaster3.executePropose();
        val model3 = smartMaster3.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertTrue(model3.getId().equals(reproposalModel.getId()) || model3.getId().equals(newModel.getId()));

        // case 4. reuse origin model and generate recommendation
        String[] secondSqls = new String[] { "select max(test_account1.ACCOUNT_CONTACT) from "
                + "\"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 "
                + "on test_account.account_id = test_account1.account_id" };
        val context4 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", secondSqls);
        NSmartMaster smartMaster4 = new NSmartMaster(context4);
        smartMaster4.runSuggestModel();
        List<AbstractContext.NModelContext> modelContexts = context4.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        AbstractContext.NModelContext modelContext = modelContexts.get(0);
        val reusedModel = modelContext.getTargetModel();
        Assert.assertTrue(
                reusedModel.getId().equals(reproposalModel.getId()) || reusedModel.getId().equals(newModel.getId()));
        Assert.assertEquals(0, modelContext.getCcRecItemMap().size());
        Assert.assertEquals(1, modelContext.getIndexRexItemMap().size());
        Assert.assertEquals(1, modelContext.getMeasureRecItemMap().size());

        // case 5. create a new model and without recommendation
        String[] thirdSqls = new String[] { "select test_kylin_fact.cal_dt, test_cal_dt.cal_dt, sum(price) \n"
                + "from test_kylin_fact inner join edw.test_cal_dt\n"
                + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "group by test_kylin_fact.cal_dt, test_cal_dt.cal_dt" };
        val context5 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten", thirdSqls);
        NSmartMaster smartMaster5 = new NSmartMaster(context5);
        smartMaster5.runSuggestModel();
        List<AbstractContext.NModelContext> modelContexts5 = context5.getModelContexts();
        Assert.assertEquals(1, modelContexts5.size());
        AbstractContext.NModelContext modelContext5 = modelContexts5.get(0);
        Assert.assertTrue(modelContext5.getCcRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getDimensionRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getMeasureRecItemMap().isEmpty());
        Assert.assertTrue(modelContext5.getIndexRexItemMap().isEmpty());
        Assert.assertFalse(modelContext5.isTargetModelMissing());
        Assert.assertEquals(1, modelContext5.getTargetModel().getJoinTables().size());
    }
}
