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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import lombok.val;

public class NSmartMasterTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testRenameModel() {
        NDataModel model1, model2, model3;
        {
            String[] sqlStatements = new String[] {
                    "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" };
            model1 = proposeModel(sqlStatements);
        }
        {
            String[] sqlStatements = new String[] {
                    "SELECT f.leaf_categ_id FROM kylin_sales f left join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN'" };
            model2 = proposeModel(sqlStatements);
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
            model3 = proposeModel(sqlStatements);
        }
        String model3Alias = model3.getAlias();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_2", model3Alias);
    }

    @Test
    public void testLoadingModelCannotOffline() {
        String[] sqls = { "select item_count, sum(price) from kylin_sales group by item_count" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        // set model maintain type to semi-auto
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(proj));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "true");

        // update existing model to offline
        NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(getTestConfig(), proj);
        NDataflow dataflow = dataflowMgr.getDataflow(targetModel.getUuid());
        NDataflowUpdate copiedDataFlow = new NDataflowUpdate(dataflow.getUuid());
        copiedDataFlow.setStatus(RealizationStatusEnum.OFFLINE);
        dataflowMgr.updateDataflow(copiedDataFlow);

        // propose in semi-auto-mode
        smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();

        String expectedPendingMsg = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]);
        Assert.assertTrue(accelerateInfo.isPending());
        Assert.assertEquals(expectedPendingMsg, accelerateInfo.getPendingMsg());
    }

    @Test
    public void testSaveAccelerateInfo() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.analyzeSQLs();
        Assert.assertEquals(5, smartMaster.getContext().getAccelerateInfoMap().size());
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : smartMaster.getContext().getAccelerateInfoMap()
                .entrySet()) {
            Assert.assertFalse(accelerateInfoEntry.getValue().isFailed());
        }
        smartMaster.selectAndOptimize();
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();

        final NSmartContext ctx = smartMaster.getContext();
        final Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(5, accelerateInfoMap.size());

        // before saveAccelerateInfo
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = indexPlan.getAllIndexes();
        final List<LayoutEntity> layouts = collectAllLayouts(allCuboids);
        Set<FavoriteQueryRealization> fqRealizationsBefore = collectFQR(layouts);
        Assert.assertTrue(fqRealizationsBefore.isEmpty());

        // do save accelerateInfo
        smartMaster.saveAccelerateInfo();

        // after saveAccelerateInfo
        Set<FavoriteQueryRealization> fqRealizationsAfter = collectFQR(layouts);
        Assert.assertEquals(5, fqRealizationsAfter.size());
    }

    @Test
    public void testCountDistinctTwoParamColumn() {
        KylinConfig kylinConfig = getTestConfig();
        /*
         * case 1:
         */
        String[] sqls = new String[] { "SELECT part_dt, SUM(price) AS GMV, COUNT(1) AS TRANS_CNT,\n"
                + "COUNT(DISTINCT lstg_format_name), COUNT(DISTINCT seller_id, lstg_format_name) AS DIST_SELLER_FORMAT\n"
                + "FROM kylin_sales GROUP BY part_dt" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        {
            final NSmartContext ctx = smartMaster.getContext();
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
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        {
            final NSmartContext ctx = smartMaster.getContext();
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
    public void testSaveAccelerateInfoOfOneSqlToManyLayouts() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select a.*, kylin_sales.lstg_format_name as lstg_format_name \n"
                + "from ( select part_dt, sum(price) as sum_price from kylin_sales\n"
                + "         where part_dt > '2010-01-01' group by part_dt) a \n"
                + "join kylin_sales on a.part_dt = kylin_sales.part_dt \n"
                + "group by lstg_format_name, a.part_dt, a.sum_price" };
        String draftVersion = UUID.randomUUID().toString();
        initFQData(sqls);
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.runAll();

        final NSmartContext ctx = smartMaster.getContext();
        final Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(1, accelerateInfoMap.size());

        // get favorite query realization relationships from database and validate them
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = indexPlan.getAllIndexes();
        final List<LayoutEntity> layouts = collectAllLayouts(allCuboids);
        val fqRealizationsAfter = collectFQR(layouts);
        Assert.assertEquals(2, fqRealizationsAfter.size());
    }

    @Test
    public void testMaintainModelTypeWithNoInitialModel() {
        KylinConfig kylinConfig = getTestConfig();
        // set to manual model type
        final NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        final ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(proj));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), lstg_format_name, sum(price) from kylin_sales \n"
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name"

        };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
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
    public void testMaintainModelType() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        final List<NDataModel.Measure> allMeasures = targetModel.getAllMeasures();
        final List<NDataModel.NamedColumn> allNamedColumns = targetModel.getAllNamedColumns();
        final List<JoinTableDesc> joinTables = targetModel.getJoinTables();
        Assert.assertTrue(CollectionUtils.isEmpty(joinTables));

        // set maintain model type to manual
        final NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        final ProjectInstance projectUpdate = projectManager.copyForWrite(projectManager.getProject(proj));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);

        // -------------- case 0: add extra measure(success case) -----------------------
        {
            sqls = new String[] {
                    "select part_dt, lstg_format_name, sum(price), count(distinct lstg_format_name) from kylin_sales \n"
                            + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
            smartMaster.runAll();
            final Map<String, AccelerateInfo> accelerateInfoMapCase0 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(1, accelerateInfoMapCase0.get(sqls[0]).getRelatedLayouts().size());

            final Throwable blockingCause0 = accelerateInfoMapCase0.get(sqls[0]).getFailedCause();
            Assert.assertNull(blockingCause0);

            final List<IndexEntity> allCuboids = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan()
                    .getAllIndexes();
            final List<LayoutEntity> layouts = collectAllLayouts(allCuboids);
            Assert.assertEquals(1, layouts.size());

            final NDataModel targetModelCase0 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            final List<NDataModel.Measure> allMeasuresCase0 = targetModelCase0.getAllMeasures();
            final List<NDataModel.NamedColumn> allNamedColumnsCase0 = targetModelCase0.getAllNamedColumns();
            final List<JoinTableDesc> joinTablesCase0 = targetModel.getJoinTables();
            Assert.assertEquals("measures changed unexpected", allMeasures, allMeasuresCase0);
            Assert.assertEquals("named columns changed unexpected", allNamedColumns, allNamedColumnsCase0);
            Assert.assertEquals("join tables changed unexpected", joinTables, joinTablesCase0);
        }

        // -------------- case 1: add extra measure -----------------------
        {
            sqls = new String[] {
                    "select part_dt, lstg_format_name, sum(price), count(distinct price) from kylin_sales \n"
                            + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
            smartMaster.runAll();
            final Map<String, AccelerateInfo> accelerateInfoMapCase1 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase1.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "The model [AUTO_MODEL_KYLIN_SALES_1] matches this query, "
                    + "but the measure [COUNT_DISTINCT([DEFAULT.KYLIN_SALES.PRICE])] is missing. "
                    + "Please add the above measure before attempting to accelerate this query.";
            final String pendingMsg1 = accelerateInfoMapCase1.get(sqls[0]).getPendingMsg();
            Assert.assertEquals(expectedMessage, pendingMsg1);

            final List<IndexEntity> allCuboids = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan()
                    .getAllIndexes();
            final List<LayoutEntity> layouts = collectAllLayouts(allCuboids);
            Assert.assertEquals(1, layouts.size());

            final NDataModel targetModelCase1 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            final List<NDataModel.Measure> allMeasuresCase1 = targetModelCase1.getAllMeasures();
            final List<NDataModel.NamedColumn> allNamedColumnsCase1 = targetModelCase1.getAllNamedColumns();
            final List<JoinTableDesc> joinTablesCase1 = targetModel.getJoinTables();
            Assert.assertEquals("measures changed unexpected", allMeasures, allMeasuresCase1);
            Assert.assertEquals("named columns changed unexpected", allNamedColumns, allNamedColumnsCase1);
            Assert.assertEquals("join tables changed unexpected", joinTables, joinTablesCase1);
        }

        // -------------- case 2: add extra column -----------------------
        {
            sqls = new String[] { "select part_dt, sum(item_count), lstg_format_name, sum(price) from kylin_sales \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
            smartMaster.runAll();

            final Map<String, AccelerateInfo> accelerateInfoMapCase2 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase2.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "The model [AUTO_MODEL_KYLIN_SALES_1] matches this query, "
                    + "but the measure [SUM([DEFAULT.KYLIN_SALES.ITEM_COUNT])] is missing. "
                    + "Please add the above measure before attempting to accelerate this query.";
            final String pendingMsg2 = accelerateInfoMapCase2.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg2);
            Assert.assertEquals(expectedMessage, pendingMsg2);

            final NDataModel targetModelCase2 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            final List<NDataModel.Measure> allMeasuresCase2 = targetModelCase2.getAllMeasures();
            final List<NDataModel.NamedColumn> allNamedColumnsCase2 = targetModelCase2.getAllNamedColumns();
            final List<JoinTableDesc> joinTablesCase2 = targetModel.getJoinTables();
            Assert.assertEquals("measures changed unexpected", allMeasures, allMeasuresCase2);
            Assert.assertEquals("named columns changed unexpected", allNamedColumns, allNamedColumnsCase2);
            Assert.assertEquals("join tables changed unexpected", joinTables, joinTablesCase2);
        }

        // -------------- case 3: add extra table -----------------------
        {
            sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                    + " left join kylin_cal_dt on cal_dt = part_dt \n"
                    + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
            smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
            smartMaster.runAll();

            final Map<String, AccelerateInfo> accelerateInfoMapCase3 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase3.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "No model matches the SQL. Please add a model matches the SQL before attempting to accelerate this query.";
            final String pendingMsg3 = accelerateInfoMapCase3.get(sqls[0]).getPendingMsg();
            Assert.assertNotNull(pendingMsg3);
            Assert.assertEquals(expectedMessage, pendingMsg3);

            final NDataModel targetModelCase3 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNull(targetModelCase3);
        }
        // -------------- case 4: add extra dimension -----------------------
        {
            sqls = new String[] { "select sum(price) from kylin_sales "
                    + "where part_dt = '2012-01-01' group by part_dt, seller_id" };
            smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
            smartMaster.runAll();

            final Map<String, AccelerateInfo> accelerateInfoMapCase4 = smartMaster.getContext().getAccelerateInfoMap();
            Assert.assertEquals(0, accelerateInfoMapCase4.get(sqls[0]).getRelatedLayouts().size());

            String expectedMessage = "The model [AUTO_MODEL_KYLIN_SALES_1] matches this query, but the dimension [KYLIN_SALES.SELLER_ID] is missing. Please add the above dimension before attempting to accelerate this query.";
            final String pendingMsg4 = accelerateInfoMapCase4.get(sqls[0]).getPendingMsg();
            Assert.assertEquals(expectedMessage, pendingMsg4);

            final NDataModel targetModelCase4 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            final List<NDataModel.Measure> allMeasuresCase4 = targetModelCase4.getAllMeasures();
            final List<NDataModel.NamedColumn> allNamedColumnsCase4 = targetModelCase4.getAllNamedColumns();
            final List<JoinTableDesc> joinTablesCase4 = targetModel.getJoinTables();
            Assert.assertEquals("measures changed unexpected", allMeasures, allMeasuresCase4);
            Assert.assertEquals("named columns changed unexpected", allNamedColumns, allNamedColumnsCase4);
            Assert.assertEquals("join tables changed unexpected", joinTables, joinTablesCase4);
        }
    }

    @Test
    public void testInitTargetModelError() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);

        smartMaster.analyzeSQLs();

        // after cutting context, change "DEFAULT.KYLIN_CAL_DT" to be a incremental load table
        val tableManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        val table = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        table.setIncrementLoading(true);
        tableManager.updateTableDesc(table);

        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.saveModel();
        smartMaster.selectIndexPlan();
        smartMaster.optimizeIndexPlan();
        smartMaster.saveIndexPlan();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        final AccelerateInfo accelerateInfo = Lists.newArrayList(accelerateInfoMap.values()).get(0);
        Assert.assertTrue(accelerateInfo.isFailed());
        Assert.assertEquals("Only one incremental loading table can be set in model!",
                accelerateInfo.getFailedCause().getMessage());
    }

    @Test
    public void testWithoutSaveModel() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.selectAndOptimize();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        Assert.assertFalse(Lists.newArrayList(accelerateInfoMap.values()).get(0).isFailed());
    }

    @Test
    public void testProposeOnExistingRuleBasedIndexPlan() {
        KylinConfig kylinConfig = getTestConfig();
        String[] sqls = new String[] { "select CAL_DT from KYLIN_CAL_DT group by CAL_DT limit 10" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, "rule_based", sqls);
        smartMaster.runAll();

        final NSmartContext context = smartMaster.getContext();
        final List<NSmartContext.NModelContext> modelContexts = context.getModelContexts();
        final NSmartContext.NModelContext modelContext = modelContexts.get(0);
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
        KylinConfig kylinConfig = getTestConfig();
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
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, "newten", sqlStatements);
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
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", sqls);
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerationInfoMap.forEach((key, value) -> {
            Assert.assertFalse(value.isNotSucceed());
        });
        List<NSmartContext.NModelContext> contexts = smartMaster.getContext().getModelContexts();
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
        smartMaster = new NSmartMaster(getTestConfig(), "newten", new String[] { sqls[2] });
        smartMaster.runAll();
        accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[2]).isFailed());

        // mock error case for measure
        measureList2.get(2).setName("COUNT_TEST_ACCOUNT_ACCOUNT_COUNTRY");
        NDataModelManager.getInstance(getTestConfig(), "newten").updateDataModelDesc(model2);
        smartMaster = new NSmartMaster(getTestConfig(), "newten", new String[] { sqls[1] });
        smartMaster.runAll();
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

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", new String[] { sql });
        smartMaster.runAll();
        Map<String, AccelerateInfo> accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sql).isNotSucceed());
        List<NSmartContext.NModelContext> contexts = smartMaster.getContext().getModelContexts();
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
        // normal case 1. create totally new model and index (without rule-based-index), check the model and index_plan
        String[] sqls = new String[] {
                "select test_account.account_id, test_account1.account_id, count(test_account.account_country) \n"
                        + "from \"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 on test_account.account_id = test_account1.account_id\n"
                        + "group by test_account.account_id, test_account1.account_id" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", sqls, false, true);
        smartMaster.recommendNewModelAndIndex();
        smartMaster.saveModelOnlyForTest();
        Assert.assertFalse(smartMaster.getContext().getModelContexts().get(0).withoutTargetModel());
        val newModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, newModel.getJoinTables().size());
        Assert.assertEquals("TEST_ACCOUNT", newModel.getRootFactTable().getTableName());
        Assert.assertEquals(2, newModel.getAllMeasures().size());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(0).getFunction().getExpression());
        Assert.assertEquals("COUNT", newModel.getAllMeasures().get(1).getFunction().getExpression());
        Assert.assertEquals(10, newModel.getAllNamedColumns().size());
        Assert.assertEquals(2, newModel.getEffectiveDimensions().size());
        val index_plan = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, index_plan.getAllIndexes().size());
        Assert.assertEquals(0, index_plan.getRuleBaseLayouts().size());

        // case 2. repropose new model and won't reuse the origin model, check the models' info is equaled with origin model
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), "newten", sqls, false, true);
        smartMaster1.recommendNewModelAndIndex();
        smartMaster1.saveModelOnlyForTest();
        val reproposalModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertNotEquals(reproposalModel.getId(), newModel.getId());
        Assert.assertTrue(reproposalModel.getJoinsGraph().match(newModel.getJoinsGraph(), Maps.newHashMap(), false));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllMeasures(), newModel.getAllMeasures()));
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(reproposalModel.getAllNamedColumns(), newModel.getAllNamedColumns()));
        val reproposalIndexPlan = smartMaster1.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertTrue(
                CollectionUtils.isEqualCollection(index_plan.getAllLayouts(), reproposalIndexPlan.getAllLayouts()));

        // case 3. reuse the origin model
        NSmartMaster smartMaster2 = new NSmartMaster(getTestConfig(), "newten", sqls);
        smartMaster2.selectAndOptimize();
        val model2 = smartMaster2.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertTrue(model2.getId().equals(reproposalModel.getId()) || model2.getId().equals(newModel.getId()));

        // case 4. reuse origin model and generate recommendation
        String[] scondSqls = new String[] { "select max(test_account1.ACCOUNT_CONTACT) from "
                + "\"DEFAULT\".test_account inner join \"DEFAULT\".test_account1 "
                + "on test_account.account_id = test_account1.account_id" };
        NSmartMaster smartMaster3 = new NSmartMaster(getTestConfig(), "newten", scondSqls);
        val modelAndRecMap = smartMaster3.selectAndGenRecommendation();
        Assert.assertEquals(1, modelAndRecMap.size());
        val reusedModel = modelAndRecMap.keySet().iterator().next();
        Assert.assertTrue(
                reusedModel.getId().equals(reproposalModel.getId()) || reusedModel.getId().equals(newModel.getId()));
        Assert.assertEquals(0, modelAndRecMap.get(reusedModel).getCcRecommendations().size());
        Assert.assertEquals(1, modelAndRecMap.get(reusedModel).getLayoutRecommendations().size());
        Assert.assertEquals(1, modelAndRecMap.get(reusedModel).getMeasureRecommendations().size());

        // case 5. create a new model and without recommendation
        String[] thirdSqls = new String[] { "select test_kylin_fact.cal_dt, test_cal_dt.cal_dt, sum(price) \n"
                + "from test_kylin_fact inner join edw.test_cal_dt\n"
                + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "group by test_kylin_fact.cal_dt, test_cal_dt.cal_dt" };
        NSmartMaster smartMaster4 = new NSmartMaster(getTestConfig(), "newten", thirdSqls);
        val modelAndRecMap4 = smartMaster4.selectAndGenRecommendation();
        Assert.assertEquals(0, modelAndRecMap4.size());
        Assert.assertFalse(smartMaster4.getContext().getModelContexts().get(0).withoutTargetModel());
        val model4 = smartMaster4.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model4.getJoinTables().size());
    }

    private NDataModel proposeModel(String[] sqlStatements) {
        KylinConfig kylinConfig = getTestConfig();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqlStatements);
        smartMaster.selectAndOptimize();
        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext modelContext = ctx.getModelContexts().get(0);
        smartMaster.renameModel();
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();
        return modelContext.getTargetModel();
    }
}
