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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.calcite.sql.SqlSelect;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.query.util.QueryAliasMatcher;
import io.kyligence.kap.smart.model.ModelTree;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;
import lombok.var;

public class ModelSelectProposerTest extends NLocalWithSparkSessionTest {

    private static final String[] sqls = new String[] {
            "select order_id, count(*) from test_order group by order_id limit 1",
            "select cal_dt, count(*) from edw.test_cal_dt group by cal_dt limit 1",
            "SELECT count(*) \n" + "FROM \n" + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n"
                    + "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                    + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                    + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\" group by test_kylin_fact.cal_dt" };

    @Test
    public void testInnerJoinProposerInAutoMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);

        // in smartMode, proposer A inner join B, will not reuse origin A inner join B inner join C model
        // so it will proposer a new model that A inner join B.
        String[] sql2 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " };
        SmartMaster smartMaster2 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);
        List<NDataModel> recommendedModels = smartMaster2.context.getProposedModels();
        Assert.assertEquals(1, recommendedModels.size());
        Assert.assertEquals(1, recommendedModels.get(0).getJoinTables().size());
        Assert.assertEquals("TEST_KYLIN_FACT", recommendedModels.get(0).getRootFactTable().getAlias());
        Assert.assertEquals("DEFAULT.TEST_ORDER", recommendedModels.get(0).getJoinTables().get(0).getTable());
    }

    @Test
    public void testInnerJoinProposerInSemiAutoMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);

        // in semiAutoMode, model(A inner join B), will reuse model(A inner join B inner join C)
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");

        String[] sql2 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " };
        SmartMaster smartMaster2 = new SmartMaster(
                AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);
        List<NDataModel> recommendedModels = smartMaster2.context.getProposedModels();
        Assert.assertEquals(1, recommendedModels.size());
        Assert.assertEquals(2, recommendedModels.get(0).getJoinTables().size());
        Assert.assertEquals("TEST_KYLIN_FACT", recommendedModels.get(0).getRootFactTable().getAlias());
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", recommendedModels.get(0).getJoinTables().get(0).getTable());
        Assert.assertEquals("DEFAULT.TEST_ORDER", recommendedModels.get(0).getJoinTables().get(1).getTable());
    }

    @Test
    public void testInnerJoinProposerInExpertMode() {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");
        // proposer and save A inner join B inner join C
        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);

        // in expertMode, proposer A inner join B, will reuse origin A inner join B inner join C model
        AccelerationContextUtil.transferProjectToPureExpertMode(getTestConfig(), "newten");

        String[] sql2 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " };
        SmartMaster smartMaster2 = new SmartMaster(
                AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);
        List<NDataModel> recommendedModels = smartMaster2.context.getProposedModels();
        Assert.assertEquals(1, recommendedModels.size());
        Assert.assertEquals(2, recommendedModels.get(0).getJoinTables().size());
        Assert.assertEquals("TEST_KYLIN_FACT", recommendedModels.get(0).getRootFactTable().getAlias());
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", recommendedModels.get(0).getJoinTables().get(0).getTable());
        Assert.assertEquals("DEFAULT.TEST_ORDER", recommendedModels.get(0).getJoinTables().get(1).getTable());
    }

    @Test
    public void testMatchModelTreeWithInnerJoin() {

        // A inner join B inner join C
        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        // A inner join B
        String[] sql2 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " };
        SmartMaster smartMaster2 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);
        ModelTree modelTree = smartMaster2.getContext().getModelContexts().get(0).getModelTree();

        // A <-> B share same subGraph with A <-> B <-> C
        Assert.assertFalse(modelTree.hasSameSubGraph(dataModel));

        // A <-> B can match A <-> B <-> C in expertMode and semiAutoMode, when enable partial match
        Assert.assertTrue(modelTree.isExactlyMatch(dataModel, true, true));

        // A <-> B can't match A <-> B <-> C in expertMode and semiAutoMode, when disable partial match
        Assert.assertFalse(modelTree.isExactlyMatch(dataModel, false, false));
    }

    @Test
    public void testMatchModelTreeWithLeftJoin() {
        // A inner join B inner join C
        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "left join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        // A inner join B
        String[] sql2 = new String[] { "select * from test_kylin_fact "
                + "left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID " };
        SmartMaster smartMaster2 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);
        ModelTree modelTree = smartMaster2.getContext().getModelContexts().get(0).getModelTree();

        // [A -> B] can match with [A -> B && A -> C]
        Assert.assertTrue(modelTree.hasSameSubGraph(dataModel));

        // [A -> B] can always exactly match with [A -> B && A -> C]
        Assert.assertTrue(modelTree.isExactlyMatch(dataModel, false, false));
        Assert.assertTrue(modelTree.isExactlyMatch(dataModel, true, true));
    }

    @Test
    public void testInnerJoinCCReplaceError() throws Exception {
        overwriteSystemProp("kylin.query.match-partial-inner-join-model", "true");

        String[] sql1 = new String[] { "select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID "
                + "inner join TEST_ACCOUNT on test_kylin_fact.ITEM_COUNT = TEST_ACCOUNT.ACCOUNT_ID" };
        SmartMaster smartMaster1 = new SmartMaster(
                AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);
        NDataModel dataModel = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();

        QueryAliasMatcher queryAliasMatcher = new QueryAliasMatcher("newten", "DEFAULT");
        SqlSelect sql2 = (SqlSelect) CalciteParser.parse("select * from test_kylin_fact "
                + "inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID ");

        // before fix, the match result is empty， when enable partial match
        Assert.assertFalse(queryAliasMatcher.match(dataModel, sql2).getAliasMapping().isEmpty());
    }

    @Test
    public void testSelectSnapshotInSemiMode() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(dataflow.getConfig(),
                dataflow.getProject());
        tableMetadataManager.getTableDesc("EDW.TEST_CAL_DT")
                .setLastSnapshotPath("default/table_snapshot/EDW.TEST_CAL_DT/a27a7f08-792a-4514-a5ec-3182ea5474cc");
        tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER")
                .setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");

        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        val modelSizeBeforeAcc = dfManager.listUnderliningDataModels().size();

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "default");

        val smartMaster = new SmartMaster(
                AccelerationContextUtil.newModelReuseContext(getTestConfig(), "default", sqls));
        smartMaster.runUtWithContext(smartUtHook);

        smartMaster.getContext().getAccelerateInfoMap()
                .forEach((sql, accInfo) -> Assert.assertFalse(accInfo.isNotSucceed()));
        smartMaster.getContext().getModelContexts().forEach(modelContext -> {
            val originalModel = modelContext.getOriginModel();
            val originalIndexPlan = indexPlanManager.getIndexPlan(originalModel.getUuid());
            val originalLayoutSize = originalIndexPlan.getAllLayouts().size();
            val targetModel = modelContext.getTargetModel();
            val targetIndexPlan = indexPlanManager.getIndexPlan(targetModel.getUuid());

            Assert.assertEquals(modelSizeBeforeAcc, dfManager.listUnderliningDataModels().size());
            Assert.assertEquals(originalModel.getUuid(), targetModel.getUuid());
            Assert.assertEquals(originalLayoutSize, targetIndexPlan.getAllLayouts().size());
            Assert.assertEquals(1, modelContext.getModelTree().getOlapContexts().size());
            val sql = modelContext.getModelTree().getOlapContexts().iterator().next().sql;
            if (sql.equalsIgnoreCase(sqls[0])) {
                Assert.assertTrue(modelContext.isSnapshotSelected());
                return;
            }

            if (sql.equalsIgnoreCase(sqls[1])) {
                Assert.assertTrue(modelContext.isSnapshotSelected());
                return;
            }

            if (sql.equalsIgnoreCase(sqls[2])) {
                Assert.assertFalse(modelContext.isSnapshotSelected());
            }
        });
    }

    @Test
    public void testSelectSnapshotInExpertMode() {
        // prepare table desc snapshot path
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(dataflow.getConfig(),
                dataflow.getProject());
        tableMetadataManager.getTableDesc("EDW.TEST_CAL_DT")
                .setLastSnapshotPath("default/table_snapshot/EDW.TEST_CAL_DT/a27a7f08-792a-4514-a5ec-3182ea5474cc");
        tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER")
                .setLastSnapshotPath("default/table_snapshot/DEFAULT.TEST_ORDER/fb283efd-36fb-43de-86dc-40cf39054f59");

        val dfManager = NDataflowManager.getInstance(getTestConfig(), "default");
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "default");
        val modelSizeBeforeAcc = dfManager.listUnderliningDataModels().size();

        AccelerationContextUtil.transferProjectToPureExpertMode(getTestConfig(), "default");

        val smartMaster = new SmartMaster(
                AccelerationContextUtil.newModelReuseContext(getTestConfig(), "default", sqls));
        smartMaster.runUtWithContext(smartUtHook);

        smartMaster.getContext().getAccelerateInfoMap()
                .forEach((sql, accInfo) -> Assert.assertFalse(accInfo.isNotSucceed()));
        smartMaster.getContext().getModelContexts().forEach(modelContext -> {
            val originalModel = modelContext.getOriginModel();
            val originalIndexPlan = indexPlanManager.getIndexPlan(originalModel.getUuid());
            val originalLayoutSize = originalIndexPlan.getAllLayouts().size();
            val targetModel = modelContext.getTargetModel();
            val targetIndexPlan = indexPlanManager.getIndexPlan(targetModel.getUuid());

            Assert.assertEquals(modelSizeBeforeAcc, dfManager.listUnderliningDataModels().size());
            Assert.assertEquals(originalModel.getUuid(), targetModel.getUuid());
            Assert.assertEquals(originalLayoutSize, targetIndexPlan.getAllLayouts().size());
            Assert.assertEquals(1, modelContext.getModelTree().getOlapContexts().size());
            val sql = modelContext.getModelTree().getOlapContexts().iterator().next().sql;
            if (sql.equalsIgnoreCase(sqls[0])) {
                Assert.assertTrue(modelContext.isSnapshotSelected());
                return;
            }

            if (sql.equalsIgnoreCase(sqls[1])) {
                Assert.assertTrue(modelContext.isSnapshotSelected());
                return;
            }

            if (sql.equalsIgnoreCase(sqls[2])) {
                Assert.assertFalse(modelContext.isSnapshotSelected());
            }
        });
    }

    @Test
    public void testSelectSnapshotInSmartMode() {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), "newten");
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "newten");
        Assert.assertEquals(0, dfManager.listUnderliningDataModels().size());

        val sql1 = new String[] {
                "select * from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id" };
        val sql2 = new String[] { "select count(*) from test_account group by account_id" };

        val smartMaster1 = new SmartMaster(AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1));
        smartMaster1.runUtWithContext(smartUtHook);

        Assert.assertEquals(1, dfManager.listUnderliningDataModels().size());
        val model = dfManager.listUnderliningDataModels().get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", model.getRootFactTableName());
        var indexPlan = indexPlanManager.getIndexPlan(model.getUuid());
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertTrue(indexPlan.getAllLayouts().get(0).getIndex().isTableIndex());

        val smartMaster2 = new SmartMaster(AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql2));
        smartMaster2.runUtWithContext(smartUtHook);

        Assert.assertEquals(2, dfManager.listUnderliningDataModels().size());
        val models = dfManager.getTableOrientedModelsUsingRootTable(
                NTableMetadataManager.getInstance(getTestConfig(), "newten").getTableDesc("DEFAULT.TEST_ACCOUNT"));
        Assert.assertEquals(1, models.size());
        indexPlan = indexPlanManager.getIndexPlan(models.get(0).getUuid());
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertFalse(indexPlan.getAllLayouts().get(0).getIndex().isTableIndex());
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_ID",
                models.get(0).getColRef(indexPlan.getAllLayouts().get(0).getColOrder().get(0)).getIdentity());
    }

    @Test
    public void testSplitModelContext() {
        String originSql1 = "SELECT LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN edw.test_cal_dt ON (test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt)";
        String originSql2 = "SELECT LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN test_account ON (seller_id = account_id)";

        val smartContext1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { originSql1 });
        runSuggestModelAndSave(smartContext1);

        val smartContext2 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { originSql2 });
        runSuggestModelAndSave(smartContext2);

        String sql1 = originSql1 + " limit 1";
        String sql2 = originSql1 + " limit 100";
        String sql3 = originSql2 + " limit 1";
        val smartContext3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten",
                new String[] { sql1, sql2, sql3 });
        runSuggestModelAndSave(smartContext3);
        Assert.assertEquals(2, smartContext3.getModelContexts().size());
    }

    @Test
    public void testTwoLeftJoinSQLs() {
        String originSql1 = "SELECT LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN edw.test_cal_dt ON (test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt)";
        String originSql2 = "SELECT LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN test_account ON (seller_id = account_id)";

        val smartContext1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { originSql1 });
        runSuggestModelAndSave(smartContext1);

        val smartContext2 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { originSql2 });
        runSuggestModelAndSave(smartContext2);

        // accelerate two left join sqls together
        String sql1 = "SELECT sum(price), LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN edw.test_cal_dt ON (test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt) group by LSTG_FORMAT_NAME";
        String sql2 = "SELECT sum(price), LSTG_FORMAT_NAME\n" + "FROM test_kylin_fact\n"
                + "LEFT JOIN test_account ON (seller_id = account_id) group by LSTG_FORMAT_NAME";

        val smartContext3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten",
                new String[] { sql1, sql2 });
        runSuggestModelAndSave(smartContext3);
        Assert.assertEquals(2, smartContext3.getModelContexts().size());
        val modelContext1 = smartContext3.getModelContexts().get(0);
        val modelContext2 = smartContext3.getModelContexts().get(1);
        Assert.assertNotNull(modelContext1.getTargetModel());
        Assert.assertNotNull(modelContext2.getTargetModel());
        val layouts1 = smartContext3.getAccelerateInfoMap().get(sql1).getRelatedLayouts();
        val layouts2 = smartContext3.getAccelerateInfoMap().get(sql2).getRelatedLayouts();
        Assert.assertEquals(1, layouts1.size());
        val layout1 = modelContext1.getTargetIndexPlan().getLayoutEntity(layouts1.iterator().next().getLayoutId());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", layout1.getColumns().get(0).getAliasDotName());
        Assert.assertEquals(1, layouts2.size());
        val layout2 = modelContext1.getTargetIndexPlan().getLayoutEntity(layouts1.iterator().next().getLayoutId());
        Assert.assertEquals("TEST_KYLIN_FACT.LSTG_FORMAT_NAME", layout2.getColumns().get(0).getAliasDotName());

    }

    @Test
    public void testModelOptRule() {
        String originSql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN edw.test_cal_dt ON (test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt)";
        String originSql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN test_account ON (seller_id = account_id)";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { originSql1 });
        runSuggestModelAndSave(context1);

        // propose join
        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten",
                new String[] { originSql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
    }

    @Test
    public void testModelOptRuleWorksOnCreateTime() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN edw.test_cal_dt ON (test_kylin_fact.cal_dt = edw.test_cal_dt.cal_dt)";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN test_account ON (seller_id = account_id)";
        String sql3 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON (TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT)"
                + "LEFT JOIN TEST_ACCOUNT ON (SELLER_ID = ACCOUNT_ID)";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(1, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        AbstractContext context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql2 });
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        // propose with model-opt-rule on new modified model
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        String uuidModel1 = context1.getModelContexts().get(0).getTargetModel().getUuid();
        String uuidModel2 = context2.getModelContexts().get(0).getTargetModel().getUuid();
        modelManager.updateDataModel(uuidModel1, copyForWrite -> copyForWrite.setCreateTime(30000));
        modelManager.updateDataModel(uuidModel2, copyForWrite -> copyForWrite.setCreateTime(20000));

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql3 });
        context3.setCanCreateNewModel(true);
        runSuggestModelAndSave(context3);
        Assert.assertEquals(1, context3.getModelContexts().size());
        AbstractContext.ModelContext modelContext3 = context3.getModelContexts().get(0);
        Assert.assertEquals(uuidModel1, modelContext3.getTargetModel().getUuid());
        Assert.assertEquals(2, modelContext3.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWorksOnLargeModel() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";
        String sql3 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(2, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        AbstractContext context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql2 });
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        // propose with model-opt-rule on last-modified model
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        String uuidModel1 = context1.getModelContexts().get(0).getTargetModel().getUuid();
        String uuidModel2 = context2.getModelContexts().get(0).getTargetModel().getUuid();
        modelManager.updateDataModel(uuidModel1, copyForWrite -> copyForWrite.setCreateTime(10000));
        modelManager.updateDataModel(uuidModel2, copyForWrite -> copyForWrite.setCreateTime(20000));

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql3 });
        context3.setCanCreateNewModel(true);
        runSuggestModelAndSave(context3);
        Assert.assertEquals(1, context3.getModelContexts().size());
        AbstractContext.ModelContext modelContext3 = context3.getModelContexts().get(0);
        Assert.assertEquals(uuidModel1, modelContext3.getTargetModel().getUuid());
        Assert.assertEquals(3, modelContext3.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWorksOnSnowModelWithSecondHierarchyIsInnerJoin() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "INNER JOIN EDW.TEST_SITES ON EDW.TEST_SITES.SITE_ID = TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(3, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(4, modelContext2.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWorksOnSnowModelWithSecondHierarchyIsLeftJoin() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "LEFT JOIN EDW.TEST_SITES ON EDW.TEST_SITES.SITE_ID = TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(3, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(4, modelContext2.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWillNotWorkOnNonLeftJoin() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "LEFT JOIN EDW.TEST_SITES ON EDW.TEST_SITES.SITE_ID = TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "INNER JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(3, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext2.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWillNotWorkOnNonEquivJoin() throws IOException {
        String originSql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { originSql1 });
        runSuggestModelAndSave(context1);
        NDataModel model = context1.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getJoinTables().size());

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        String path = "src/test/resources/data/join.json";
        JoinDesc joinDesc = JsonUtil.readValue(new File(path), JoinDesc.class);
        modelManager.updateDataModel(model.getUuid(), copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            JoinTableDesc joinTable = joinTables.get(0);
            joinTable.setJoin(joinDesc);
            copyForWrite.setJoinTables(joinTables);
        });
        Assert.assertFalse(modelManager.getDataModelDesc(model.getUuid()).isBroken());

        // propose join
        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
    }

    @Test
    public void testModelOptRuleWillNotWorkOnToManyLeftJoinModel() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "LEFT JOIN EDW.TEST_SITES ON EDW.TEST_SITES.SITE_ID = TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        NDataModel model = context1.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(3, model.getJoinTables().size());

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.updateDataModel(model.getUuid(), copyForWrite -> {
            List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
            joinTables.get(0).setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);
            copyForWrite.setJoinTables(joinTables);
        });
        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext2.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testModelOptRuleWillNotWorkIfJoinsFromFactTableAreNotAlwaysLeft() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT\n"
                + "LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID\n"
                + "LEFT JOIN EDW.TEST_SITES ON EDW.TEST_SITES.SITE_ID = TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL";
        String sql2 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM test_kylin_fact\n" //
                + "LEFT JOIN TEST_ORDER ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID ";

        String project = "newten";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), project,
                new String[] { sql1 });
        runSuggestModelAndSave(context1);
        Assert.assertEquals(3, context1.getModelContexts().get(0).getTargetModel().getJoinTables().size());

        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), project);
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), project,
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext2.getTargetModel().getJoinTables().size());
    }

    @Test
    public void testExactlyMatchWillNotApplyModelOptRule() {
        String sql1 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON (TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT)"
                + "LEFT JOIN TEST_ACCOUNT ON (SELLER_ID = ACCOUNT_ID)";
        String sql2 = "SELECT LSTG_FORMAT_NAME, SUM(PRICE)\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN TEST_ACCOUNT ON (SELLER_ID = ACCOUNT_ID)\n" //
                + "GROUP BY LSTG_FORMAT_NAME";
        String sql3 = "SELECT LSTG_FORMAT_NAME\n" //
                + "FROM TEST_KYLIN_FACT\n" //
                + "LEFT JOIN EDW.TEST_CAL_DT ON (TEST_KYLIN_FACT.CAL_DT = EDW.TEST_CAL_DT.CAL_DT)\n"
                + "GROUP BY LSTG_FORMAT_NAME";
        AbstractContext context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten",
                new String[] { sql1 });
        runSuggestModelAndSave(context1);

        // propose without model-opt-rule 
        AbstractContext context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten",
                new String[] { sql2 });
        context2.setCanCreateNewModel(true);
        runSuggestModelAndSave(context2);
        Assert.assertEquals(1, context2.getModelContexts().size());
        AbstractContext.ModelContext modelContext2 = context2.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext2.getIndexRexItemMap().size());
        Assert.assertEquals(1, modelContext2.getMeasureRecItemMap().size());
        Assert.assertTrue(modelContext2.getDimensionRecItemMap().isEmpty());

        // propose with model-opt-rule
        getTestConfig().setProperty("kylin.smart.conf.model-opt-rule", "append");
        AbstractContext context3 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten",
                new String[] { sql3 });
        context3.setCanCreateNewModel(true);
        runSuggestModelAndSave(context3);
        Assert.assertEquals(1, context3.getModelContexts().size());
        AbstractContext.ModelContext modelContext3 = context3.getModelContexts().get(0);
        Assert.assertEquals(1, modelContext3.getIndexRexItemMap().size());
        Assert.assertTrue(modelContext3.getMeasureRecItemMap().isEmpty());
        Assert.assertTrue(modelContext3.getDimensionRecItemMap().isEmpty());
    }

    private void runSuggestModelAndSave(AbstractContext smartContext) {
        val smartMaster = new SmartMaster(smartContext);
        smartMaster.executePropose();
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
    }
}