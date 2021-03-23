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
package io.kyligence.kap.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NLookupCandidate;
import io.kyligence.kap.metadata.cube.cuboid.NQueryLayoutChooser;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowCapabilityChecker;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.newten.auto.NAutoTestBase;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;
import lombok.var;

public class NQueryLayoutChooserTest extends NAutoTestBase {

    private static final String PROJECT = "default";

    @Test
    public void testSelectIndexInOneModel() {
        // 1. test match aggIndex
        String sql = "select CAL_DT, count(price) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' group by CAL_DT ";
        OLAPContext context = prepareOlapContext(sql).get(0);
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);

        // prepare metadata
        // add a lowest cost layout
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout lowestCostLayout = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(),
                10001L);
        lowestCostLayout.setRows(1000L);
        dataflowUpdate.setToAddOrUpdateLayouts(lowestCostLayout);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflow(dataflowUpdate);

        NDataflow newDf = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NLayoutCandidate pair = NQueryLayoutChooser
                .selectLayoutCandidate(newDf, newDf.getQueryableSegments(), context.getSQLDigest());
        Assert.assertNotNull(pair);
        Assert.assertEquals(10001L, pair.getLayoutEntity().getId());
        Assert.assertFalse(pair.getLayoutEntity().getIndex().isTableIndex());
        Assert.assertEquals(1000.0D, pair.getCost(), 0.01);

        // 2. tableIndex match
        sql = "select CAL_DT from test_kylin_fact where CAL_DT='2012-01-10'";
        OLAPContext context1 = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName1 = RealizationChooser.matchJoins(newDf.getModel(), context1);
        context1.fixModel(newDf.getModel(), sqlAlias2ModelName1);
        pair = NQueryLayoutChooser.selectLayoutCandidate(newDf, newDf.getQueryableSegments(), context1.getSQLDigest());
        Assert.assertTrue(pair.getLayoutEntity().getIndex().isTableIndex());
    }

    @Test
    public void testFilterColsAffectIndexSelection() {
        // prepare metadata
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        addLayout(dataflow);

        String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT='2012-01-10' and TRANS_ID > 10000 group by CAL_DT, TRANS_ID ";
        OLAPContext context = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);

        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val pair = NQueryLayoutChooser.selectLayoutCandidate(df, df.getQueryableSegments(), context.getSQLDigest());
        Assert.assertNotNull(pair);
        Assert.assertEquals(1010001, pair.getLayoutEntity().getId());

        sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT > '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
        OLAPContext context1 = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName1 = RealizationChooser.matchJoins(dataflow.getModel(), context1);
        context1.fixModel(dataflow.getModel(), sqlAlias2ModelName1);
        val pair1 = NQueryLayoutChooser.selectLayoutCandidate(df, df.getQueryableSegments(), context1.getSQLDigest());
        Assert.assertNotNull(pair1);
        Assert.assertEquals(1010002, pair1.getLayoutEntity().getId());

        // same filter level, select the col with smallest cardinality
        sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT = '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
        mockTableStats();
        OLAPContext context2 = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName2 = RealizationChooser.matchJoins(dataflow.getModel(), context2);
        context2.fixModel(dataflow.getModel(), sqlAlias2ModelName2);
        val pair2 = NQueryLayoutChooser.selectLayoutCandidate(df, df.getQueryableSegments(), context2.getSQLDigest());
        Assert.assertNotNull(pair2);
        Assert.assertEquals(1010002, pair2.getLayoutEntity().getId());
    }

    @Test
    public void testDerivedColsSelection() {
        // prepare metadata
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        mockDerivedIndex(dataflow);

        String sql = "select test_kylin_fact.lstg_format_name, META_CATEG_NAME, count(*) as TRANS_CNT \n"
                + " from test_kylin_fact \n" + "left JOIN edw.test_cal_dt as test_cal_dt\n"
                + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n" + " left JOIN test_category_groupings\n"
                + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + " left JOIN edw.test_sites as test_sites\n"
                + " ON test_kylin_fact.lstg_site_id = test_sites.site_id\n"

                + " group by test_kylin_fact.lstg_format_name, META_CATEG_NAME";
        OLAPContext context = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);

        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val pair = NQueryLayoutChooser.selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(),
                context.getSQLDigest());
        Assert.assertNotNull(pair);
        Assert.assertEquals(1010001L, pair.getLayoutEntity().getId());
    }

    @Test
    public void testLookupMatch() {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // prepare table desc snapshot path
        NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject()).getTableDesc("EDW.TEST_SITES")
                .setLastSnapshotPath("default/table_snapshot/EDW.TEST_SITES/c1e8096e-4e7f-4387-b7c3-5147c1ce38d6");

        // case 1. raw-query answered by Lookup
        String sql = "select SITE_ID from EDW.TEST_SITES";
        OLAPContext context = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        val result = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                context.getSQLDigest());
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getSelectedCandidate() instanceof NLookupCandidate);
        Assert.assertFalse(context.getSQLDigest().allColumns.isEmpty());
        Assert.assertEquals(1, context.getSQLDigest().allColumns.size());

        // case 2. aggregate-query answered by lookup
        String sql1 = "select sum(SITE_ID) from EDW.TEST_SITES";
        OLAPContext context1 = prepareOlapContext(sql1).get(0);
        Map<String, String> sqlAlias2ModelName1 = RealizationChooser.matchJoins(dataflow.getModel(), context1);
        context1.fixModel(dataflow.getModel(), sqlAlias2ModelName1);
        val result1 = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                context1.getSQLDigest());
        Assert.assertNotNull(result1);
        Assert.assertTrue(result1.getSelectedCandidate() instanceof NLookupCandidate);
        Assert.assertFalse(context1.getSQLDigest().allColumns.isEmpty());
        Assert.assertEquals(1, context1.getSQLDigest().allColumns.size());

        // case 3. cannot answered when there are no ready segment
        removeAllSegment(dataflow);
        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val result2 = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                context.getSQLDigest());
        Assert.assertFalse(result2.capable);

    }

    @Test
    public void testShardByCol() {
        // prepare metadata
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        mockShardByLayout(dataflow);
        mockTableStats();

        String sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT = '2012-01-10' and TRANS_ID = 10000 group by CAL_DT, TRANS_ID ";
        OLAPContext context1 = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName1 = RealizationChooser.matchJoins(dataflow.getModel(), context1);
        context1.fixModel(dataflow.getModel(), sqlAlias2ModelName1);

        // same filter level, select the col with smallest cardinality and with shardby col
        dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val pair1 = NQueryLayoutChooser.selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(),
                context1.getSQLDigest());
        Assert.assertNotNull(pair1);
        Assert.assertEquals(1010002, pair1.getLayoutEntity().getId());

        sql = "select CAL_DT, TRANS_ID, count(*) as GMV from test_kylin_fact \n"
                + " where CAL_DT = '2012-01-10' and TRANS_ID > 10000 group by CAL_DT, TRANS_ID ";
        OLAPContext context2 = prepareOlapContext(sql).get(0);
        Map<String, String> sqlAlias2ModelName2 = RealizationChooser.matchJoins(dataflow.getModel(), context2);
        context2.fixModel(dataflow.getModel(), sqlAlias2ModelName2);
        val pair2 = NQueryLayoutChooser.selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(),
                context2.getSQLDigest());
        Assert.assertNotNull(pair2);
        Assert.assertEquals(1010003, pair2.getLayoutEntity().getId());
    }

    @Test
    public void testUnmatchedCountColunm() {
        overwriteSystemProp("kylin.query.replace-count-column-with-count-star", "true");
        String sql = "select avg(TEST_KYLIN_FACT.ITEM_COUNT) from TEST_KYLIN_FACT";
        OLAPContext context = prepareOlapContext(sql).get(0);
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                .getDataflow("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
        context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
        NLayoutCandidate pair = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context.getSQLDigest());

        Assert.assertNotNull(pair);
        List<NDataModel.Measure> allMeasures = dataflow.getModel().getAllMeasures();
        Assert.assertTrue(containMeasure(allMeasures, "COUNT", "1"));
        Assert.assertTrue(containMeasure(allMeasures, "SUM", "DEFAULT.TEST_KYLIN_FACT.PRICE"));
        Assert.assertFalse(containMeasure(allMeasures, "COUNT", "DEFAULT.TEST_KYLIN_FACT.PRICE"));
    }

    @Test
    public void testTableIndexAndAggIndex() {
        overwriteSystemProp("kylin.query.use-tableindex-answer-non-raw-query", "true");
        String uuid = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
        NDataflow dataflow = NDataflowManager.getInstance(getTestConfig(), "table_index").getDataflow(uuid);

        String sql1 = "select sum(ORDER_ID) from TEST_KYLIN_FACT";
        OLAPContext context1 = prepareOlapContext(sql1).get(0);
        Map<String, String> sqlAlias2ModelName1 = RealizationChooser.matchJoins(dataflow.getModel(), context1);
        context1.fixModel(dataflow.getModel(), sqlAlias2ModelName1);
        NLayoutCandidate pair1 = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context1.getSQLDigest());
        Assert.assertFalse(pair1.getLayoutEntity().getIndex().isTableIndex());

        String sql2 = "select max(ORDER_ID) from TEST_KYLIN_FACT";
        OLAPContext context2 = prepareOlapContext(sql2).get(0);
        Map<String, String> sqlAlias2ModelName2 = RealizationChooser.matchJoins(dataflow.getModel(), context2);
        context2.fixModel(dataflow.getModel(), sqlAlias2ModelName2);
        NLayoutCandidate pair2 = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context2.getSQLDigest());
        Assert.assertFalse(pair2.getLayoutEntity().getIndex().isTableIndex());

        String sql3 = "select min(ORDER_ID) from TEST_KYLIN_FACT";
        OLAPContext context3 = prepareOlapContext(sql3).get(0);
        Map<String, String> sqlAlias2ModelName3 = RealizationChooser.matchJoins(dataflow.getModel(), context3);
        context3.fixModel(dataflow.getModel(), sqlAlias2ModelName3);
        val pair3 = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context3.getSQLDigest());
        Assert.assertFalse(pair3.getLayoutEntity().getIndex().isTableIndex());

        String sql4 = "select count(ORDER_ID) from TEST_KYLIN_FACT";
        OLAPContext context4 = prepareOlapContext(sql4).get(0);
        Map<String, String> sqlAlias2ModelName4 = RealizationChooser.matchJoins(dataflow.getModel(), context4);
        context4.fixModel(dataflow.getModel(), sqlAlias2ModelName4);
        val pair4 = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context4.getSQLDigest());
        Assert.assertFalse(pair4.getLayoutEntity().getIndex().isTableIndex());

        String sql5 = "select count(distinct ORDER_ID) from TEST_KYLIN_FACT";
        OLAPContext context5 = prepareOlapContext(sql5).get(0);
        Map<String, String> sqlAlias2ModelName5 = RealizationChooser.matchJoins(dataflow.getModel(), context5);
        context5.fixModel(dataflow.getModel(), sqlAlias2ModelName5);
        val pair5 = NQueryLayoutChooser
                .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context5.getSQLDigest());
        Assert.assertFalse(pair5.getLayoutEntity().getIndex().isTableIndex());
    }

    @Test
    public void testDimensionAsMeasure_CountDistinctDerived_derivePkFromFk() throws Exception {
        val sql1 = new String[] { "select cal_dt, seller_id, count(*) " + "from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "group by test_kylin_fact.cal_dt, test_kylin_fact.seller_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "newten");
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        val joinTables = model.getJoinTables();
        joinTables.get(0).setKind(NDataModel.TableKind.LOOKUP);
        modelManager.updateDataModelDesc(model);

        buildAllCubes(getTestConfig(), "newten");

        val sql2 = "select cal_dt, count(distinct account_id) " + "from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id " + "group by cal_dt";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_derived_pk_from_fk", sql2));
        populateSSWithCSVData(getTestConfig(), "newten", SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(query, "newten", NExecAndComp.CompareLevel.SAME, "left");

    }

    @Test
    public void testDimensionAsMeasure_CountDistinctDerived_deriveFkFromPk() throws Exception {
        val sql1 = new String[] {
                "select cal_dt, account_id, count(*) " + "from test_kylin_fact inner join test_account "
                        + "on test_kylin_fact.seller_id = test_account.account_id "
                        + "group by test_kylin_fact.cal_dt, test_account.account_id" };

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "newten");
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        val joinTables = model.getJoinTables();
        joinTables.get(0).setKind(NDataModel.TableKind.LOOKUP);
        modelManager.updateDataModelDesc(model);

        buildAllCubes(getTestConfig(), "newten");

        val sql2 = "select cal_dt, count(distinct seller_id) " + "from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id " + "group by cal_dt";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_derived_fk_from_pk", sql2));
        populateSSWithCSVData(getTestConfig(), "newten", SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(query, "newten", NExecAndComp.CompareLevel.SAME, "inner");

    }

    @Test
    public void testDimensionAsMeasure_CountDistinctDerived_onLookup() throws Exception {
        val sql1 = new String[] { "select cal_dt, seller_id, count(*) " + "from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "group by test_kylin_fact.cal_dt, test_kylin_fact.seller_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "newten");
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        val joinTables = model.getJoinTables();
        joinTables.get(0).setKind(NDataModel.TableKind.LOOKUP);
        modelManager.updateDataModelDesc(model);

        buildAllCubes(getTestConfig(), "newten");

        val sql2 = "select cal_dt, count(distinct ACCOUNT_COUNTRY) " + "from test_kylin_fact left join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id " + "group by cal_dt";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_derived_fk_from_pk", sql2));
        populateSSWithCSVData(getTestConfig(), "newten", SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(query, "newten", NExecAndComp.CompareLevel.SAME, "left");

    }

    @Test
    public void testDimensionAsMeasure_CountDistinctComplexExpr() throws Exception {
        val sql1 = new String[] { "select cal_dt, price, item_count from test_kylin_fact group by cal_dt, price, item_count" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "newten");
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        modelManager.updateDataModelDesc(model);

        buildAllCubes(getTestConfig(), "newten");

        val sql2 = "select count(distinct (case when cal_dt > date'2013-01-01' then price else item_count end)) from test_kylin_fact";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_complex_expr", sql2));
        populateSSWithCSVData(getTestConfig(), "newten", SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(query, "newten", NExecAndComp.CompareLevel.SAME, "left");

    }

    @Test
    public void test_CountDistinctExpr_fallback() throws Exception {
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "true");
        val sql1 = new String[] { "select cal_dt, price from test_kylin_fact group by cal_dt, price" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sql1);
        val smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "newten");
        val model = modelManager
                .getDataModelDesc(smartMaster.getContext().getModelContexts().get(0).getTargetModel().getId());
        modelManager.updateDataModelDesc(model);

        buildAllCubes(getTestConfig(), "newten");

        val sql2 = "select count(distinct (case when cal_dt > date'2013-01-01' then price else null end)) from test_kylin_fact";

        List<Pair<String, String>> query = new ArrayList<>();
        query.add(new Pair<>("count_distinct_expr_fallback", sql2));
        populateSSWithCSVData(getTestConfig(), "newten", SparderEnv.getSparkSession());
        NExecAndComp.execAndCompare(query, "newten", NExecAndComp.CompareLevel.SAME, "left");

    }

    @Test
    public void testMatchJoinWithFiter() {
        try {
            final List<String> filters = ImmutableList.of(" b.SITE_NAME is not null",
                    " b.SITE_NAME is not null and b.SITE_NAME is null", " b.SITE_NAME = '英国'", " b.SITE_NAME < '英国'",
                    " b.SITE_NAME > '英国'", " b.SITE_NAME >= '英国'", " b.SITE_NAME <= '英国'", " b.SITE_NAME <> '英国'",
                    " b.SITE_NAME like '%英国%'", " b.SITE_NAME not like '%英国%'", " b.SITE_NAME not in ('英国%')",
                    " b.SITE_NAME similar to '%英国%'", " b.SITE_NAME not similar to '%英国%'",
                    " b.SITE_NAME is not distinct from '%英国%'", " b.SITE_NAME between '1' and '2'",
                    " b.SITE_NAME not between '1' and '2'", " b.SITE_NAME <= '英国' OR b.SITE_NAME >= '英国'",
                    " b.SITE_NAME = '英国' is not false", " b.SITE_NAME = '英国' is not true",
                    " b.SITE_NAME = '英国' is false", " b.SITE_NAME = '英国' is true");
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
            for (String filter : filters) {
                String sql = "select CAL_DT from test_kylin_fact a \n" + " inner join EDW.test_sites b \n"
                        + " on a.LSTG_SITE_ID = b.SITE_ID \n" + " where " + filter;
                OLAPContext context = prepareOlapContext(sql).get(0);
                NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                        .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
                Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
                context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
                val pair = NQueryLayoutChooser
                        .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context.getSQLDigest());
                Assert.assertEquals(20000010001L, pair.getLayoutEntity().getId());
            }
        } finally {
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "false");
        }
    }

    @Test
    public void testCanNotMatchJoinWithFiter() {
        try {
            final List<String> filters = ImmutableList.of(" b.SITE_NAME is null",
                    " b.SITE_NAME is distinct from '%英国%'", " b.SITE_NAME is not distinct from null",
                    " b.SITE_NAME is not null or a.TRANS_ID is not null",
                    " case when b.SITE_NAME is not null then false else true end" // TODO
            );
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
            for (String filter : filters) {
                String sql = "select CAL_DT from test_kylin_fact a \n" + " inner join EDW.test_sites b \n"
                        + " on a.LSTG_SITE_ID = b.SITE_ID \n" + " where " + filter;
                OLAPContext context = prepareOlapContext(sql).get(0);
                NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                        .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
                Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
                Assert.assertEquals(null, sqlAlias2ModelName);
            }
        } finally {
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "false");
        }
    }

    @Test
    public void testMatchJoinWithEnhancedMode() {
        try {
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
            String sql = "SELECT \n" + "COUNT(\"TEST_KYLIN_FACT\".\"SELLER_ID\")\n" + "FROM \n"
                    + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                    + "LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n" + // left or inner join
                    "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND "
                    + "\"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                    + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                    + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                    + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n" + // left or inner join
                    "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                    + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n" + // left or inner join
                    "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                    + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                    + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                    + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\"\n"
                    + "GROUP BY \"TEST_KYLIN_FACT\".\"TRANS_ID\"";
            OLAPContext context = prepareOlapContext(sql).get(0);
            NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT)
                    .getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            Map<String, String> sqlAlias2ModelName = RealizationChooser.matchJoins(dataflow.getModel(), context);
            context.fixModel(dataflow.getModel(), sqlAlias2ModelName);
            val pair = NQueryLayoutChooser
                    .selectLayoutCandidate(dataflow, dataflow.getQueryableSegments(), context.getSQLDigest());
            Assert.assertEquals(1L, pair.getLayoutEntity().getId());
        } finally {
            getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "false");
        }
    }

    public boolean containMeasure(List<NDataModel.Measure> allMeasures, String expression, String parameter) {
        for (NDataModel.Measure measure : allMeasures) {
            if (measure.getFunction().getExpression().equals(expression)
                    && measure.getFunction().getParameters().get(0).toString().equals(parameter)) {
                return true;
            }
        }
        return false;
    }

    private List<OLAPContext> prepareOlapContext(String sql) {
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), PROJECT, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.analyzeSQLs();
        List<OLAPContext> ctxs = Lists.newArrayList();
        smartMaster.getContext().getModelContexts()
                .forEach(nModelContext -> ctxs.addAll(nModelContext.getModelTree().getOlapContexts()));
        return ctxs;
    }

    private void removeAllSegment(NDataflow dataflow) {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        val indexPlan = indexPlanManager.getIndexPlanByModelAlias("nmodel_basic");
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflow(dataflowUpdate);
    }

    private void addLayout(NDataflow dataflow) {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = indePlanManager.getIndexPlanByModelAlias("nmodel_basic");
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();

            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(2, 1, 3, 100000));
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newAggIndex.getId() + 2);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2));
            cuboids.add(newAggIndex);

            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater);

        // test case that select the layout with less columns
        NIndexPlanManager.NIndexPlanUpdater updater1 = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();
            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(1, 2, 3, 4));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            val newLayout = new LayoutEntity();
            newLayout.setId(newAggIndex.getId() + 1);
            newLayout.setAuto(true);
            newLayout.setColOrder(Lists.newArrayList(2, 1, 3, 4, 100000));
            newAggIndex.setLayouts(Lists.newArrayList(newLayout));
            cuboids.add(newAggIndex);

            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater1);

        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout1 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010001L);
        layout1.setRows(1000L);
        NDataLayout layout2 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010002L);
        layout2.setRows(1000L);
        NDataLayout layout3 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1020001L);
        layout3.setRows(1000L);
        dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2, layout3);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflow(dataflowUpdate);
    }

    private void mockShardByLayout(NDataflow dataflow) {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = indePlanManager.getIndexPlanByModelAlias("nmodel_basic");
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();

            val newAggIndex = new IndexEntity();
            newAggIndex.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex.setDimensions(Lists.newArrayList(1, 2, 3));
            newAggIndex.setMeasures(Lists.newArrayList(100000));
            // mock no shardby column
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
            // mock shardby trans_id
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newAggIndex.getId() + 2);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
            newLayout2.setShardByColumns(Lists.newArrayList(1));
            //mock shardby cal_dt
            val newLayout3 = new LayoutEntity();
            newLayout3.setId(newAggIndex.getId() + 3);
            newLayout3.setAuto(true);
            newLayout3.setColOrder(Lists.newArrayList(1, 2, 3, 100000));
            newLayout3.setShardByColumns(Lists.newArrayList(2));

            newAggIndex.setLayouts(Lists.newArrayList(newLayout1, newLayout2, newLayout3));
            cuboids.add(newAggIndex);

            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater);

        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout1 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010001L);
        layout1.setRows(1000L);
        NDataLayout layout2 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010002L);
        layout2.setRows(1000L);
        NDataLayout layout3 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010003L);
        layout3.setRows(1000L);
        dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2, layout3);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflow(dataflowUpdate);
    }

    private void mockDerivedIndex(NDataflow dataflow) {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = indePlanManager.getIndexPlanByModelAlias("nmodel_basic");
        NIndexPlanManager.NIndexPlanUpdater updater = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();
            val newAggIndex1 = new IndexEntity();
            newAggIndex1.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex1.setDimensions(Lists.newArrayList(1, 3, 4, 5, 8));
            newAggIndex1.setMeasures(Lists.newArrayList(100000));
            val newLayout1 = new LayoutEntity();
            newLayout1.setId(newAggIndex1.getId() + 1);
            newLayout1.setAuto(true);
            newLayout1.setColOrder(Lists.newArrayList(3, 1, 5, 4, 8, 100000));
            newAggIndex1.setLayouts(Lists.newArrayList(newLayout1));
            cuboids.add(newAggIndex1);

            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater);

        NIndexPlanManager.NIndexPlanUpdater updater1 = copyForWrite -> {
            val cuboids = copyForWrite.getIndexes();
            val newAggIndex2 = new IndexEntity();
            newAggIndex2.setId(copyForWrite.getNextAggregationIndexId());
            newAggIndex2.setDimensions(Lists.newArrayList(1, 3, 4, 8));
            newAggIndex2.setMeasures(Lists.newArrayList(100000));
            val newLayout2 = new LayoutEntity();
            newLayout2.setId(newAggIndex2.getId() + 1);
            newLayout2.setAuto(true);
            newLayout2.setColOrder(Lists.newArrayList(3, 1, 4, 8, 100000));
            newAggIndex2.setLayouts(Lists.newArrayList(newLayout2));
            cuboids.add(newAggIndex2);
            copyForWrite.setIndexes(cuboids);
        };
        indePlanManager.updateIndexPlan(indexPlan.getUuid(), updater1);

        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        NDataLayout layout1 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1010001L);
        layout1.setRows(1000L);
        NDataLayout layout2 = NDataLayout.newDataLayout(dataflow.getLatestReadySegment().getSegDetails(), 1020001L);
        layout2.setRows(1000L);
        dataflowUpdate.setToAddOrUpdateLayouts(layout1, layout2);
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflow(dataflowUpdate);
    }

    private void mockTableStats() {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        var tableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);
        tableExt = tableMetadataManager.copyForWrite(tableExt);
        List<TableExtDesc.ColumnStats> columnStats = Lists.newArrayList();

        for (ColumnDesc columnDesc : tableDesc.getColumns()) {
            if (columnDesc.isComputedColumn()) {
                continue;
            }
            TableExtDesc.ColumnStats colStats = tableExt.getColumnStatsByName(columnDesc.getName());
            if (colStats == null) {
                colStats = new TableExtDesc.ColumnStats();
                colStats.setColumnName(columnDesc.getName());
            }
            if ("CAL_DT".equals(columnDesc.getName())) {
                colStats.setCardinality(1000);
            } else if ("TRANS_ID".equals(columnDesc.getName())) {
                colStats.setCardinality(10000);
            } else {
                colStats.setCardinality(100);
            }
            columnStats.add(colStats);
        }
        tableExt.setColumnStats(columnStats);
        tableMetadataManager.saveTableExt(tableExt);
    }
}