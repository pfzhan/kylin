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

package io.kyligence.kap.smart.index;

import static io.kyligence.kap.smart.model.GreedyModelTreesBuilderTest.smartUtHook;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.AutoTestOnLearnKylinData;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;
import lombok.var;

public class IndexSuggesterTest extends AutoTestOnLearnKylinData {

    @Test
    public void testAggIndexSuggesetColOrder() {

        String[] sqls = new String[] { "select lstg_format_name, buyer_id, seller_id, sum(price) from kylin_sales "
                + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name, buyer_id, seller_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        {
            AbstractContext ctx = smartMaster.getContext();
            AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
            final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
            final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
            final List<LayoutEntity> layouts = allCuboids.get(0).getLayouts();
            final LayoutEntity layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 0, 3, 9, 100000, 100001]", layout.getColOrder().toString());
        }
    }

    @Test
    public void testTableIndexSuggestColOrder() {
        String[] sqls = new String[] {
                "select ops_user_id, ops_region, price from kylin_sales where "
                        + "ops_user_id = '10009998' order by item_count, lstg_site_id",
                "select ops_user_id, ops_region, price from kylin_sales where "
                        + "part_dt = '2012-01-08' order by item_count, lstg_site_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals(Lists.newArrayList(6, 1, 4, 5, 8), allCuboids.get(0).getDimensions());
        Assert.assertEquals("{1, 4, 5, 6, 8}", allCuboids.get(0).getDimensionBitset().toString());
        Assert.assertEquals("unexpected colOrder", "[6, 1, 4, 5, 8]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);

        final LayoutEntity layout2 = allCuboids.get(1).getLayouts().get(0);
        Assert.assertEquals(Lists.newArrayList(7, 1, 4, 5, 6, 8), allCuboids.get(1).getDimensions());
        Assert.assertEquals("{1, 4, 5, 6, 7, 8}", allCuboids.get(1).getDimensionBitset().toString());
        Assert.assertEquals("unexpected colOrder", "[7, 1, 4, 5, 6, 8]", layout2.getColOrder().toString());
        Assert.assertTrue(layout2.getUpdateTime() > 0);
    }

    @Test
    public void testAggIndexSuggestColOrder() {
        String[] sqls = new String[] {
                "SELECT COUNT(KYLIN_ACCOUNT.ACCOUNT_COUNTRY), KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL\n" + "FROM (\n"
                        + "\tSELECT PRICE, TRANS_ID, SELLER_ID FROM KYLIN_SALES ORDER BY TRANS_ID DESC\n" + "\t) FACT\n"
                        + "INNER JOIN KYLIN_ACCOUNT\n" + "ON KYLIN_ACCOUNT.ACCOUNT_ID = FACT.SELLER_ID\n"
                        + "GROUP BY KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL\n"
                        + "ORDER BY KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[4, 100000, 100001]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);
    }

    @Test
    public void testCountOneMeasureIdInheritCorrectly() {
        String[] sqls = { "select sum(price) from kylin_sales" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls[0]).isNotSucceed());

        // update measure id to id + 1, so id of count(1) is 100001
        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        IndexPlan indexPlan = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan();
        model.getAllMeasures().forEach(measure -> measure.setId(measure.getId() + 1));
        indexPlan.getIndexes().clear();
        NDataModelManager.getInstance(getTestConfig(), proj).updateDataModelDesc(model);
        NIndexPlanManager.getInstance(getTestConfig(), proj).updateIndexPlan(indexPlan);

        // propose again
        String[] sqls2 = { "select count(price) from kylin_sales" };
        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls2);
        smartMaster = new SmartMaster(context2);
        smartMaster.runUtWithContext(smartUtHook);

        // assert propose success
        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sqls2[0]).isNotSucceed());

        // assert the id of count(1) is 100001 for 
        NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        List<NDataModel.Measure> allMeasures = targetModel.getAllMeasures();
        NDataModel.Measure measure = allMeasures.get(0);
        Assert.assertEquals(100001, measure.getId());
        Assert.assertEquals(FunctionDesc.newCountOne(), measure.getFunction());
    }

    @Test
    public void testComplicateSuggestColOrder() {
        String project = "newten";
        KylinConfig kylinConfig = getTestConfig();

        String[] sqls = new String[] { "SELECT test_cal_dt.week_beg_dt, test_category_groupings.meta_categ_name, "
                + "test_category_groupings.categ_lvl2_name, test_category_groupings.categ_lvl3_name\n"
                + "\t, SUM(test_kylin_fact.price) AS GMV, COUNT(*) AS TRANS_CNT\nFROM test_kylin_fact\n"
                + "\tINNER JOIN edw.test_cal_dt test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "\tINNER JOIN test_category_groupings\n"
                + "\tON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id\n"
                + "\t\tAND test_kylin_fact.lstg_site_id = test_category_groupings.site_id\n"
                + "WHERE ((test_kylin_fact.leaf_categ_id = 100\n\t\tOR test_kylin_fact.leaf_categ_id > 200)\n"
                + "\tAND test_kylin_fact.price > 10\n\tAND test_kylin_fact.lstg_format_name LIKE '%BIN%'\n"
                + "\tAND test_cal_dt.week_beg_dt BETWEEN DATE '2013-05-01' AND DATE '2013-08-01')\n"
                + "\tAND concat(test_category_groupings.categ_lvl3_name, 'H') = 'AAA'\n"
                + "GROUP BY test_cal_dt.week_beg_dt, test_category_groupings.meta_categ_name, "
                + "test_category_groupings.categ_lvl2_name, test_category_groupings.categ_lvl3_name" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), project, sqls);
        SmartMaster smartMaster = new SmartMaster(context);

        val tableManager = NTableMetadataManager.getInstance(kylinConfig, project);
        val tableTestKylinFact = tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableTestKylinFact.setIncrementLoading(true);
        tableManager.updateTableDesc(tableTestKylinFact);

        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final ImmutableBiMap<Integer, TblColRef> effectiveDimensions = mdCtx.getTargetModel().getEffectiveDimensions();

        String[] expectedColOrder = new String[] { "DEFAULT.TEST_KYLIN_FACT.LEAF_CATEG_ID",
                "DEFAULT.TEST_KYLIN_FACT.PRICE", "EDW.TEST_CAL_DT.WEEK_BEG_DT",
                "DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME",
                "DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME", "DEFAULT.TEST_CATEGORY_GROUPINGS.META_CATEG_NAME" };

        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        final ImmutableList<Integer> colOrder = layout.getColOrder();
        Assert.assertEquals(expectedColOrder[0], effectiveDimensions.get(colOrder.get(0)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[1], effectiveDimensions.get(colOrder.get(1)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[2], effectiveDimensions.get(colOrder.get(2)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[3], effectiveDimensions.get(colOrder.get(3)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[4], effectiveDimensions.get(colOrder.get(4)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[5], effectiveDimensions.get(colOrder.get(5)).getCanonicalName());
        Assert.assertEquals(expectedColOrder[6], effectiveDimensions.get(colOrder.get(6)).getCanonicalName());
        Assert.assertTrue(layout.getUpdateTime() > 0);

        tableTestKylinFact.setIncrementLoading(false);
        tableManager.updateTableDesc(tableTestKylinFact);
    }

    @Test
    public void testSuggestWithoutDimension() {
        // TODO add a case to verify redundant layout of aggGroup will not consider in auto-modeling, maybe CI test
        String[] sqls = new String[] { "select count(*) from kylin_sales", // count star
                "select count(price) from kylin_sales", // measure with column
                "select sum(price) from kylin_sales", //
                "select 1 as ttt from kylin_sales" // no dimension and no measure, but will add an extra dimension
        };

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final NDataModel targetModel = mdCtx.getTargetModel();
        Assert.assertEquals(1, targetModel.getEffectiveDimensions().size());
        Assert.assertEquals(3, targetModel.getEffectiveMeasures().size());
        Assert.assertEquals(12, targetModel.getEffectiveCols().size());

        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        Assert.assertEquals(2, allCuboids.size());

        final IndexEntity indexEntity0 = allCuboids.get(0);
        Assert.assertEquals(1, indexEntity0.getLayouts().size());
        Assert.assertEquals(IndexEntity.TABLE_INDEX_START_ID + 1, indexEntity0.getLayouts().get(0).getId());
        Assert.assertEquals("[0]", indexEntity0.getLayouts().get(0).getColOrder().toString());

        final IndexEntity indexEntity1 = allCuboids.get(1);
        Assert.assertEquals(1, indexEntity1.getLayouts().size());
        Assert.assertEquals(30001L, indexEntity1.getLayouts().get(0).getId());
        Assert.assertEquals("[100000, 100001, 100002]", indexEntity1.getLayouts().get(0).getColOrder().toString());
    }

    @Test
    public void testMinMaxForAllTypes() {
        String[] sqls = new String[] { "select min(lstg_format_name), max(lstg_format_name) from kylin_sales",
                "select min(part_dt), max(part_dt) from kylin_sales",
                "select lstg_format_name, min(price), max(price) from kylin_sales group by lstg_format_name",
                "select min(seller_id), max(seller_id) from kylin_sales" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final List<NDataModel.Measure> allMeasures = mdCtx.getTargetModel().getAllMeasures();
        Assert.assertEquals(9, allMeasures.size());
        Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
        Assert.assertEquals("MIN_KYLIN_SALES_LSTG_FORMAT_NAME", allMeasures.get(1).getName());
        Assert.assertEquals("MAX_KYLIN_SALES_LSTG_FORMAT_NAME", allMeasures.get(2).getName());
        Assert.assertEquals("MIN_KYLIN_SALES_PART_DT", allMeasures.get(3).getName());
        Assert.assertEquals("MAX_KYLIN_SALES_PART_DT", allMeasures.get(4).getName());
        Assert.assertEquals("MIN_KYLIN_SALES_PRICE", allMeasures.get(5).getName());
        Assert.assertEquals("MAX_KYLIN_SALES_PRICE", allMeasures.get(6).getName());
        Assert.assertEquals("MIN_KYLIN_SALES_SELLER_ID", allMeasures.get(7).getName());
        Assert.assertEquals("MAX_KYLIN_SALES_SELLER_ID", allMeasures.get(8).getName());

        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        List<IndexEntity> allCuboids = indexPlan.getIndexes();
        final IndexEntity indexEntity0 = allCuboids.get(0);

        Assert.assertEquals("{100000, 100005, 100006}", indexEntity0.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity0.getLayouts().size());
        Assert.assertEquals(20001L, indexEntity0.getLayouts().get(0).getId());

        final IndexEntity indexEntity1 = allCuboids.get(1);
        Assert.assertEquals("{100000, 100001, 100002, 100003, 100004, 100007, 100008}",
                indexEntity1.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity1.getLayouts().size());
        Assert.assertEquals(40001L, indexEntity1.getLayouts().get(0).getId());
    }

    @Test
    public void testSuggestShardByInSemiMode() {
        // set 'kylin.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        // currently, column part_dt's cardinality < 2000 && tans_id's > 2000
        getTestConfig().setProperty("kylin.smart.conf.rowkey.uhc.min-cardinality", "2000");

        String[] sql1 = new String[] { "select part_dt, lstg_format_name, trans_id from kylin_sales" };
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sql1);
        SmartMaster smartMaster = new SmartMaster(context1);
        smartMaster.runUtWithContext(smartUtHook);
        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), proj);
        val modelId = mdCtx.getTargetModel().getId();
        var indexPlan = indexPlanManager.getIndexPlan(modelId);
        Assert.assertNotNull(indexPlan);
        Assert.assertTrue(indexPlan.getAggShardByColumns().isEmpty());

        indexPlanManager.updateIndexPlan(indexPlan.getId(), indexPlanToBeUpdated -> {
            indexPlanToBeUpdated.setAggShardByColumns(Lists.newArrayList(3, 7));
        });

        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, trans_id from kylin_sales where part_dt = '2012-01-01'",
                "select part_dt, trans_id from kylin_sales where trans_id = 100000",
                "select part_dt, lstg_format_name, trans_id from kylin_sales where trans_id = 100000 group by part_dt, lstg_format_name, trans_id" };

        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        new SmartMaster(context2).runUtWithContext(smartUtHook);

        indexPlan = indexPlanManager.getIndexPlan(modelId);
        val indexEntities = indexPlan.getIndexes();
        Assert.assertEquals(3, indexEntities.size());

        // table index does covered by manually set agg index shardby columns
        val layouts = indexEntities.get(0).getLayouts();
        Assert.assertEquals(2, layouts.size());
        Assert.assertEquals(0, layouts.get(0).getShardByColumns().size());
        Assert.assertEquals(0, layouts.get(1).getShardByColumns().size());
        val layouts1 = indexEntities.get(1).getLayouts();
        Assert.assertTrue(indexEntities.get(1).isTableIndex());
        Assert.assertEquals(1, layouts1.size());
        Assert.assertEquals(1, layouts1.get(0).getShardByColumns().size());
        Assert.assertEquals("KYLIN_SALES.TRANS_ID", mdCtx.getTargetModel().getEffectiveCols()
                .get(layouts1.get(0).getShardByColumns().get(0)).getIdentity());

        // agg index
        val layouts2 = indexEntities.get(2).getLayouts();
        Assert.assertFalse(indexEntities.get(2).isTableIndex());
        Assert.assertEquals(1, layouts2.size());
        Assert.assertEquals(2, layouts2.get(0).getShardByColumns().size());
        Assert.assertEquals(indexPlan.getAggShardByColumns(), layouts2.get(0).getShardByColumns());

        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.setAggShardByColumns(Lists.newArrayList(3, 7, 100));
            copyForWrite.removeLayouts(Sets.newHashSet(layouts2.get(0).getId()), true, false);
        });

        sqls = new String[] {
                "select part_dt, lstg_format_name, trans_id from kylin_sales where trans_id = 100000 group by part_dt, lstg_format_name, trans_id" };

        val context3 = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        new SmartMaster(context3).runUtWithContext(smartUtHook);

        indexPlan = indexPlanManager.getIndexPlan(modelId);
        val aggIndexLayouts = indexPlan.getIndexes().get(2).getLayouts();
        Assert.assertFalse(indexEntities.get(2).isTableIndex());
        Assert.assertEquals(1, aggIndexLayouts.size());
        Assert.assertEquals(1, aggIndexLayouts.get(0).getShardByColumns().size());
        Assert.assertEquals(Lists.newArrayList(11), aggIndexLayouts.get(0).getShardByColumns());

        getTestConfig().setProperty("kylin.smart.conf.rowkey.uhc.min-cardinality", "1000000");
    }

    @Test
    public void testSuggestShardBy() {

        // set 'kylin.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        // currently, column part_dt's cardinality < 2000 && tans_id's > 2000
        getTestConfig().setProperty("kylin.smart.conf.rowkey.uhc.min-cardinality", "2000");

        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, trans_id from kylin_sales where part_dt = '2012-01-01'",
                "select part_dt, trans_id from kylin_sales where trans_id = 100000",
                "select part_dt, lstg_format_name, trans_id from kylin_sales",
                "select part_dt, lstg_format_name, trans_id from kylin_sales where trans_id = 100000 group by part_dt, lstg_format_name, trans_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        Assert.assertEquals(mdCtx.getTargetModel().getUuid(), indexPlan.getUuid());

        List<IndexEntity> indexEntities = indexPlan.getIndexes();
        Assert.assertEquals("unmatched cuboids size", 3, indexEntities.size());
        // case 1. with eq-filter but without high cardinality
        final List<LayoutEntity> layouts = indexEntities.get(0).getLayouts();
        Assert.assertEquals("unmatched layouts size", 2, layouts.size());
        Assert.assertEquals("unmatched shard by columns size", 0, layouts.get(0).getShardByColumns().size());
        Assert.assertEquals("unmatched shard by columns size", 0, layouts.get(1).getShardByColumns().size());
        // case 2. tableIndex with eq-filter and high cardinality
        final List<LayoutEntity> layouts1 = indexEntities.get(1).getLayouts();
        Assert.assertTrue(indexEntities.get(1).isTableIndex());
        Assert.assertEquals("unmatched layouts size", 1, layouts1.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts1.get(0).getShardByColumns().size());
        Assert.assertEquals("unexpected identity name of shard by column", "KYLIN_SALES.TRANS_ID", mdCtx
                .getTargetModel().getEffectiveCols().get(layouts1.get(0).getShardByColumns().get(0)).getIdentity());

        // case 2. AggIndex with eq-filter and high cardinality
        final List<LayoutEntity> layouts2 = indexEntities.get(2).getLayouts();
        Assert.assertFalse(indexEntities.get(2).isTableIndex());
        Assert.assertEquals("unmatched layouts size", 1, layouts2.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts2.get(0).getShardByColumns().size());
        Assert.assertEquals("unexpected identity name of shard by column", "KYLIN_SALES.TRANS_ID", mdCtx
                .getTargetModel().getEffectiveCols().get(layouts2.get(0).getShardByColumns().get(0)).getIdentity());
    }

    @Test
    public void testIndexShouldNotMerge_WhenSameDimButDifferentShardBy() {

        // set 'kylin.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        // currently, column part_dt's cardinality < 2000 && tans_id's > 2000
        getTestConfig().setProperty("kylin.smart.conf.rowkey.uhc.min-cardinality", "2000");

        String[] sqls = new String[] {
                "select part_dt, max(lstg_format_name), trans_id from kylin_sales where part_dt = '2012-01-01' group by part_dt, trans_id",
                "select part_dt, count(lstg_format_name), trans_id from kylin_sales where trans_id = 100000 group by part_dt, trans_id",
                "select part_dt, max(lstg_format_name), trans_id from kylin_sales where trans_id = 100000 group by part_dt, trans_id",
                "select part_dt, count(lstg_format_name), trans_id from kylin_sales group by part_dt, trans_id" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);
        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        List<IndexEntity> indexEntities = indexPlan.getIndexes();
        Assert.assertEquals("unmatched cuboids size", 1, indexEntities.size());

        // AggIndex with eq-filter and high cardinality
        final List<LayoutEntity> layouts2 = indexEntities.get(0).getLayouts();
        Assert.assertEquals("unmatched layouts size", 2, layouts2.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts2.get(0).getShardByColumns().size());
        Assert.assertEquals("unexpected identity name of shard by column", "KYLIN_SALES.TRANS_ID", mdCtx
                .getTargetModel().getEffectiveCols().get(layouts2.get(0).getShardByColumns().get(0)).getIdentity());
        Assert.assertEquals("[11, 7, 100000, 100001, 100002]", layouts2.get(0).getColOrder().toString());

        Assert.assertEquals(0, layouts2.get(1).getShardByColumns().size());
        Assert.assertEquals("[7, 11, 100000, 100001, 100002]", layouts2.get(1).getColOrder().toString());
    }

    @Test
    public void testSqlPattern2Layout() {
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                // error case
                "select part_name, lstg_format_name, sum(price) from kylin_sales " };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        // validate sql pattern to layout
        final Map<String, AccelerateInfo> accelerateMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(5, accelerateMap.size());
        Assert.assertEquals(1, accelerateMap.get(sqls[0]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[1]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[2]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[3]).getRelatedLayouts().size());
        Assert.assertEquals(0, accelerateMap.get(sqls[4]).getRelatedLayouts().size());
        Assert.assertTrue(accelerateMap.get(sqls[4]).isPending());

        String cubePlan0 = Lists.newArrayList(accelerateMap.get(sqls[0]).getRelatedLayouts()).get(0).getModelId();
        String cubePlan1 = Lists.newArrayList(accelerateMap.get(sqls[1]).getRelatedLayouts()).get(0).getModelId();
        String cubePlan2 = Lists.newArrayList(accelerateMap.get(sqls[2]).getRelatedLayouts()).get(0).getModelId();
        String cubePlan3 = Lists.newArrayList(accelerateMap.get(sqls[3]).getRelatedLayouts()).get(0).getModelId();
        Assert.assertEquals(cubePlan0, cubePlan1);
        Assert.assertEquals(cubePlan0, cubePlan2);
        Assert.assertEquals(cubePlan0, cubePlan3);

        long layout0 = Lists.newArrayList(accelerateMap.get(sqls[0]).getRelatedLayouts()).get(0).getLayoutId();
        long layout1 = Lists.newArrayList(accelerateMap.get(sqls[1]).getRelatedLayouts()).get(0).getLayoutId();
        long layout2 = Lists.newArrayList(accelerateMap.get(sqls[2]).getRelatedLayouts()).get(0).getLayoutId();
        long layout3 = Lists.newArrayList(accelerateMap.get(sqls[3]).getRelatedLayouts()).get(0).getLayoutId();
        Assert.assertEquals(1L, layout0);
        Assert.assertEquals(1L, layout1);
        Assert.assertEquals(2L, layout2);
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP + 1, layout3);
    }

    @Test
    public void testCuboidWithWindow() {

        String[] sqls = new String[] {
                "select first_value(price) over(partition by lstg_format_name order by part_dt, lstg_format_name) as \"first\",\n"
                        + "last_value(price) over(partition by lstg_format_name order by part_dt, lstg_format_name) as \"current\",\n"
                        + "lag(price, 1, 0.0) over(partition by lstg_format_name order by part_dt, lstg_format_name) as \"prev\",\n"
                        + "lead(price, 1, 0.0) over(partition by lstg_format_name order by part_dt, lstg_format_name) as \"next\",\n"
                        + "ntile(4) over (partition by lstg_format_name order by part_dt, lstg_format_name) as \"quarter\"\n"
                        + "from kylin_sales\n" + "where part_dt < '2012-02-01'\n" };
        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals(Lists.newArrayList(7, 3, 8), allCuboids.get(0).getDimensions());
        Assert.assertEquals("{3, 7, 8}", allCuboids.get(0).getDimensionBitset().toString());
        Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);
    }

    @Test
    public void testCuboidWithWindowLimit() {

        String[] sqls = new String[] { "select trans_id, seller_id,part_dt\n" + ",lead(part_dt, 1) over w as next_dt\n"
                + ",lag(part_dt, 1) over w as last_dt \n" + "from kylin_sales \n"
                + "window w as (partition by seller_id order by part_dt) limit 100;" };

        val context = AccelerationContextUtil.newSmartContext(getTestConfig(), proj, sqls);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(smartUtHook);

        AbstractContext ctx = smartMaster.getContext();
        AbstractContext.ModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals(3, layout.getColumns().size());
        Assert.assertEquals(Lists.newArrayList(7, 9, 11), allCuboids.get(0).getDimensions());
        Assert.assertEquals("{7, 9, 11}", allCuboids.get(0).getDimensionBitset().toString());
        Assert.assertEquals("unexpected colOrder", "[7, 9, 11]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);
    }
}
