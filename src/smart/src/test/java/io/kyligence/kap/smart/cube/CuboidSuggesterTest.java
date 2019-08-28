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

package io.kyligence.kap.smart.cube;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import lombok.val;

public class CuboidSuggesterTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testAggIndexSuggesetColOrder() {

        String[] sqls = new String[] { "select lstg_format_name, buyer_id, seller_id, sum(price) from kylin_sales "
                + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name, buyer_id, seller_id" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
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
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[6, 1, 4, 5, 8]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);

        final LayoutEntity layout2 = allCuboids.get(1).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[7, 1, 4, 5, 6, 8]", layout2.getColOrder().toString());
        Assert.assertTrue(layout2.getUpdateTime() > 0);
    }

    @Test
    public void testAggIndexSuggestColOrder() {
        String[] sqls = new String[] {
                "SELECT COUNT(KYLIN_ACCOUNT.ACCOUNT_COUNTRY), KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL\n" +
                        "FROM (\n" +
                        "\tSELECT PRICE, TRANS_ID, SELLER_ID FROM KYLIN_SALES ORDER BY TRANS_ID DESC\n" +
                        "\t) FACT\n" +
                        "INNER JOIN KYLIN_ACCOUNT\n" +
                        "ON KYLIN_ACCOUNT.ACCOUNT_ID = FACT.SELLER_ID\n" +
                        "GROUP BY KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL\n" +
                        "ORDER BY KYLIN_ACCOUNT.ACCOUNT_SELLER_LEVEL"
                        };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        final LayoutEntity layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[4, 100000, 100001]", layout.getColOrder().toString());
        Assert.assertTrue(layout.getUpdateTime() > 0);
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
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, project, sqls);

        val tableManager = NTableMetadataManager.getInstance(kylinConfig, project);
        val tableTestKylinFact = tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        tableTestKylinFact.setIncrementLoading(true);
        tableManager.updateTableDesc(tableTestKylinFact);

        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
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
        String[] sqls = new String[] { "select count(*) from kylin_sales", // count star
                "select count(price) from kylin_sales", // measure with column
                "select sum(price) from kylin_sales", //
                "select 1 as ttt from kylin_sales" // no dimension and no measure, but will suggest count(*)
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final NDataModel targetModel = mdCtx.getTargetModel();
        Assert.assertEquals(0, targetModel.getEffectiveDimenionsMap().size());
        Assert.assertEquals(3, targetModel.getEffectiveMeasureMap().size());
        Assert.assertEquals(12, targetModel.getEffectiveCols().size());

        final IndexPlan targetIndexPlan = mdCtx.getTargetIndexPlan();
        final List<IndexEntity> allCuboids = targetIndexPlan.getAllIndexes();
        Assert.assertEquals(4, allCuboids.size());

        final IndexEntity indexEntity0 = allCuboids.get(0);
        Assert.assertEquals(1, indexEntity0.getLayouts().size());
        Assert.assertEquals(1L, indexEntity0.getLayouts().get(0).getId());
        Assert.assertEquals("[100000]", indexEntity0.getLayouts().get(0).getColOrder().toString());

        final IndexEntity indexEntity1 = allCuboids.get(1);
        Assert.assertEquals(1, indexEntity1.getLayouts().size());
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP + 1, indexEntity1.getLayouts().get(0).getId());
        Assert.assertEquals("[100000, 100001]", indexEntity1.getLayouts().get(0).getColOrder().toString());

        final IndexEntity indexEntity2 = allCuboids.get(2);
        Assert.assertEquals(1, indexEntity2.getLayouts().size());
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP * 2 + 1, indexEntity2.getLayouts().get(0).getId());
        Assert.assertEquals("[100000, 100002]", indexEntity2.getLayouts().get(0).getColOrder().toString());

        final IndexEntity indexEntity3 = allCuboids.get(3);
        Assert.assertEquals(1, indexEntity3.getLayouts().size());
        Assert.assertEquals(IndexEntity.TABLE_INDEX_START_ID + 1, indexEntity3.getLayouts().get(0).getId());
        Assert.assertEquals("[0]", indexEntity3.getLayouts().get(0).getColOrder().toString());
    }

    @Test
    public void testMinMaxForAllTypes() {
        String[] sqls = new String[] { "select min(lstg_format_name), max(lstg_format_name) from kylin_sales",
                "select min(part_dt), max(part_dt) from kylin_sales",
                "select lstg_format_name, min(price), max(price) from kylin_sales group by lstg_format_name",
                "select min(seller_id), max(seller_id) from kylin_sales" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final List<NDataModel.Measure> allMeasures = mdCtx.getTargetModel().getAllMeasures();
        Assert.assertEquals(9, allMeasures.size());
        Assert.assertEquals("COUNT_ALL", allMeasures.get(0).getName());
        Assert.assertEquals("MIN_LSTG_FORMAT_NAME", allMeasures.get(1).getName());
        Assert.assertEquals("MAX_LSTG_FORMAT_NAME", allMeasures.get(2).getName());
        Assert.assertEquals("MIN_PART_DT", allMeasures.get(3).getName());
        Assert.assertEquals("MAX_PART_DT", allMeasures.get(4).getName());
        Assert.assertEquals("MIN_PRICE", allMeasures.get(5).getName());
        Assert.assertEquals("MAX_PRICE", allMeasures.get(6).getName());
        Assert.assertEquals("MIN_SELLER_ID", allMeasures.get(7).getName());
        Assert.assertEquals("MAX_SELLER_ID", allMeasures.get(8).getName());

        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        List<IndexEntity> allCuboids = indexPlan.getIndexes();
        final IndexEntity indexEntity0 = allCuboids.get(0);

        Assert.assertEquals("{100000, 100001, 100002}", indexEntity0.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity0.getLayouts().size());
        Assert.assertEquals(1L, indexEntity0.getLayouts().get(0).getId());

        final IndexEntity indexEntity1 = allCuboids.get(1);
        Assert.assertEquals("{100000, 100003, 100004}", indexEntity1.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity1.getLayouts().size());
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP + 1, indexEntity1.getLayouts().get(0).getId());

        final IndexEntity indexEntity2 = allCuboids.get(2);
        Assert.assertEquals("{100000, 100005, 100006}", indexEntity2.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity2.getLayouts().size());
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP * 2 + 1, indexEntity2.getLayouts().get(0).getId());

        final IndexEntity indexEntity3 = allCuboids.get(3);
        Assert.assertEquals("{100000, 100007, 100008}", indexEntity3.getMeasureBitset().toString());
        Assert.assertEquals(1, indexEntity3.getLayouts().size());
        Assert.assertEquals(IndexEntity.INDEX_ID_STEP * 3 + 1, indexEntity3.getLayouts().get(0).getId());
    }

    @Test
    public void testSuggestShardBy() {

        // set 'kap.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        // currently, column part_dt's cardinality < 2000 && tans_id's > 2000
        getTestConfig().setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", "2000");

        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, trans_id from kylin_sales where part_dt = '2012-01-01'",
                "select part_dt, trans_id from kylin_sales where trans_id = 100000",
                "select part_dt, lstg_format_name, trans_id from kylin_sales",
                "select part_dt, lstg_format_name, trans_id from kylin_sales where trans_id = 100000 group by part_dt, lstg_format_name, trans_id" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();
        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
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
                .getTargetModel().getEffectiveColsMap().get(layouts1.get(0).getShardByColumns().get(0)).getIdentity());

        // case 2. AggIndex with eq-filter and high cardinality
        final List<LayoutEntity> layouts2 = indexEntities.get(2).getLayouts();
        Assert.assertFalse(indexEntities.get(2).isTableIndex());
        Assert.assertEquals("unmatched layouts size", 1, layouts2.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts2.get(0).getShardByColumns().size());
        Assert.assertEquals("unexpected identity name of shard by column", "KYLIN_SALES.TRANS_ID", mdCtx
                .getTargetModel().getEffectiveColsMap().get(layouts2.get(0).getShardByColumns().get(0)).getIdentity());
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
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);
        smartMaster.runAll();

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
}
