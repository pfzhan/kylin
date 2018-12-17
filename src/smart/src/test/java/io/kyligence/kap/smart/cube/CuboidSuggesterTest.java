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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NTestBase;

public class CuboidSuggesterTest extends NTestBase {

    @Test
    public void testAggIndexSuggesetColOrder() throws IOException {

        hideTableExdInfo();
        String[] sqls = new String[] { "select lstg_format_name, buyer_id, seller_id, sum(price) from kylin_sales "
                + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name, buyer_id, seller_id" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            final NCubePlan targetCubePlan = mdCtx.getTargetCubePlan();
            final List<NCuboidDesc> allCuboids = targetCubePlan.getAllCuboids();
            final List<NCuboidLayout> layouts = allCuboids.get(0).getLayouts();
            final NCuboidLayout layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 0, 3, 9, 1000, 1001]", layout.getColOrder().toString());
        }
    }

    @Test
    public void testTableIndexSuggestColOrder() {
        String[] sqls = new String[] {
                "select ops_user_id, ops_region, price from kylin_sales where "
                        + "ops_user_id = '10009998' order by item_count, lstg_site_id",
                "select ops_user_id, ops_region, price from kylin_sales where "
                        + "part_dt = '2012-01-08' order by item_count, lstg_site_id" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final NCubePlan targetCubePlan = mdCtx.getTargetCubePlan();
        final List<NCuboidDesc> allCuboids = targetCubePlan.getAllCuboids();
        final NCuboidLayout layout = allCuboids.get(0).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[6, 1, 4, 5, 8]", layout.getColOrder().toString());

        final NCuboidLayout layout2 = allCuboids.get(1).getLayouts().get(0);
        Assert.assertEquals("unexpected colOrder", "[7, 1, 4, 5, 6, 8]", layout2.getColOrder().toString());
    }

    @Test
    public void testSuggestWithoutDimension() {
        String[] sqls = new String[] { "select count(*) from kylin_sales", // count star
                "select count(price) from kylin_sales", // measure with column
                "select sum(price) from kylin_sales", //
                "select 1 as ttt from kylin_sales" // no dimension and no measure
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        final NDataModel targetModel = mdCtx.getTargetModel();
        Assert.assertEquals(0, targetModel.getEffectiveDimenionsMap().size());
        Assert.assertEquals(3, targetModel.getEffectiveMeasureMap().size());
        Assert.assertEquals(12, targetModel.getEffectiveCols().size());

        final NCubePlan targetCubePlan = mdCtx.getTargetCubePlan();
        final List<NCuboidDesc> allCuboids = targetCubePlan.getAllCuboids();
        Assert.assertEquals(4, allCuboids.size());

        final NCuboidDesc cuboidDesc0 = allCuboids.get(0);
        Assert.assertEquals(1, cuboidDesc0.getLayouts().size());
        Assert.assertEquals(1L, cuboidDesc0.getLayouts().get(0).getId());
        Assert.assertEquals("[1000]", cuboidDesc0.getLayouts().get(0).getColOrder().toString());

        final NCuboidDesc cuboidDesc1 = allCuboids.get(1);
        Assert.assertEquals(1, cuboidDesc1.getLayouts().size());
        Assert.assertEquals(1001L, cuboidDesc1.getLayouts().get(0).getId());
        Assert.assertEquals("[1000, 1001]", cuboidDesc1.getLayouts().get(0).getColOrder().toString());

        final NCuboidDesc cuboidDesc2 = allCuboids.get(2);
        Assert.assertEquals(1, cuboidDesc2.getLayouts().size());
        Assert.assertEquals(2001L, cuboidDesc2.getLayouts().get(0).getId());
        Assert.assertEquals("[1000, 1002]", cuboidDesc2.getLayouts().get(0).getColOrder().toString());

        final NCuboidDesc cuboidDesc3 = allCuboids.get(3);
        Assert.assertEquals(1, cuboidDesc3.getLayouts().size());
        Assert.assertEquals(20000000001L, cuboidDesc3.getLayouts().get(0).getId());
        Assert.assertEquals("[0]", cuboidDesc3.getLayouts().get(0).getColOrder().toString());
    }

    @Test
    public void testMinMaxForAllTypes() {
        String[] sqls = new String[] { "select min(lstg_format_name), max(lstg_format_name) from kylin_sales",
                "select min(part_dt), max(part_dt) from kylin_sales",
                "select lstg_format_name, min(price), max(price) from kylin_sales group by lstg_format_name",
                "select min(seller_id), max(seller_id) from kylin_sales" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
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

        NCubePlan cubePlan = mdCtx.getTargetCubePlan();
        List<NCuboidDesc> allCuboids = cubePlan.getCuboids();
        final NCuboidDesc cuboidDesc0 = allCuboids.get(0);

        Assert.assertEquals("{1000, 1001, 1002}", cuboidDesc0.getMeasureBitset().toString());
        Assert.assertEquals(1, cuboidDesc0.getLayouts().size());
        Assert.assertEquals(1L, cuboidDesc0.getLayouts().get(0).getId());

        final NCuboidDesc cuboidDesc1 = allCuboids.get(1);
        Assert.assertEquals("{1000, 1003, 1004}", cuboidDesc1.getMeasureBitset().toString());
        Assert.assertEquals(1, cuboidDesc1.getLayouts().size());
        Assert.assertEquals(1001L, cuboidDesc1.getLayouts().get(0).getId());

        final NCuboidDesc cuboidDesc2 = allCuboids.get(2);
        Assert.assertEquals("{1000, 1005, 1006}", cuboidDesc2.getMeasureBitset().toString());
        Assert.assertEquals(1, cuboidDesc2.getLayouts().size());
        Assert.assertEquals(2001L, cuboidDesc2.getLayouts().get(0).getId());

        final NCuboidDesc cuboidDesc3 = allCuboids.get(3);
        Assert.assertEquals("{1000, 1007, 1008}", cuboidDesc3.getMeasureBitset().toString());
        Assert.assertEquals(1, cuboidDesc3.getLayouts().size());
        Assert.assertEquals(3001L, cuboidDesc3.getLayouts().get(0).getId());
    }

    @Test
    public void testSuggestShardBy() {
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };

        // set 'kap.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        kylinConfig.setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", "2000");
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        NCubePlan cubePlan = mdCtx.getTargetCubePlan();
        Assert.assertNotNull(cubePlan);
        Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

        List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
        Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());

        final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
        Assert.assertEquals("unmatched layouts size", 1, layouts.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts.get(0).getShardByColumns().size());
        Assert.assertEquals("unexpected identity name of shard by column", "KYLIN_SALES.PRICE", mdCtx.getTargetModel()
                .getEffectiveColsMap().get(layouts.get(0).getShardByColumns().get(0)).getIdentity());
    }

    @Test
    public void testSuggestSortBy() {

        String[] sqls = new String[] { "select part_dt, lstg_format_name, price from kylin_sales order by part_dt" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        NCubePlan cubePlan = mdCtx.getTargetCubePlan();
        Assert.assertNotNull(cubePlan);
        Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

        List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
        Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());

        final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
        Assert.assertEquals("unmatched layouts size", 1, layouts.size());
        Assert.assertEquals("unmatched shard by columns size", 1, layouts.get(0).getSortByColumns().size());
        Assert.assertEquals("unexpected identity name of sort by column", "KYLIN_SALES.PART_DT", mdCtx.getTargetModel()
                .getEffectiveColsMap().get(layouts.get(0).getSortByColumns().get(0)).getIdentity());
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
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();

        // validate sql pattern to layout
        final Map<String, AccelerateInfo> accelerateMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(5, accelerateMap.size());
        Assert.assertEquals(1, accelerateMap.get(sqls[0]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[1]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[2]).getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateMap.get(sqls[3]).getRelatedLayouts().size());
        Assert.assertEquals(0, accelerateMap.get(sqls[4]).getRelatedLayouts().size());
        Assert.assertTrue(accelerateMap.get(sqls[4]).isBlocked());

        String cubePlan0 = Lists.newArrayList(accelerateMap.get(sqls[0]).getRelatedLayouts()).get(0).getCubePlanId();
        String cubePlan1 = Lists.newArrayList(accelerateMap.get(sqls[1]).getRelatedLayouts()).get(0).getCubePlanId();
        String cubePlan2 = Lists.newArrayList(accelerateMap.get(sqls[2]).getRelatedLayouts()).get(0).getCubePlanId();
        String cubePlan3 = Lists.newArrayList(accelerateMap.get(sqls[3]).getRelatedLayouts()).get(0).getCubePlanId();
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
        Assert.assertEquals(1001L, layout3);
    }
}
