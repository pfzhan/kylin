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
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NTestBase;

public class CuboidSuggesterTest extends NTestBase {

    @Test
    public void testSuggestShardBy() throws IOException {
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
    public void testSuggestSortBy() throws IOException {

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
    public void testSqlPattern2Layout() throws IOException {
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
