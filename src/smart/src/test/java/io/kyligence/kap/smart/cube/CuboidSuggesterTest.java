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

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;

public class CuboidSuggesterTest extends NTestBase {

    @Test
    public void testSuggestShardBy() throws IOException {
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };

        // set 'kap.smart.conf.rowkey.uhc.min-cardinality' = 2000 to test
        kylinConfig.setProperty("kap.smart.conf.rowkey.uhc.min-cardinality", "2000");
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.renameModel();
        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();

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

        smartMaster.saveModel();
        smartMaster.selectCubePlan();
    }

    @Test
    public void testSuggestSortBy() throws IOException {

        String[] sqls = new String[] { "select part_dt, lstg_format_name, price from kylin_sales order by part_dt" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.renameModel();
        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();

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

        smartMaster.saveModel();
        smartMaster.selectCubePlan();

    }
}
