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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.NTestBase;
import lombok.val;

public class NCuboidRefresherTest extends NTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NCuboidRefresherTest.class);

    private static final int MAX_TRY_NUM = 3;

    @Test
    public void testSingleTimeLineTableIndex() throws Exception {

        hideTableExdInfo();

        //------------propose-----------------------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.runAll();

        //------------validate propose result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDescs.get(0).getLayouts().size());

            final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
            final NCuboidLayout layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());

            val fqRealizations = collectFavoriteQueryRealizations(layouts);
            Assert.assertEquals(1, fqRealizations.size());
        }

        showTableExdInfo();

        //------------update-----------------------------------------
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.selectModelAndCubePlan();
        smartMaster.refreshCubePlan();

        //------------validate update result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDescs.get(0).getLayouts().size());

            final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
            final NCuboidLayout layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();
            smartMaster.saveAccelerateInfo();

            val fqRealizations = collectFavoriteQueryRealizations(layouts);
            Assert.assertEquals(1, fqRealizations.size());
        }
    }

    @Test
    public void testSingleTimeLineAggIndex() throws Exception {

        hideTableExdInfo();

        //------------propose-----------------------------------------
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name" };
        initFQData(sqls);
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.runAll();

        //------------validate propose result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", 0, cuboidDescs.get(0).getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDescs.get(0).getLayouts().size());

            final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
            final NCuboidLayout layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 1000, 1001]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", 1, layout.getId());

            val fqRealizations = collectFavoriteQueryRealizations(layouts);
            Assert.assertEquals(1, fqRealizations.size());
        }

        showTableExdInfo();

        //------------update-----------------------------------------
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.selectModelAndCubePlan();
        smartMaster.refreshCubePlan();

        //------------validate update result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", 0, cuboidDescs.get(0).getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDescs.get(0).getLayouts().size());

            final List<NCuboidLayout> layouts = cuboidDescs.get(0).getLayouts();
            final NCuboidLayout layout = layouts.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 1000, 1001]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", 1, layout.getId());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();
            smartMaster.saveAccelerateInfo();

            val fqRealizations = collectFavoriteQueryRealizations(layouts);
            Assert.assertEquals(1, fqRealizations.size());
        }
    }

    /**
     * one line but batch of sql
     */
    @Test
    public void testSingleTimeLineWithBatchSql() throws Exception {

        hideTableExdInfo();

        //----------------propose------------------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name",
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'",
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" };
        initFQData(sqls);
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.runAll();

        //------------validate propose result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            final List<NCuboidLayout> allLayouts = collectAllLayouts(cuboidDescs);
            Assert.assertEquals("unmatched cuboids size", 3, cuboidDescs.size());
            Assert.assertEquals("unmatched layouts size", 4, allLayouts.size());

            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(4, realizations.size());
        }

        showTableExdInfo();

        //------------update-----------------------------------------
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.selectModelAndCubePlan();
        smartMaster.refreshCubePlan();

        //------------validate update result-------------------------
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            final List<NCuboidLayout> allLayouts = collectAllLayouts(cuboidDescs);
            Assert.assertEquals("unmatched cuboids size", 3, cuboidDescs.size());
            Assert.assertEquals("unmatched layouts size", 4, allLayouts.size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();
            smartMaster.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(4, realizations.size());

            val list = Lists.newArrayList(realizations);
            list.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            Assert.assertEquals(1, list.get(0).getCuboidLayoutId());
            Assert.assertEquals(2, list.get(1).getCuboidLayoutId());
            Assert.assertEquals(1001, list.get(2).getCuboidLayoutId());
            Assert.assertEquals(2001, list.get(3).getCuboidLayoutId());
        }
    }

    /**
     * test parallel refresh operate interrupted by each other, the course time line as follows:
     * ------------------t1----------t2--------------t3-----------t4-------
     * -                 |           |               |            |       -
     * -             propose(A)      |           refresh(A)       |       -
     * -                         propose(B)                   refresh(B)  -
     */
    @Test
    public void testParallelTimeLineWithSimilarSqlCase1() throws Exception {

        hideTableExdInfo();

        // -----------t1: line A propose-------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        smartMasterA.runAll();

        //------------t2: line B propose-------------------------
        String draftVersionB = UUID.randomUUID().toString();
        String[] otherSqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt < '2012-01-02'" };
        initFQData(otherSqls);
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        smartMasterB.runAll();

        Assert.assertEquals("should return a equal model name, but failed",
                smartMasterA.getContext().getModelContexts().get(0).getTargetModel().getName(),
                smartMasterB.getContext().getModelContexts().get(0).getTargetModel().getName());
        Assert.assertNotEquals("should return a unequal draft version, but failed",
                smartMasterA.getContext().getDraftVersion(), smartMasterB.getContext().getDraftVersion());
        Assert.assertEquals("layout draft id not from result after t1", draftVersionA,
                smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0).getLayouts()
                        .get(0).getDraftVersion());

        // get favorite query realization relationships from database and validate them
        List<NCuboidLayout> allLayoutsAfterRunAllB = smartMasterB.getContext().getModelContexts().get(0)
                .getTargetCubePlan().getCuboids().get(0).getLayouts();
        val realizations = collectFavoriteQueryRealizations(allLayoutsAfterRunAllB);
        Assert.assertEquals(2, realizations.size());

        val list = Lists.newArrayList(realizations);
        list.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
        final FavoriteQueryRealization queryRealization1 = list.get(0);
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, queryRealization1.getCuboidLayoutId());
        final FavoriteQueryRealization queryRealization2 = list.get(1);
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, queryRealization2.getCuboidLayoutId());

        showTableExdInfo();

        // -----------t3: line A update and validate-------------------------
        {
            smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
            smartMasterA.selectModelAndCubePlan();
            Assert.assertEquals("layout version has been modified unexpected", draftVersionA,
                    smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0)
                            .getLayouts().get(0).getDraftVersion());

            smartMasterA.refreshCubePlan();
            NSmartContext ctx = smartMasterA.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());
            Assert.assertEquals("unmatched layouts size", 2, cuboidDescs.get(0).getLayouts().size());

            final List<NCuboidLayout> layoutsAfterRefreshA = cuboidDescs.get(0).getLayouts();
            final NCuboidLayout layout = layoutsAfterRefreshA.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 1, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 2, layout.getId());
            Assert.assertEquals("unexpected draft version", draftVersionB, layout.getDraftVersion());

            final NCuboidLayout layout2 = layoutsAfterRefreshA.get(1);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 3, layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterA.saveModel();
            smartMasterA.saveCubePlan();
            smartMasterA.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshA = collectFavoriteQueryRealizations(layoutsAfterRefreshA);
            Assert.assertEquals(2, realizationsAfterRefreshA.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshA);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 3, fqRealization2.getCuboidLayoutId());
        }

        // -----------t4: line B update and validate-------------------------
        {
            smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
            smartMasterB.selectModelAndCubePlan();
            final List<NCuboidLayout> layoutsBefore = smartMasterB.getContext().getModelContexts().get(0)
                    .getTargetCubePlan().getCuboids().get(0).getLayouts();
            Assert.assertEquals("unexpected draft version", draftVersionB, layoutsBefore.get(0).getDraftVersion());
            Assert.assertNull("line A published error(in t3 stage)", layoutsBefore.get(1).getDraftVersion());

            smartMasterB.refreshCubePlan();
            NSmartContext ctx = smartMasterB.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshB = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 2, layoutsAfterRefreshB.size());

            final NCuboidLayout layout = layoutsAfterRefreshB.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 3, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            final NCuboidLayout layout2 = layoutsAfterRefreshB.get(1);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 1, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 4, layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterB.saveModel();
            smartMasterB.saveCubePlan();
            smartMasterB.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshB = collectFavoriteQueryRealizations(layoutsAfterRefreshB);
            Assert.assertEquals(2, realizationsAfterRefreshB.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshB);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 3, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 4, fqRealization2.getCuboidLayoutId());
        }
    }

    /**
     * test parallel refresh operate only A is interrupted, the course time line as follows:
     * ------------------t1----------t2--------------t3-----------t4-------
     * -                 |           |               |            |       -
     * -             propose(A)      |               |        refresh(A)  -
     * -                         propose(B)      refresh(B)               -
     */
    @Test
    public void testParallelTimeLineWithSimilarSqlCase2() throws Exception {

        hideTableExdInfo();

        // -----------t1: line A propose-------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        smartMasterA.runAll();

        //------------t2: line B propose-------------------------
        String draftVersionB = UUID.randomUUID().toString();
        String[] otherSqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt <= '2012-01-02'" };
        initFQData(otherSqls);
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        smartMasterB.runAll();

        Assert.assertEquals("get unequal model name",
                smartMasterA.getContext().getModelContexts().get(0).getTargetModel().getName(),
                smartMasterB.getContext().getModelContexts().get(0).getTargetModel().getName());
        Assert.assertNotEquals("different draftVersion", smartMasterA.getContext().getDraftVersion(),
                smartMasterB.getContext().getDraftVersion());
        Assert.assertEquals("layout draft id not from result after t1", draftVersionA,
                smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0).getLayouts()
                        .get(0).getDraftVersion());

        showTableExdInfo();

        // -----------t3: line B update and validate-------------------------
        {
            smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
            smartMasterB.selectModelAndCubePlan();
            Assert.assertEquals("layout draft id not from result after t1", draftVersionA,
                    smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0)
                            .getLayouts().get(0).getDraftVersion());

            smartMasterB.refreshCubePlan();
            NSmartContext ctx = smartMasterB.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshB = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 2, layoutsAfterRefreshB.size());

            final NCuboidLayout layout = layoutsAfterRefreshB.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());
            Assert.assertEquals("unexpected draft id", draftVersionA, layout.getDraftVersion());

            final NCuboidLayout layout2 = layoutsAfterRefreshB.get(1);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 1, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 2, layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterB.saveModel();
            smartMasterB.saveCubePlan();
            smartMasterB.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshB = collectFavoriteQueryRealizations(layoutsAfterRefreshB);
            Assert.assertEquals(2, realizationsAfterRefreshB.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshB);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, fqRealization2.getCuboidLayoutId());
        }

        // -----------t4: line A update and validate-------------------------
        {
            smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
            smartMasterA.selectModelAndCubePlan();
            Assert.assertEquals("until now the layout should not be published", draftVersionA,
                    smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0)
                            .getLayouts().get(0).getDraftVersion());

            smartMasterA.refreshCubePlan();
            NSmartContext ctx = smartMasterA.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshA = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 2, layoutsAfterRefreshA.size());

            final NCuboidLayout layout = layoutsAfterRefreshA.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 1, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 2, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            final NCuboidLayout layout2 = layoutsAfterRefreshA.get(1);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 3, layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterA.saveModel();
            smartMasterA.saveCubePlan();
            smartMasterA.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshA = collectFavoriteQueryRealizations(layoutsAfterRefreshA);
            Assert.assertEquals(2, realizationsAfterRefreshA.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshA);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 3, fqRealization2.getCuboidLayoutId());
        }
    }

    /**
     * test parallel unrelated refresh operate, the course time line as follows:
     * ------------------t1----------t2--------------t3-----------t4-------
     * -                 |           |               |            |       -
     * -             propose(A)  refresh(A)          |            |       -
     * -                                         propose(B)  refresh(B)   -
     */
    @Test
    public void testParallelTimeLineWithSimilarSqlCase3() throws Exception {
        hideTableExdInfo();

        // -----------t1: line A propose-------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        smartMasterA.runAll();

        // -----------t2: line A update and validate-------------------------
        {
            smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
            smartMasterA.selectModelAndCubePlan();
            Assert.assertEquals("until now the layout should not be published", draftVersionA,
                    smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0)
                            .getLayouts().get(0).getDraftVersion());

            smartMasterA.refreshCubePlan();
            NSmartContext ctx = smartMasterA.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshA = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 1, layoutsAfterRefreshA.size());

            final NCuboidLayout layout = layoutsAfterRefreshA.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            smartMasterA.saveModel();
            smartMasterA.saveCubePlan();
            smartMasterA.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshA = collectFavoriteQueryRealizations(layoutsAfterRefreshA);
            Assert.assertEquals(1, realizationsAfterRefreshA.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshA);
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, fqRealization1.getCuboidLayoutId());
        }

        //------------t3: line B propose-------------------------
        String draftVersionB = UUID.randomUUID().toString();
        String[] otherSqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt <= '2012-01-02'" };
        initFQData(otherSqls);
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        smartMasterB.runAll();

        Assert.assertEquals("get unequal model name",
                smartMasterA.getContext().getModelContexts().get(0).getTargetModel().getName(),
                smartMasterB.getContext().getModelContexts().get(0).getTargetModel().getName());
        Assert.assertNotEquals("different draftVersion", smartMasterA.getContext().getDraftVersion(),
                smartMasterB.getContext().getDraftVersion());
        Assert.assertNull("should be null, but unexpected draft id", smartMasterB.getContext().getModelContexts().get(0)
                .getTargetCubePlan().getCuboids().get(0).getLayouts().get(0).getDraftVersion());

        showTableExdInfo();

        // -----------t4: line B update and validate-------------------------
        {
            smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
            smartMasterB.selectModelAndCubePlan();
            Assert.assertNull("should be null, but unexpected draft id", smartMasterB.getContext().getModelContexts()
                    .get(0).getTargetCubePlan().getCuboids().get(0).getLayouts().get(0).getDraftVersion());

            smartMasterB.refreshCubePlan();
            NSmartContext ctx = smartMasterB.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 1, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshB = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 2, layoutsAfterRefreshB.size());

            final NCuboidLayout layout = layoutsAfterRefreshB.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            final NCuboidLayout layout2 = layoutsAfterRefreshB.get(1);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 1, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 2, layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterB.saveModel();
            smartMasterB.saveCubePlan();
            smartMasterB.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshB = collectFavoriteQueryRealizations(layoutsAfterRefreshB);
            Assert.assertEquals(2, realizationsAfterRefreshB.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshB);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, fqRealization2.getCuboidLayoutId());
        }
    }

    /**
     * test parallel unrelated refresh operate, due to two line is independent, just give one case
     * to show the process, the course time line as follows:
     * ------------------t1----------t2--------------t3-----------t4-------
     * -                 |           |               |            |       -
     * -             propose(A)      |           refresh(A)       |       -
     * -                          propose(B)                  refresh(B)  -
     * <p>
     * it will create two cuboid, each has one published layout
     */
    @Test
    public void testParallelTimeLineWithDifferentSqlCase() throws Exception {
        hideTableExdInfo();

        // -----------t1: line A propose-------------------------
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        smartMasterA.runAll();

        //------------t2: line B propose-------------------------
        String draftVersionB = UUID.randomUUID().toString();
        String[] otherSqls = new String[] {
                "select lstg_format_name, part_dt, price, item_count from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(otherSqls);
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        smartMasterB.runAll();

        Assert.assertEquals("get unequal model name",
                smartMasterA.getContext().getModelContexts().get(0).getTargetModel().getName(),
                smartMasterB.getContext().getModelContexts().get(0).getTargetModel().getName());
        Assert.assertNotEquals("different draftVersion", smartMasterA.getContext().getDraftVersion(),
                smartMasterB.getContext().getDraftVersion());
        Assert.assertEquals("layout draft id not from result after t1", draftVersionA,
                smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0).getLayouts()
                        .get(0).getDraftVersion());

        showTableExdInfo();

        // -----------t3: line A update and validate-------------------------
        {
            smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
            smartMasterA.selectModelAndCubePlan();
            Assert.assertEquals("layout version has been modified unexpected", draftVersionA,
                    smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan().getCuboids().get(0)
                            .getLayouts().get(0).getDraftVersion());

            smartMasterA.refreshCubePlan();
            NSmartContext ctx = smartMasterA.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 2, cuboidDescs.size());
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDescs.get(0).getId());

            final List<NCuboidLayout> layoutsAfterRefreshA = cuboidDescs.get(0).getLayouts();
            Assert.assertEquals("unmatched layouts size", 1, layoutsAfterRefreshA.size());

            final NCuboidLayout layout = layoutsAfterRefreshA.get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            smartMasterA.saveModel();
            smartMasterA.saveCubePlan();
            smartMasterA.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizationsAfterRefreshA = collectFavoriteQueryRealizations(layoutsAfterRefreshA);
            Assert.assertEquals(1, realizationsAfterRefreshA.size());
            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshA);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, queryRealizationsList.get(0).getCuboidLayoutId());
        }

        // -----------t4: line B update and validate-------------------------
        {
            smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
            smartMasterB.selectModelAndCubePlan();
            Assert.assertNull("line A published error(in t3 stage)", smartMasterB.getContext().getModelContexts().get(0)
                    .getTargetCubePlan().getCuboids().get(0).getLayouts().get(0).getDraftVersion());

            smartMasterB.refreshCubePlan();
            NSmartContext ctx = smartMasterB.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals("unmatched cuboids size", 2, cuboidDescs.size());

            final NCuboidDesc cuboidDesc = cuboidDescs.get(0);
            Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDesc.getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDesc.getLayouts().size());

            final NCuboidLayout layout = cuboidDesc.getLayouts().get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 1, layout.getId());
            Assert.assertNull("not published error", layout.getDraftVersion());

            final NCuboidDesc cuboidDesc2 = cuboidDescs.get(1);
            Assert.assertEquals("unmatched cuboid id",
                    NCuboidDesc.TABLE_INDEX_START_ID + NCuboidDesc.CUBOID_DESC_ID_STEP, cuboidDesc2.getId());
            Assert.assertEquals("unmatched layouts size", 1, cuboidDesc2.getLayouts().size());

            final NCuboidLayout layout2 = cuboidDesc2.getLayouts().get(0);
            Assert.assertEquals("unexpected colOrder", "[7, 1, 3, 8]", layout2.getColOrder().toString());
            Assert.assertEquals("unexpected override indices", 0, layout2.getLayoutOverrideIndices().size());
            Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + NCuboidDesc.CUBOID_DESC_ID_STEP + 1,
                    layout2.getId());
            Assert.assertNull("not published error", layout2.getDraftVersion());

            smartMasterB.saveModel();
            smartMasterB.saveCubePlan();
            smartMasterB.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            final List<NCuboidLayout> layoutsAfterRefreshB = Lists.newArrayList();
            layoutsAfterRefreshB.add(layout);
            layoutsAfterRefreshB.add(layout2);
            val realizationsAfterRefreshB = collectFavoriteQueryRealizations(layoutsAfterRefreshB);
            Assert.assertEquals(2, realizationsAfterRefreshB.size());

            val queryRealizationsList = Lists.newArrayList(realizationsAfterRefreshB);
            queryRealizationsList.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            final FavoriteQueryRealization fqRealization1 = queryRealizationsList.get(0);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, fqRealization1.getCuboidLayoutId());
            final FavoriteQueryRealization fqRealization2 = queryRealizationsList.get(1);
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + NCuboidDesc.CUBOID_DESC_ID_STEP + 1,
                    fqRealization2.getCuboidLayoutId());
        }
    }

    /**
     * parallel line but batch of sql, give one case as follow:
     * ------------------t1----------t2--------------t3-----------t4-------
     * -                 |           |               |            |       -
     * -             propose(A)      |           refresh(A)       |       -
     * -                          propose(B)                  refresh(B)  -
     */
    @Test
    public void testParallelTimeLineWithBatchSqls() throws Exception {

        hideTableExdInfo();

        // -----------t1: line A propose-------------------------
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
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        smartMasterA.runAll();
        {
            final NCubePlan targetCubePlan = smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan();
            Assert.assertEquals(3, targetCubePlan.getAllCuboids().size());
            checkDraftOfEachLayout(draftVersionA, targetCubePlan.getAllCuboids());

            // get favorite query realization relationships from database and validate them
            val allLayouts = collectAllLayouts(targetCubePlan.getAllCuboids());
            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(5, realizations.size());

            val list = Lists.newArrayList(realizations);
            list.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
            Assert.assertEquals(1, list.get(0).getCuboidLayoutId());
            Assert.assertEquals(1, list.get(1).getCuboidLayoutId());
            Assert.assertEquals(2, list.get(2).getCuboidLayoutId());
            Assert.assertEquals(1001, list.get(3).getCuboidLayoutId());
            Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, list.get(4).getCuboidLayoutId());
        }

        //------------t2: line B propose-------------------------
        String draftVersionB = UUID.randomUUID().toString();
        String[] otherSqls = new String[] {

                "select lstg_format_name, part_dt, price from kylin_sales where part_dt = '2012-01-01'",
                "select lstg_format_name, part_dt, price, item_count from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(otherSqls);
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        smartMasterB.runAll();
        {
            Assert.assertEquals("get unequal model name",
                    smartMasterA.getContext().getModelContexts().get(0).getTargetModel().getName(),
                    smartMasterB.getContext().getModelContexts().get(0).getTargetModel().getName());
            Assert.assertNotEquals("different draftVersion", smartMasterA.getContext().getDraftVersion(),
                    smartMasterB.getContext().getDraftVersion());

            final NCubePlan targetCubePlan = smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan();
            final List<NCuboidDesc> allCuboids = targetCubePlan.getAllCuboids();
            Assert.assertEquals(4, allCuboids.size());

            final NCuboidDesc lastCuboid = allCuboids.get(3);
            allCuboids.remove(3);
            checkDraftOfEachLayout(draftVersionA, allCuboids);
            checkDraftOfEachLayout(draftVersionB, Lists.newArrayList(lastCuboid));

            // get favorite query realization relationships from database and validate them
            val allLayouts = collectAllLayouts(targetCubePlan.getAllCuboids());
            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(7, realizations.size());
            validateRealizations(realizations);
        }

        showTableExdInfo();

        // -----------t3: line A update and validate-------------------------
        {
            smartMasterA = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
            smartMasterA.selectModelAndCubePlan();

            final NCubePlan targetCubePlan = smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan();
            final List<NCuboidDesc> allCuboidsBeforeRefresh = targetCubePlan.getAllCuboids();
            Assert.assertEquals("unmatched cuboids size", 4, allCuboidsBeforeRefresh.size());
            final NCuboidDesc lastCuboid = allCuboidsBeforeRefresh.get(3);
            allCuboidsBeforeRefresh.remove(3);
            checkDraftOfEachLayout(draftVersionA, allCuboidsBeforeRefresh);
            checkDraftOfEachLayout(draftVersionB, Lists.newArrayList(lastCuboid));

            smartMasterA.refreshCubePlan();

            NCubePlan cubePlan = smartMasterA.getContext().getModelContexts().get(0).getTargetCubePlan();
            List<NCuboidDesc> allCuboidsAfterRefresh = cubePlan.getAllCuboids();
            val allLayouts = collectAllLayouts(allCuboidsAfterRefresh);
            Assert.assertEquals("unmatched cuboids size", 4, allCuboidsAfterRefresh.size());
            allCuboidsAfterRefresh.remove(3);
            checkDraftOfEachLayout(null, allCuboidsAfterRefresh);
            checkDraftOfEachLayout(draftVersionB, Lists.newArrayList(lastCuboid));

            smartMasterA.saveModel();
            smartMasterA.saveCubePlan();
            smartMasterA.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(7, realizations.size());
            validateRealizations(realizations);
        }

        // -----------t4: line B update and validate-------------------------
        {
            smartMasterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
            smartMasterB.selectModelAndCubePlan();

            final NCubePlan targetCubePlan = smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan();
            final List<NCuboidDesc> allCuboidsBeforeRefresh = targetCubePlan.getAllCuboids();
            Assert.assertEquals("unmatched cuboids size", 4, allCuboidsBeforeRefresh.size());
            final NCuboidDesc lastCuboid = allCuboidsBeforeRefresh.get(3);
            allCuboidsBeforeRefresh.remove(3);
            checkDraftOfEachLayout(null, allCuboidsBeforeRefresh);
            checkDraftOfEachLayout(draftVersionB, Lists.newArrayList(lastCuboid));

            smartMasterB.refreshCubePlan();

            NCubePlan cubePlan = smartMasterB.getContext().getModelContexts().get(0).getTargetCubePlan();
            List<NCuboidDesc> allCuboidsAfterRefresh = cubePlan.getAllCuboids();
            val allLayouts = collectAllLayouts(allCuboidsAfterRefresh);
            Assert.assertEquals("unmatched cuboids size", 4, allCuboidsAfterRefresh.size());
            final NCuboidDesc lastCuboidAfterRefresh = allCuboidsAfterRefresh.get(3);
            allCuboidsAfterRefresh.remove(3);
            checkDraftOfEachLayout(null, allCuboidsAfterRefresh);
            checkDraftOfEachLayout(null, Lists.newArrayList(lastCuboidAfterRefresh));

            smartMasterB.saveModel();
            smartMasterB.saveCubePlan();
            smartMasterB.saveAccelerateInfo();

            // get favorite query realization relationships from database and validate them
            val realizations = collectFavoriteQueryRealizations(allLayouts);
            Assert.assertEquals(7, realizations.size());
            validateRealizations(realizations);
        }
    }

    private void validateRealizations(Set<FavoriteQueryRealization> realizations) {
        val list = Lists.newArrayList(realizations);
        list.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
        Assert.assertEquals(1, list.get(0).getCuboidLayoutId());
        Assert.assertEquals(1, list.get(1).getCuboidLayoutId());
        Assert.assertEquals(2, list.get(2).getCuboidLayoutId());
        Assert.assertEquals(1001, list.get(3).getCuboidLayoutId());
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, list.get(4).getCuboidLayoutId());
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 1, list.get(5).getCuboidLayoutId());
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + NCuboidDesc.CUBOID_DESC_ID_STEP + 1,
                list.get(6).getCuboidLayoutId());
    }

    /**
     * Parallel line need retry refresh. When saving refresh result, refresh(B) modified the
     * metadata info, thus the refresh program of A need retry. Give one case as follow:
     * ------------t1----------t2---------t3---------t4---------t5--------t6-----
     * -           |           |          |          |          |         |     -
     * -      propose(A)       |       refresh(A)    |          |      save(A)  -
     * -                   propose(B)            refresh(B)   save(B)           -
     */
    @Test
    public void testParallelTimeLineRetryWithMultiThread()
            throws IOException, ExecutionException, InterruptedException {

        hideTableExdInfo();

        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        initFQData(sqls);
        String draftVersionA = "a";
        NSmartMaster master = new NSmartMaster(kylinConfig, proj, sqls, draftVersionA);
        master.runAll();

        String[] otherSqls = new String[] {
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt < '2012-01-02'" };
        initFQData(otherSqls);
        String draftVersionB = "b";
        NSmartMaster masterB = new NSmartMaster(kylinConfig, proj, otherSqls, draftVersionB);
        masterB.runAll();

        showTableExdInfo();

        ExecutorService service = Executors.newCachedThreadPool();
        Future futureA = service.submit(() -> {
            try {
                course(sqls, draftVersionA, 2, 10);
            } catch (InterruptedException e) {
                logger.debug("interrupt exception" + draftVersionA, e);
            }
        });

        Future futureB = service.submit(() -> {
            try {
                course(otherSqls, draftVersionB, 3, 3);
            } catch (InterruptedException e) {
                logger.debug("interrupt exception" + draftVersionB, e);
            }
        });

        futureA.get();
        futureB.get();

        final NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig, proj);
        final List<NCubePlan> cubePlans = cubePlanManager.listAllCubePlans();
        final List<NCuboidDesc> allCuboids = cubePlans.get(0).getAllCuboids();
        Assert.assertEquals(1, allCuboids.size());

        final NCuboidDesc cuboidDesc = allCuboids.get(0);
        final List<NCuboidLayout> allLayouts = cuboidDesc.getLayouts();
        Assert.assertEquals("unmatched cuboid id", NCuboidDesc.TABLE_INDEX_START_ID, cuboidDesc.getId());
        Assert.assertEquals("unmatched layouts size", 2, allLayouts.size());

        final NCuboidLayout layout1 = allLayouts.get(0);
        Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout1.getColOrder().toString());
        Assert.assertEquals("unexpected override indices", 1, layout1.getLayoutOverrideIndices().size());
        Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 2, layout1.getId());
        Assert.assertNull("not published error", layout1.getDraftVersion());

        final NCuboidLayout layout2 = allLayouts.get(1);
        Assert.assertEquals("unexpected colOrder", "[7, 3, 8]", layout2.getColOrder().toString());
        Assert.assertEquals("unexpected override indices", 0, layout2.getLayoutOverrideIndices().size());
        Assert.assertEquals("unexpected id", NCuboidDesc.TABLE_INDEX_START_ID + 3, layout2.getId());
        Assert.assertNull("not published error", layout2.getDraftVersion());

        // get favorite query realization relationships from database and validate them
        val realizations = collectFavoriteQueryRealizations(allLayouts);
        Assert.assertEquals(2, realizations.size());

        val list = Lists.newArrayList(realizations);
        list.sort(Comparator.comparingLong(FavoriteQueryRealization::getCuboidLayoutId));
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 2, list.get(0).getCuboidLayoutId());
        Assert.assertEquals(NCuboidDesc.TABLE_INDEX_START_ID + 3, list.get(1).getCuboidLayoutId());
    }

    private void course(final String[] sqls, final String draftVer, final int sleepSecondsBeforeRefresh,
            final int sleepSecondsAfterRefresh) throws InterruptedException {

        Thread.sleep(sleepSecondsBeforeRefresh * 1000);

        int retryCount = 0;
        while (retryCount < MAX_TRY_NUM) {
            KylinConfig.setKylinConfigThreadLocal(kylinConfig);
            NSmartMaster master = new NSmartMaster(kylinConfig, proj, sqls, draftVer);
            master.selectModelAndCubePlan();
            master.refreshCubePlan();
            logger.info("draft version [{}] is refreshing CubePlan", draftVer);
            try {
                Thread.sleep(sleepSecondsAfterRefresh * 1000);
            } catch (InterruptedException e) {
                logger.debug("interrupted exception", e);
            }

            try {
                master.saveModel();
                master.saveCubePlan();
                master.saveAccelerateInfo();
                logger.info("draft version [{}], save success after refresh!!!", draftVer);
                break;
            } catch (Exception e) {
                logger.info("save error after refresh, retry " + (retryCount + 1) + " times", e);
                retryCount++;
            }
        }
    }

    // ==================================================

    private void checkDraftOfEachLayout(String draftVersion, List<NCuboidDesc> cuboids) {
        Preconditions.checkNotNull(cuboids);
        for (NCuboidDesc cuboid : cuboids) {
            final List<NCuboidLayout> layouts = cuboid.getLayouts();
            Preconditions.checkNotNull(layouts);
            for (NCuboidLayout layout : layouts) {
                Assert.assertEquals(draftVersion, layout.getDraftVersion());
            }
        }
    }
}
