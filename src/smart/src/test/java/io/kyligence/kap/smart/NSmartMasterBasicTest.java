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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.NTestBase;
import io.kyligence.kap.smart.model.ModelTree;

public class NSmartMasterBasicTest extends NTestBase {

    private NTableMetadataManager tableMetadataManager;
    private NDataModelManager dataModelManager;
    private NCubePlanManager cubePlanManager;
    private NDataflowManager dataflowManager;

    @Before
    public void setupManagers() throws Exception {
        setUp();
        tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        dataModelManager = NDataModelManager.getInstance(kylinConfig, proj);
        cubePlanManager = NCubePlanManager.getInstance(kylinConfig, proj);
        dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);
    }

    @Ignore
    @Test
    public void test() throws Exception {

        // 1st round - input SQLs, create model and cube_plan
        test1stRound();

        // 2nd round - input SQLs, update model and cube_plan
        test2ndRound();

        // 3rd round - shrink model and cube_plan
        test3rdRound();

        /* FIXME KAP-7518
        // 4th round - input complex SQLs, update model and cube_plan
        test4thRound();
        
        // 5th round - unload all queries
        test5thRound();
        */
    }

    private void test1stRound() throws IOException {
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt" //
        };
        final int expectedEffectiveOLAPCtxNum = 4;

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analyze SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(1, ctx.getModelContexts().size());

            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getModelTree());
            ModelTree modelTree = mdCtx.getModelTree();
            Assert.assertEquals(expectedEffectiveOLAPCtxNum, modelTree.getOlapContexts().size());
            Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());
        }

        // select model
        {
            smartMaster.selectModel();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNull(mdCtx.getTargetModel());
            Assert.assertNull(mdCtx.getOrigModel());
        }

        // opt model
        {
            smartMaster.optimizeModel();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NDataModel model = mdCtx.getTargetModel();
            Assert.assertNotNull(model);
            Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
            Assert.assertFalse(model.getEffectiveColsMap().isEmpty());
            Assert.assertFalse(model.getEffectiveMeasureMap().isEmpty());
        }

        // select cube_plan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNull(mdCtx.getTargetCubePlan());
            Assert.assertNull(mdCtx.getOrigCubePlan());
        }

        // opt cube_plan
        {
            smartMaster.optimizeCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getAllCuboids();
            Assert.assertEquals(2, cuboidDescs.size());
            Assert.assertEquals(3, collectAllLayouts(cuboidDescs).size());
        }

        // save
        {
            Assert.assertEquals(0, dataModelManager.listModels().size());
            Assert.assertEquals(0, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(0, dataflowManager.listAllDataflows().size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();

            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        }
    }

    private void test2ndRound() throws IOException {
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-03' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name", //
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'", //
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" //
        };

        final int expectedEffectiveOLAPCtxNum = 4;

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analyze SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(1, ctx.getModelContexts().size());

            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getModelTree());
            ModelTree modelTree = mdCtx.getModelTree();
            Assert.assertEquals(expectedEffectiveOLAPCtxNum, modelTree.getOlapContexts().size());
            Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());
        }

        // select model
        {
            smartMaster.selectModel();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getTargetModel());
            Assert.assertNotNull(mdCtx.getOrigModel());
        }

        // opt model
        {
            smartMaster.optimizeModel();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NDataModel model = mdCtx.getTargetModel();
            Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
            Assert.assertEquals(model.getName(), mdCtx.getOrigModel().getName());
            Assert.assertFalse(model.getEffectiveColsMap().isEmpty());
            Assert.assertFalse(model.getEffectiveMeasureMap().isEmpty());
        }

        // select cube_plan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getTargetCubePlan());
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
        }

        // opt cube_plan
        {
            smartMaster.optimizeCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertNotNull(cubePlan);
            Assert.assertEquals(cubePlan.getName(), mdCtx.getOrigCubePlan().getName());
            Assert.assertEquals(mdCtx.getTargetModel().getName(), cubePlan.getModelName());

            List<NCuboidDesc> cuboidDescs = cubePlan.getAllCuboids();
            Assert.assertEquals(4, cuboidDescs.size());
            Assert.assertEquals(6, collectAllLayouts(cuboidDescs));
        }

        // save
        {
            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();

            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        }
    }

    private void test3rdRound() throws IOException {
        String[] sqls = new String[] { "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt", //
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" //
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analyze SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(1, ctx.getModelContexts().size());
        }

        // select model
        {
            smartMaster.selectModel();
            NSmartContext ctx = smartMaster.getContext();

            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getTargetModel());
            Assert.assertNotNull(mdCtx.getOrigModel());
        }

        // select cube_plan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getTargetCubePlan());
            Assert.assertNotNull(mdCtx.getOrigCubePlan());
        }

        // reduce cube_plan
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NCubePlan cubePlan = mdCtx.getTargetCubePlan();
            Assert.assertEquals(4, cubePlan.getAllCuboids().size());
            smartMaster.shrinkCubePlan();
            NCubePlan shrunkCubePlan = mdCtx.getTargetCubePlan();
            Assert.assertEquals(2, shrunkCubePlan.getAllCuboids().size());
        }

        // shrink model
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NDataModel model = mdCtx.getTargetModel();
            Assert.assertEquals(4, model.getEffectiveColsMap().size());
            Assert.assertEquals(3, model.getEffectiveMeasureMap().size());
            smartMaster.shrinkModel();
            NDataModel shrunkModel = mdCtx.getTargetModel();
            Assert.assertEquals(3, shrunkModel.getEffectiveColsMap().size());
            Assert.assertEquals(2, shrunkModel.getEffectiveMeasureMap().size());
        }

        // save
        {
            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());

            smartMaster.saveCubePlan();
            smartMaster.saveModel();

            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        }
    }

    private void test4thRound() throws IOException {
        String[] sqls = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analyze SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(2, ctx.getModelContexts().size());
        }

        // select Model
        {
            smartMaster.selectModel();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNull(modelContext.getOrigModel());
            }
        }

        // opt Model
        {
            smartMaster.optimizeModel();
            NSmartContext ctx = smartMaster.getContext();
            NDataModel model0 = ctx.getModelContexts().get(0).getTargetModel();
            Assert.assertNotNull(model0);
            NDataModel model1 = ctx.getModelContexts().get(1).getTargetModel();
            Assert.assertNotNull(model1);
            Assert.assertEquals(5, model0.getEffectiveColsMap().size() + model1.getEffectiveColsMap().size());
            Assert.assertEquals(1, model0.getEffectiveMeasureMap().size());
            Assert.assertEquals(1, model1.getEffectiveMeasureMap().size());
        }

        // select CubePlan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNull(modelContext.getOrigCubePlan());
            }
        }

        // opt CubePlan
        {
            smartMaster.optimizeCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            NCubePlan cubePlan0 = ctx.getModelContexts().get(0).getTargetCubePlan();
            Assert.assertNotNull(cubePlan0);
            Assert.assertEquals(1, cubePlan0.getAllCuboids().size());
            NCubePlan cubePlan1 = ctx.getModelContexts().get(1).getTargetCubePlan();
            Assert.assertNotNull(cubePlan1);
            Assert.assertEquals(1, cubePlan1.getAllCuboids().size());
        }

        // save
        {
            Assert.assertEquals(1, dataModelManager.listModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();

            Assert.assertEquals(3, dataModelManager.listModels().size());
            Assert.assertEquals(3, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
        }
    }

    private void test5thRound() throws IOException {
        String[] sqls = new String[] {
                // 1st round
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt", //
                // 2nd round
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where part_dt = '2012-01-03' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name", //
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'", //
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name", //
                // 3rd round
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt", //
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name", //
                // 4th round
                "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT "
                        + " from kylin_sales as test_kylin_fact where test_kylin_fact.lstg_format_name <= 'ABZ' "
                        + " group by test_kylin_fact.lstg_format_name having count(seller_id) > 2", //
                "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                        + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                        + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                        + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analyze SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(3, ctx.getModelContexts().size());
        }

        // select model
        {
            smartMaster.selectModel();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNotNull(modelContext.getOrigModel());
            }
        }

        // select cube_plan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNotNull(modelContext.getOrigCubePlan());
            }
        }

        // reduce cube_plan
        {
            smartMaster.shrinkCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                NCubePlan cubePlan = modelContext.getTargetCubePlan();
                Assert.assertNotNull(cubePlan);
                Assert.assertTrue(cubePlan.getAllCuboids().isEmpty());
            }
        }

        // shrink model
        {
            smartMaster.shrinkModel();
            NSmartContext ctx = smartMaster.getContext();
            for (NSmartContext.NModelContext modelContext : ctx.getModelContexts()) {
                NDataModel model = modelContext.getTargetModel();
                Assert.assertNotNull(model);
                Assert.assertTrue(model.getEffectiveColsMap().isEmpty());
                Assert.assertEquals(1, model.getEffectiveMeasureMap().size());
            }
        }

        // save
        {
            smartMaster.saveCubePlan();
            smartMaster.saveModel();
        }
    }
}
