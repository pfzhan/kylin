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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NTestBase;
import io.kyligence.kap.smart.model.ModelTree;
import lombok.val;

public class NSmartMasterTest extends NTestBase {
    private static final Logger logger = LoggerFactory.getLogger(NSmartMasterTest.class);

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
            for (NModelContext modelContext : ctx.getModelContexts()) {
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
            for (NModelContext modelContext : ctx.getModelContexts()) {
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
            for (NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNotNull(modelContext.getOrigModel());
            }
        }

        // select cube_plan
        {
            smartMaster.selectCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            for (NModelContext modelContext : ctx.getModelContexts()) {
                Assert.assertNotNull(modelContext.getOrigCubePlan());
            }
        }

        // reduce cube_plan
        {
            smartMaster.shrinkCubePlan();
            NSmartContext ctx = smartMaster.getContext();
            for (NModelContext modelContext : ctx.getModelContexts()) {
                NCubePlan cubePlan = modelContext.getTargetCubePlan();
                Assert.assertNotNull(cubePlan);
                Assert.assertTrue(cubePlan.getAllCuboids().isEmpty());
            }
        }

        // shrink model
        {
            smartMaster.shrinkModel();
            NSmartContext ctx = smartMaster.getContext();
            for (NModelContext modelContext : ctx.getModelContexts()) {
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

    // Test shrink model/cube_plan process may contaminate the original model/cube_plan or not
    @Test
    public void testShrinkIsolation() throws Exception {
        String[] sqlStatements = new String[] {
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" };
        NDataModel originalModel1, originalModel2, targetModel1, targetModel2, targetModel3;
        NCubePlan originalCubePlan1, originalCubePlan2, targetCubePlan1, targetCubePlan2, targetCubePlan3;
        // select, optimize and save the model & cube_plan
        {
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqlStatements);
            smartMaster.analyzeSQLs();
            smartMaster.selectModel();
            smartMaster.optimizeModel();

            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            targetModel1 = modelContext.getTargetModel();

            smartMaster.selectCubePlan();
            smartMaster.optimizeCubePlan();
            targetCubePlan1 = modelContext.getTargetCubePlan();

            smartMaster.saveModel();
            smartMaster.saveCubePlan();
        }
        // select, shrink the model & cube_plan without save
        {
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqlStatements);
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();

            smartMaster.selectModel();
            NSmartContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            originalModel1 = modelContext.getOrigModel();
            // Make sure the saveModel() is taken effect
            Assert.assertEquals(originalModel1, targetModel1);

            smartMaster.selectCubePlan();
            originalCubePlan1 = modelContext.getOrigCubePlan();
            Assert.assertEquals(originalCubePlan1, targetCubePlan1);

            smartMaster.shrinkCubePlan();
            targetCubePlan2 = modelContext.getTargetCubePlan();

            smartMaster.shrinkModel();
            targetModel2 = modelContext.getTargetModel();
        }
        // select, shrink and save the model & cube_plan
        {
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqlStatements);

            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();

            smartMaster.selectModel();
            NSmartContext.NModelContext modelContext = ctx.getModelContexts().get(0);
            originalModel2 = modelContext.getOrigModel();
            // Make sure shrinkModel() does not soil the originalModel
            Assert.assertEquals(originalModel1, originalModel2);

            smartMaster.selectCubePlan();
            originalCubePlan2 = modelContext.getOrigCubePlan();
            Assert.assertEquals(originalCubePlan1, originalCubePlan2);

            smartMaster.shrinkCubePlan();
            targetCubePlan3 = modelContext.getTargetCubePlan();
            Assert.assertEquals(targetCubePlan2, targetCubePlan3);

            smartMaster.shrinkModel();
            targetModel3 = modelContext.getTargetModel();
            Assert.assertEquals(targetModel2, targetModel3);

            smartMaster.saveModel();
            smartMaster.saveCubePlan();
        }
    }

    @Test
    public void testRenameModel() throws Exception {
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
        /* FIXME KAP-7518
        {
            String[] sqlStatements = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                    + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                    + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                    + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
            model3 = proposeModel(sqlStatements);
        }
        String model3Alias = model3.getAlias();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_2", model3Alias);
        */
    }

    @Test
    public void testSaveAccelerateInfo() throws IOException {
        String[] sqls = new String[] {
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.analyzeSQLs();
        Assert.assertEquals(5, smartMaster.getContext().getAccelerateInfoMap().size());
        for (Map.Entry<String, AccelerateInfo> accelerateInfoEntry : smartMaster.getContext().getAccelerateInfoMap()
                .entrySet()) {
            Assert.assertEquals(false, accelerateInfoEntry.getValue().isBlocked());
        }
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.saveModel();

        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();
        smartMaster.saveCubePlan();

        final NSmartContext ctx = smartMaster.getContext();
        final Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(5, accelerateInfoMap.size());

        // before saveAccelerateInfo
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        NCubePlan cubePlan = mdCtx.getTargetCubePlan();
        final List<NCuboidDesc> allCuboids = cubePlan.getAllCuboids();
        final List<NCuboidLayout> layouts = collectAllLayouts(allCuboids);
        Set<FavoriteQueryRealization> fqRealizationsBefore = collectFavoriteQueryRealizations(layouts);
        Assert.assertTrue(fqRealizationsBefore.isEmpty());

        // do save accelerateInfo
        smartMaster.saveAccelerateInfo();

        // after saveAccelerateInfo
        Set<FavoriteQueryRealization> fqRealizationsAfter = collectFavoriteQueryRealizations(layouts);
        Assert.assertEquals(5, fqRealizationsAfter.size());
    }

    @Test
    public void testSaveAccelerateInfoOfOneSqlToManyLayouts() throws IOException {
        String[] sqls = new String[] { "select a.*, kylin_sales.lstg_format_name as lstg_format_name \n"
                + "from ( select part_dt, sum(price) as sum_price from kylin_sales\n"
                + "         where part_dt > '2010-01-01' group by part_dt) a \n"
                + "join kylin_sales on a.part_dt = kylin_sales.part_dt \n"
                + "group by lstg_format_name, a.part_dt, a.sum_price" };
        String draftVersion = UUID.randomUUID().toString();
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls, draftVersion);
        smartMaster.runAll();

        final NSmartContext ctx = smartMaster.getContext();
        final Map<String, AccelerateInfo> accelerateInfoMap = ctx.getAccelerateInfoMap();
        Assert.assertEquals(1, ctx.getModelContexts().size());
        Assert.assertEquals(1, accelerateInfoMap.size());

        // get favorite query realization relationships from database and validate them
        NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
        NCubePlan cubePlan = mdCtx.getTargetCubePlan();
        final List<NCuboidDesc> allCuboids = cubePlan.getAllCuboids();
        final List<NCuboidLayout> layouts = collectAllLayouts(allCuboids);
        val fqRealizationsAfter = collectFavoriteQueryRealizations(layouts);
        Assert.assertEquals(2, fqRealizationsAfter.size());
    }

    /**
     * Refer to: <ref>NCuboidRefresherTest.testParallelTimeLineRetryWithMultiThread()</ref>
     */
    @Test
    public void testRefreshCubePlanWithRetry() throws IOException, ExecutionException, InterruptedException {
        String[] sqlsA = new String[] {
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt",
                "select part_dt, lstg_format_name, price from kylin_sales where part_dt = '2012-01-01'" };
        String draftVersionA = UUID.randomUUID().toString();
        NSmartMaster smartMasterA = new NSmartMaster(kylinConfig, proj, sqlsA, draftVersionA);
        smartMasterA.runAll();

        String draftVersionB = UUID.randomUUID().toString();
        String[] sqlsB = new String[] {

                "select lstg_format_name, part_dt, price from kylin_sales where part_dt = '2012-01-01'",
                "select lstg_format_name, part_dt, price, item_count from kylin_sales where part_dt = '2012-01-01'" };
        NSmartMaster smartMasterB = new NSmartMaster(kylinConfig, proj, sqlsB, draftVersionB);
        smartMasterB.runAll();

        String draftVersionC = UUID.randomUUID().toString();
        String[] sqlsC = new String[] {

                "select lstg_format_name, price from kylin_sales where price = 100" };
        NSmartMaster smartMasterC = new NSmartMaster(kylinConfig, proj, sqlsC, draftVersionC);
        smartMasterC.runAll();

        ExecutorService service = Executors.newCachedThreadPool();
        Future futureA = service.submit(() -> {
            try {
                refreshCubePlanWithRetry(smartMasterA);
            } catch (IOException e) {
                Assert.fail();
            }
        });

        Future futureB = service.submit(() -> {
            try {
                refreshCubePlanWithRetry(smartMasterB);
            } catch (IOException e) {
                Assert.fail();
            }
        });

        Future futureC = service.submit(() -> {
            try {
                refreshCubePlanWithRetry(smartMasterC);
            } catch (IOException e) {
                Assert.fail();
            }
        });

        futureA.get();
        futureB.get();
        futureC.get();

        final NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig, proj);
        final List<NCubePlan> cubePlans = cubePlanManager.listAllCubePlans();
        Assert.assertEquals(1, cubePlans.size());

        final List<NCuboidDesc> allCuboids = cubePlans.get(0).getAllCuboids();
        Assert.assertEquals(5, allCuboids.size());
        Assert.assertEquals(6, collectAllLayouts(allCuboids).size());
    }

    // TODO if fixed  #7844 delete this method
    private void refreshCubePlanWithRetry(NSmartMaster smartMaster) throws IOException {
        try {
            KylinConfig externalConfig = smartMaster.getContext().getKylinConfig();
            KylinConfig.setKylinConfigThreadLocal(externalConfig);
            smartMaster.refreshCubePlanWithRetry();
        } catch (InterruptedException e) {
            logger.error("thread interrupted exception", e);
            Assert.fail();
        }
    }

    @Test
    public void testRefreshCubePlanWithRetryFail() {
        kylinConfig.setProperty("kap.smart.conf.propose.retry-max", "0");
        try {

            testRefreshCubePlanWithRetry();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ExecutionException);
            Assert.assertTrue(e.getCause() instanceof IllegalStateException);
        }
    }

    @Test
    public void testMaintainModelTypeWithNoInitialModel() throws IOException {
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
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();

        {
            final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            smartMaster.renameModel();
            Assert.assertNull(targetModel);
        }

        {
            final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            smartMaster.saveModel();
            Assert.assertNull(targetModel);
        }

        smartMaster.selectCubePlan();

        {
            smartMaster.optimizeCubePlan();
            final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNull(targetModel);
            final NCubePlan targetCubePlan = smartMaster.getContext().getModelContexts().get(0).getTargetCubePlan();
            Assert.assertNull(targetCubePlan);
        }

        {
            smartMaster.saveCubePlan();
            final NDataModel targetModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNull(targetModel);
            final NCubePlan targetCubePlan = smartMaster.getContext().getModelContexts().get(0).getTargetCubePlan();
            Assert.assertNull(targetCubePlan);
        }
    }

    @Test
    public void testMaintainModelType() throws IOException {
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

            final Throwable blockingCause0 = accelerateInfoMapCase0.get(sqls[0]).getBlockingCause();
            Assert.assertNull(blockingCause0);

            final List<NCuboidDesc> allCuboids = smartMaster.getContext().getModelContexts().get(0).getTargetCubePlan()
                    .getAllCuboids();
            final List<NCuboidLayout> layouts = collectAllLayouts(allCuboids);
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

            String prefix = "In the current manually designed project, the system cannot modify "
                    + "the model semantic (correlation of dimensions, measures and tables). ";
            String postFix = "so that the system could accelerate this query.";
            final Throwable blockingCause1 = accelerateInfoMapCase1.get(sqls[0]).getBlockingCause();
            Assert.assertTrue(blockingCause1.getMessage().startsWith(prefix) //
                    && blockingCause1.getMessage().endsWith(postFix));

            final List<NCuboidDesc> allCuboids = smartMaster.getContext().getModelContexts().get(0).getTargetCubePlan()
                    .getAllCuboids();
            final List<NCuboidLayout> layouts = collectAllLayouts(allCuboids);
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

            String prefix = "In the current manually designed project, the system "
                    + "cannot modify the model semantic (correlation of dimensions, measures and tables). "
                    + "Please add measure";
            final Throwable blockingCause2 = accelerateInfoMapCase2.get(sqls[0]).getBlockingCause();
            Assert.assertNotNull(blockingCause2);
            Assert.assertTrue(blockingCause2.getMessage().startsWith(prefix));

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

            String preFix = "In the current manually designed project, the system "
                    + "cannot modify the model semantic (correlation of dimensions, measures and tables). "
                    + "Query and model mismatch. Please make sure the model";
            String postFix = "contains all tables [DEFAULT.KYLIN_CAL_DT, DEFAULT.KYLIN_SALES] in your input query.";
            final Throwable blockingCause3 = accelerateInfoMapCase3.get(sqls[0]).getBlockingCause();
            Assert.assertNotNull(blockingCause3);
            Assert.assertTrue(blockingCause3.getMessage().startsWith(preFix) //
                    && blockingCause3.getMessage().endsWith(postFix));

            final NDataModel targetModelCase3 = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            final List<NDataModel.Measure> allMeasuresCase3 = targetModelCase3.getAllMeasures();
            final List<NDataModel.NamedColumn> allNamedColumnsCase3 = targetModelCase3.getAllNamedColumns();
            final List<JoinTableDesc> joinTablesCase3 = targetModel.getJoinTables();
            Assert.assertEquals("measures changed unexpected", allMeasures, allMeasuresCase3);
            Assert.assertEquals("named columns changed unexpected", allNamedColumns, allNamedColumnsCase3);
            Assert.assertEquals("join tables changed unexpected", joinTables, joinTablesCase3);
        }
    }

    @Test
    public void testRenameAllColumns() {
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
        String[] namedColumns = new String[9];
        for (int i = 0; i < 9; ++i) {
            String namedColumn = model.getAllNamedColumns().get(i).getAliasDotColumn();
            namedColumns[i] = namedColumn;
        }
        Arrays.sort(namedColumns);
        String namedColumn1 = namedColumns[0];
        String namedColumn2 = namedColumns[1];
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_COUNTRY", namedColumn1);
        Assert.assertEquals("TEST_ACCOUNT_1.ACCOUNT_COUNTRY", namedColumn2);
    }

    private NDataModel proposeModel(String[] sqlStatements) throws Exception {
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqlStatements);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        NSmartContext ctx = smartMaster.getContext();
        NSmartContext.NModelContext modelContext = ctx.getModelContexts().get(0);
        smartMaster.renameModel();
        smartMaster.saveModel();
        return modelContext.getTargetModel();
    }

    /*
    public void testNewtenDemoScript() throws Exception {
        metaDir = "src/test/resources/nsmart/newten/meta";
        proj = "newten";
        setupManagers();
    
        String[] sqls = new String[] { 
    
    "select lstg_format_name, sum(price) as GMV "
    + " from test_kylin_fact "
    + " where test_kylin_fact.seller_id in ( 10000002, 10000003, 10000004,10000005,10000006,10000008,10000009,10000001,10000010,10000011)"
    + " group by lstg_format_name"
    ,
    "select sum(PRICE) as GMV, LSTG_FORMAT_NAME as FORMAT_NAME"
    + " from test_kylin_fact"
    + " where (LSTG_FORMAT_NAME in ('ABIN')) or (LSTG_FORMAT_NAME>='FP-GTC' and LSTG_FORMAT_NAME<='Others')"
    + " group by LSTG_FORMAT_NAME"
    ,
    "select test_kylin_fact.cal_dt, count(*) as mmm"
    + " from test_kylin_fact inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where lstg_format_name = 'Others' "
    + " group by test_kylin_fact.cal_dt order by test_kylin_fact.cal_dt"
    ,
    "select test_kylin_fact.cal_dt,test_kylin_fact.seller_id, sum(test_kylin_fact.price) as GMV, count(*) as TRANS_CNT "
    + " from test_kylin_fact"
    + " where DATE '2012-09-01' <= test_kylin_fact.cal_dt and test_kylin_fact.seller_id = 10000002"
    + " group by test_kylin_fact.cal_dt, test_kylin_fact.seller_id"
    ,
    "select sum(price) as GMV, count(*) as TRANS_CNT "
    + " FROM test_kylin_fact"
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id"
    + " AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id "
    + " where test_kylin_fact.cal_dt < DATE '2012-05-01' or test_kylin_fact.cal_dt > DATE '2013-05-01'"
    ,
    "select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV, count(*) as TRANS_CNT "
    + " from test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where  DATE '2013-03-24'  <= test_cal_dt.week_beg_dt"
    + " group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt"
    ,
    "select count(*) as x"
    + " FROM test_kylin_fact"
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where( META_CATEG_NAME IN ('jenny','esrzongguan','Baby') "
    + "   AND ( META_CATEG_NAME IN ('non_existing_dict_value1', 'Baby', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3')"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR 'administrators' IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1' ))"
    + "   and ( META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR META_CATEG_NAME IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2',"
    + "     'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value2', 'non_existing_dict_value3', 'non_existing_dict_value3',"
    + "     'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3', 'non_existing_dict_value3' )"
    + "   OR 'administrators' IN ('non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1',"
    + "     'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1', 'non_existing_dict_value1' )) )"
    ,
    "select meta_categ_name, count(1) as cnt, sum(price) as GMV "
    + " from test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where meta_categ_name not in ('', 'a')"
    + " group by meta_categ_name"
    ,
    "select test_cal_dt.week_beg_dt, sum(test_kylin_fact.price) as GMV , count(*) as TRANS_CNT "
    + " from test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where (test_kylin_fact.lstg_format_name='FP-GTC') and extract(MONTH from test_cal_dt.week_beg_dt) = 12"
    + " group by test_cal_dt.week_beg_dt"
    ,
    "select max(cal_dt) as cnt from test_kylin_fact"
    ,
    "select test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV, count(1) as TRANS_CNT"
    + " from test_kylin_fact "
    + " left JOIN edw.test_cal_dt as test_cal_dt "
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt "
    + " left JOIN test_category_groupings "
    + " on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id and test_kylin_fact.lstg_site_id = test_category_groupings.site_id "
    + " left JOIN edw.test_sites as test_sites "
    + " on test_kylin_fact.lstg_site_id = test_sites.site_id "
    + " left JOIN edw.test_seller_type_dim as test_seller_type_dim "
    + " on test_kylin_fact.slr_segment_cd = test_seller_type_dim.seller_type_cd "
    + " where test_kylin_fact.lstg_format_name='FP-GTC' and test_cal_dt.week_beg_dt between DATE '2013-05-01'"
    + " and DATE '2013-08-01' and test_cal_dt.cal_dt between DATE '2013-06-01' and DATE '2013-09-01' "
    + " group by test_cal_dt.week_beg_dt"
    ,
    "select meta_categ_name, count(1) as cnt, sum(price) as GMV "
    + " from test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where meta_categ_name is not null"
    + " group by meta_categ_name"
    ,
    "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT "
    + " from test_kylin_fact where test_kylin_fact.lstg_format_name <= 'ABZ' "
    + " group by test_kylin_fact.lstg_format_name having count(seller_id) > 2"
    ,
    "select fact.lstg_format_name"
    + " from (select * from test_kylin_fact where cal_dt > date'2010-01-01' ) as fact"
    + " group by fact.lstg_format_name"
    + " order by CASE WHEN fact.lstg_format_name IS NULL THEN 'sdf' ELSE fact.lstg_format_name END"
    ,
    "select test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt,sum(test_kylin_fact.price) as GMV, count(*) as TRANS_CNT ,sum(test_kylin_fact.item_count) as total_items"
    + " from test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where  1 < 3"
    + " group by test_kylin_fact.lstg_format_name, test_cal_dt.week_beg_dt"
    ,
    "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT "
    + " from test_kylin_fact where test_kylin_fact.lstg_format_name > 'AB' "
    + " group by test_kylin_fact.lstg_format_name having count(seller_id) > 2"
    ,
    "select concat(meta_categ_name, lstg_format_name) as c1, concat(meta_categ_name, 'CONST') as c2,"
    + " concat(meta_categ_name, concat(test_sites.site_name, lstg_format_name)) as c3, count(1) as cnt, sum(price) as GMV "
    + " from test_kylin_fact"
    + " left JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " left JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " left JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " where not ( meta_categ_name not in ('', 'a','Computers') or meta_categ_name not in ('Crafts','Computers'))"
    + " group by concat(meta_categ_name, lstg_format_name), concat(meta_categ_name, 'CONST'),"
    + " concat(meta_categ_name, concat(test_sites.site_name, lstg_format_name))"
    ,
    "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(*) as TRANS_CNT"
    + " from test_kylin_fact "
    + " where test_kylin_fact.lstg_format_name = 'GGGG'"
    + " group by test_kylin_fact.lstg_format_name"
    ,
    "SELECT test_category_groupings.meta_categ_name, sum(test_kylin_fact.price) as GMV, count(*) as trans_cnt "
    + " FROM test_kylin_fact "
    + " inner JOIN edw.test_cal_dt as test_cal_dt"
    + " ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt"
    + " inner JOIN test_category_groupings"
    + " ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id"
    + " inner JOIN edw.test_sites as test_sites"
    + " ON test_kylin_fact.lstg_site_id = test_sites.site_id"
    + " group by test_category_groupings.meta_categ_name"
        };
    
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            smartMaster.analyzeSQLs();
            smartMaster.selectModel();
            smartMaster.optimizeModel();
            smartMaster.selectCubePlan();
            smartMaster.optimizeCubePlan();
            smartMaster.saveModel();
            smartMaster.saveCubePlan();
        }
        
        smartMaster = new NSmartMaster(kylinConfig, proj, new String[]{
                "SELECT test_category_groupings.meta_categ_name ,sum(test_kylin_fact.price) as GMV ,count(*) as trans_cnt FROM test_kylin_fact inner JOIN edw.test_cal_dt as test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt inner JOIN test_category_groupings ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id inner JOIN edw.test_sites as test_sites ON test_kylin_fact.lstg_site_id = test_sites.site_id group by test_category_groupings.meta_categ_name "
        });
        {
            smartMaster.analyzeSQLs();
            smartMaster.selectModel();
            smartMaster.selectCubePlan();
            smartMaster.shrinkCubePlan();
            smartMaster.shrinkModel();
            smartMaster.saveCubePlan();
            smartMaster.saveModel();
        }
        
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            smartMaster.analyzeSQLs();
            smartMaster.selectModel();
            smartMaster.selectCubePlan();
            smartMaster.shrinkCubePlan();
            smartMaster.shrinkModel();
            smartMaster.saveCubePlan();
            smartMaster.saveModel();
        }
    }
    */
}
