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
import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.NTestBase;
import io.kyligence.kap.smart.model.ModelTree;

public class NSmartMasterTest extends NTestBase {

    NTableMetadataManager tableMetadataManager;
    NDataModelManager dataModelManager;
    NCubePlanManager cubePlanManager;
    NDataflowManager dataflowManager;

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

        // analysis SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(1, ctx.getModelContexts().size());
            Assert.assertEquals(expectedEffectiveOLAPCtxNum,
                    countInnerObj(ctx.getOlapContexts().values().toArray(new Collection[0])));

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

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals(2, cuboidDescs.size());
            Assert.assertEquals(3, countInnerObj(cuboidDescs.get(0).getLayouts(), cuboidDescs.get(1).getLayouts()));
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

        // analysis SQL
        {
            smartMaster.analyzeSQLs();
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertEquals(1, ctx.getModelContexts().size());
            Assert.assertEquals(expectedEffectiveOLAPCtxNum,
                    countInnerObj(ctx.getOlapContexts().values().toArray(new Collection[0])));
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            Assert.assertNotNull(mdCtx.getModelTree());
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

            List<NCuboidDesc> cuboidDescs = cubePlan.getCuboids();
            Assert.assertEquals(4, cuboidDescs.size());
            Assert.assertEquals(6, countInnerObj(cuboidDescs.get(0).getLayouts(), cuboidDescs.get(1).getLayouts(),
                    cuboidDescs.get(2).getLayouts(), cuboidDescs.get(3).getLayouts()));
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

        // analysis SQL
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
            Assert.assertEquals(4, cubePlan.getCuboids().size());
            smartMaster.shrinkCubePlan();
            NCubePlan shrinkedCubePlan = mdCtx.getTargetCubePlan();
            Assert.assertEquals(2, shrinkedCubePlan.getCuboids().size());
        }

        // shrink model
        {
            NSmartContext ctx = smartMaster.getContext();
            NSmartContext.NModelContext mdCtx = ctx.getModelContexts().get(0);
            NDataModel model = mdCtx.getTargetModel();
            Assert.assertEquals(4, model.getEffectiveColsMap().size());
            Assert.assertEquals(3, model.getEffectiveMeasureMap().size());
            smartMaster.shrinkModel();
            NDataModel shrinkedModel = mdCtx.getTargetModel();
            Assert.assertEquals(3, shrinkedModel.getEffectiveColsMap().size());
            Assert.assertEquals(2, shrinkedModel.getEffectiveMeasureMap().size());
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
        String[] sqls = new String[] { 
                "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id"
        };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analysis SQL
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
            Assert.assertEquals(1, cubePlan0.getCuboids().size());
            NCubePlan cubePlan1 = ctx.getModelContexts().get(1).getTargetCubePlan();
            Assert.assertNotNull(cubePlan1);
            Assert.assertEquals(1, cubePlan1.getCuboids().size());
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
                + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id"
        };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        {
            NSmartContext ctx = smartMaster.getContext();
            Assert.assertNotNull(ctx);
        }

        // analysis SQL
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
                Assert.assertTrue(cubePlan.getCuboids().isEmpty());
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

    @Test
    public void test() throws Exception {

        // 1st round - input SQLs, create model and cube_plan
        test1stRound();

        // 2nd round - input SQLs, update model and cube_plan
        test2ndRound();

        // 3rd round - shrink model and cube_plan
        test3rdRound();
        
        // 4th round - input complex SQLs, update model and cube_plan
        test4thRound();
        
        // 5th round - unload all queries
        test5thRound();
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
