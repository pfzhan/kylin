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
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NTestBase;
import lombok.val;

public class NSmartMasterTest extends NTestBase {

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
        {
            String[] sqlStatements = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums"
                    + " FROM (SELECT f.leaf_categ_id FROM kylin_sales f inner join KYLIN_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id WHERE f.lstg_format_name = 'ABIN') t1"
                    + " INNER JOIN (SELECT leaf_categ_id FROM kylin_sales f INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id WHERE buyer_id > 100) t2"
                    + " ON t1.leaf_categ_id = t2.leaf_categ_id GROUP BY t1.leaf_categ_id ORDER BY nums, leaf_categ_id" };
            model3 = proposeModel(sqlStatements);
        }
        String model3Alias = model3.getAlias();
        Assert.assertEquals("AUTO_MODEL_KYLIN_SALES_2", model3Alias);
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
            Assert.assertFalse(accelerateInfoEntry.getValue().isBlocked());
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
    public void testCountDistinctTwoParamColumn() throws IOException {
        /*
         * case 1:
         */
        String[] sqls = new String[] { "SELECT part_dt, SUM(price) AS GMV, COUNT(1) AS TRANS_CNT,\n"
                + "COUNT(DISTINCT lstg_format_name), COUNT(DISTINCT seller_id, lstg_format_name) AS DIST_SELLER_FORMAT\n"
                + "FROM kylin_sales GROUP BY part_dt" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        {
            final NSmartContext ctx = smartMaster.getContext();
            final List<NDataModel.Measure> allMeasures = ctx.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(4, allMeasures.size());

            final NDataModel.Measure measure3 = allMeasures.get(2);
            Assert.assertEquals("COUNT_DISTINCT_LSTG_FORMAT_NAME", measure3.getName());
            Assert.assertEquals(1, measure3.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure3.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure4 = allMeasures.get(3);
            Assert.assertEquals("COUNT_DISTINCT_SELLER_ID", measure4.getName());
            Assert.assertEquals(2, measure4.getFunction().getParameterCount());
            Assert.assertEquals("hllc", measure4.getFunction().getReturnDataType().getName());
        }

        /*
         * case 2:
         */
        sqls = new String[] { "SELECT COUNT(DISTINCT META_CATEG_NAME) AS CNT, MAX(META_CATEG_NAME) AS max_name\n"
                + "FROM kylin_category_groupings" };
        smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.runAll();
        {
            final NSmartContext ctx = smartMaster.getContext();
            final List<NDataModel.Measure> allMeasures = ctx.getModelContexts().get(0).getTargetModel()
                    .getAllMeasures();
            Assert.assertEquals(3, allMeasures.size());

            final NDataModel.Measure measure1 = allMeasures.get(1);
            Assert.assertEquals("COUNT_DISTINCT_META_CATEG_NAME", measure1.getName());
            Assert.assertEquals(1, measure1.getFunction().getParameterCount());
            Assert.assertEquals("bitmap", measure1.getFunction().getReturnDataType().getName());

            final NDataModel.Measure measure2 = allMeasures.get(2);
            Assert.assertEquals("MAX_META_CATEG_NAME", measure2.getName());
            Assert.assertEquals(1, measure2.getFunction().getParameterCount());
            Assert.assertEquals("varchar", measure2.getFunction().getReturnDataType().getName());
        }
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

            String prefix = "In the model designer project, the system is not allowed to modify the semantic layer"
                    + " (dimensions, measures, tables, and joins) of the model. ";
            String postFix = "to enable system accelerate this query.";
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

            String prefix = "In the model designer project, the system is not allowed to modify the semantic layer"
                    + " (dimensions, measures, tables, and joins) of the model. Please add measure";
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

            String preFix = "In the model designer project, the system is not allowed to modify the semantic layer "
                    + "(dimensions, measures, tables, and joins) of the model.";
            String postFix = "has some difference with the joins of this query. Please adjust model's join to match the query.";
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
    public void testInitTargetModelError() throws IOException {
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);

        smartMaster.analyzeSQLs();

        // after cutting context, change "DEFAULT.KYLIN_CAL_DT" to be a incremental load table
        val tableManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        val table = tableManager.getTableDesc("DEFAULT.KYLIN_CAL_DT");
        table.setFact(true);
        tableManager.updateTableDesc(table);

        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.saveModel();
        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();
        smartMaster.saveCubePlan();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        final AccelerateInfo accelerateInfo = Lists.newArrayList(accelerateInfoMap.values()).get(0);
        Assert.assertTrue(accelerateInfo.isBlocked());
        Assert.assertEquals("Only one incrementing loading table can be setted in model!",
                accelerateInfo.getBlockingCause().getMessage());
    }

    @Test
    public void testInitTargetCubePlanError() throws IOException {
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);

        smartMaster.runAll();

        // just mock a case to cover exception may happen in NDimensionProposer
        final NSmartContext.NModelContext modelContext = smartMaster.getContext().getModelContexts().get(0);
        modelContext.getTargetCubePlan().setCubePlanOverrideEncodings(null);

        smartMaster.optimizeCubePlan();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        final AccelerateInfo accelerateInfo = Lists.newArrayList(accelerateInfoMap.values()).get(0);
        Assert.assertTrue(accelerateInfo.isBlocked());
        Assert.assertTrue(accelerateInfo.getBlockingCause() instanceof NullPointerException);
    }

    @Test
    public void testWithoutSaveModel() {
        String[] sqls = new String[] { "select part_dt, lstg_format_name, sum(price) from kylin_sales \n"
                + " left join kylin_cal_dt on cal_dt = part_dt \n"
                + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, proj, sqls);
        smartMaster.analyzeSQLs();
        smartMaster.selectModel();
        smartMaster.optimizeModel();
        smartMaster.selectCubePlan();
        smartMaster.optimizeCubePlan();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.values().size());
        Assert.assertFalse(Lists.newArrayList(accelerateInfoMap.values()).get(0).isBlocked());
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
        String[] namedColumns = model.getAllNamedColumns().stream().map(NDataModel.NamedColumn::getAliasDotColumn)
                .sorted().toArray(String[]::new);

        String namedColumn1 = namedColumns[2];
        String namedColumn2 = namedColumns[7];
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
}
