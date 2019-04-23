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

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;
import io.kyligence.kap.smart.model.ModelTree;

public class NSmartMasterBasicTest extends NAutoTestOnLearnKylinData {

    private NTableMetadataManager tableMetadataManager;
    private NIndexPlanManager indexPlanManager;
    private NDataflowManager dataflowManager;

    @Before
    public void setupManagers() throws Exception {
        setUp();
        KylinConfig kylinConfig = getTestConfig();
        tableMetadataManager = NTableMetadataManager.getInstance(kylinConfig, proj);
        indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, proj);
        dataflowManager = NDataflowManager.getInstance(kylinConfig, proj);
    }

    @Test
    public void test() {

        // 1st round - input SQLs, create model and cube_plan
        test1stRound();

        // 2nd round - input SQLs, update model and cube_plan
        test2ndRound();

        // 3rd round - input complex SQLs, update model and cube_plan
        test3rdRound();

        // 4th round - unload all queries
        test4thRound();
    }

    private void test1stRound() {
        final int expectedEffectiveOLAPCtxNum = 4;
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name",
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt" //
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);

        // validation after initializing NSmartMaster
        NSmartContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.analyzeSQLs();
        Assert.assertEquals(1, smartContext.getModelContexts().size());
        NSmartContext.NModelContext mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getModelTree());
        ModelTree modelTree = mdCtx.getModelTree();
        Assert.assertEquals(expectedEffectiveOLAPCtxNum, modelTree.getOlapContexts().size());
        Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());

        // select model
        smartMaster.selectModel();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNull(mdCtx.getTargetModel());
        Assert.assertNull(mdCtx.getOrigModel());

        // optimize model
        smartMaster.optimizeModel();
        mdCtx = smartContext.getModelContexts().get(0);
        NDataModel model = mdCtx.getTargetModel();
        Assert.assertNotNull(model);
        Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
        Assert.assertFalse(model.getEffectiveColsMap().isEmpty());
        Assert.assertFalse(model.getEffectiveMeasureMap().isEmpty());

        // select IndexPlan
        smartMaster.selectIndexPlan();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNull(mdCtx.getOrigIndexPlan());
        Assert.assertNull(mdCtx.getTargetIndexPlan());

        // optimize IndexPlan
        smartMaster.optimizeIndexPlan();
        mdCtx = smartContext.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        Assert.assertEquals(mdCtx.getTargetModel().getUuid(), indexPlan.getUuid());
        List<IndexEntity> indexEntities = indexPlan.getAllIndexes();
        Assert.assertEquals(2, indexEntities.size());
        Assert.assertEquals(3, collectAllLayouts(indexEntities).size());

        // validation before save
        Assert.assertEquals(0, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(0, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(0, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(6, accelerateInfoMap.size());
        Assert.assertEquals(4, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
    }

    private void test2ndRound() {
        final int expectedEffectiveOLAPCtxNum = 4;
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("DEFAULT.KYLIN_SALES");

        String[] sqls = new String[] { //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name",
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name",
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'",
                "select part_dt, lstg_format_name, sum(price * item_count + 2) "
                        + "from kylin_sales where part_dt > '2012-01-01' "
                        + "union select part_dt, lstg_format_name, price "
                        + "from kylin_sales where part_dt < '2012-01-01'",
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name" //
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);

        // validation after initializing NSmartMaster
        NSmartContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.analyzeSQLs();
        Assert.assertEquals(1, smartContext.getModelContexts().size());
        NSmartContext.NModelContext mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getModelTree());
        ModelTree modelTree = mdCtx.getModelTree();
        Assert.assertEquals(expectedEffectiveOLAPCtxNum, modelTree.getOlapContexts().size());
        Assert.assertEquals(kylinSalesTblDesc, modelTree.getRootFactTable());

        // select model
        smartMaster.selectModel();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getTargetModel());
        Assert.assertNotNull(mdCtx.getOrigModel());

        // optimize model
        smartMaster.optimizeModel();
        mdCtx = smartContext.getModelContexts().get(0);
        NDataModel model = mdCtx.getTargetModel();
        Assert.assertEquals(kylinSalesTblDesc, model.getRootFactTable().getTableDesc());
        Assert.assertEquals(model.getUuid(), mdCtx.getOrigModel().getUuid());
        Assert.assertFalse(model.getEffectiveColsMap().isEmpty());
        Assert.assertFalse(model.getEffectiveMeasureMap().isEmpty());

        // select IndexPlan
        smartMaster.selectIndexPlan();
        mdCtx = smartContext.getModelContexts().get(0);
        Assert.assertNotNull(mdCtx.getTargetIndexPlan());
        Assert.assertNotNull(mdCtx.getOrigIndexPlan());

        // optimize IndexPlan
        smartMaster.optimizeIndexPlan();
        mdCtx = smartContext.getModelContexts().get(0);
        IndexPlan indexPlan = mdCtx.getTargetIndexPlan();
        Assert.assertNotNull(indexPlan);
        Assert.assertEquals(indexPlan.getUuid(), mdCtx.getOrigIndexPlan().getUuid());
        Assert.assertEquals(mdCtx.getTargetModel().getUuid(), indexPlan.getUuid());
        List<IndexEntity> indexEntities = indexPlan.getAllIndexes();
        Assert.assertEquals(4, indexEntities.size());
        Assert.assertEquals(5, collectAllLayouts(indexEntities).size());

        // validation before save
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(5, accelerateInfoMap.size());
        Assert.assertEquals(4, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
    }

    private void test3rdRound() {

        String[] sqls = new String[] { "SELECT t1.leaf_categ_id, COUNT(*) AS nums " //
                + "FROM ( " //
                + "  SELECT f.leaf_categ_id FROM kylin_sales f\n" //
                + "    INNER JOIN KYLIN_CATEGORY_GROUPINGS o\n" //
                + "    ON f.leaf_categ_id = o.leaf_categ_id AND f.LSTG_SITE_ID = o.site_id\n" //
                + "  WHERE f.lstg_format_name = 'ABIN'\n" //
                + ") t1 INNER JOIN (\n" //
                + "    SELECT leaf_categ_id FROM kylin_sales f\n" //
                + "      INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id\n" //
                + "    WHERE buyer_id > 100\n" //
                + ") t2 ON t1.leaf_categ_id = t2.leaf_categ_id\n" //
                + "GROUP BY t1.leaf_categ_id\n" //
                + "ORDER BY nums, leaf_categ_id" };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);

        // validation after initializing NSmartMaster
        NSmartContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.analyzeSQLs();
        Assert.assertEquals(2, smartContext.getModelContexts().size());

        // select Model
        smartMaster.selectModel();
        for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNull(modelContext.getOrigModel());
            Assert.assertNull(modelContext.getTargetModel());
        }

        // optimize Model
        smartMaster.optimizeModel();
        NDataModel model0 = smartContext.getModelContexts().get(0).getTargetModel();
        Assert.assertNotNull(model0);
        Assert.assertEquals(48, model0.getEffectiveColsMap().size());
        Assert.assertEquals(2, model0.getEffectiveDimenionsMap().size());
        Assert.assertEquals(1, model0.getEffectiveMeasureMap().size());
        NDataModel model1 = smartContext.getModelContexts().get(1).getTargetModel();
        Assert.assertNotNull(model1);
        Assert.assertEquals(17, model1.getEffectiveColsMap().size());
        Assert.assertEquals(0, model1.getEffectiveDimenionsMap().size());
        Assert.assertEquals(1, model1.getEffectiveMeasureMap().size());

        // select IndexPlan
        smartMaster.selectIndexPlan();
        for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNull(modelContext.getOrigIndexPlan());
            Assert.assertNull(modelContext.getTargetIndexPlan());
        }

        // optimize IndexPlan
        smartMaster.optimizeIndexPlan();
        IndexPlan indexPlan0 = smartContext.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertNotNull(indexPlan0);
        Assert.assertEquals(1, indexPlan0.getAllIndexes().size());
        IndexPlan indexPlan1 = smartContext.getModelContexts().get(1).getTargetIndexPlan();
        Assert.assertNotNull(indexPlan1);
        Assert.assertEquals(1, indexPlan1.getAllIndexes().size());

        // validation before save
        Assert.assertEquals(1, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(1, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(1, accelerateInfoMap.size());
        Assert.assertEquals(1, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
    }

    private void test4thRound() {
        String[] sqls = new String[] {
                // 1st round
                "select 1", // not effective olap_context
                "create table a", // not effective olap_context
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-01' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-02' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name > 'ABIN' group by part_dt, lstg_format_name", //
                "select part_dt, sum(item_count), count(*) from kylin_sales group by part_dt", //

                // 2nd round
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where part_dt = '2012-01-03' group by part_dt, lstg_format_name", //
                "select part_dt, lstg_format_name, sum(price) from kylin_sales "
                        + "where lstg_format_name = 'ABIN' group by part_dt, lstg_format_name", //
                "select sum(price) from kylin_sales where part_dt = '2012-01-03'", //
                "select part_dt, lstg_format_name, sum(price * item_count + 2) "
                        + "from kylin_sales where part_dt > '2012-01-01' "
                        + "union select part_dt, lstg_format_name, price "
                        + "from kylin_sales where part_dt < '2012-01-01'",
                "select lstg_format_name, sum(item_count), count(*) from kylin_sales group by lstg_format_name", //

                // 3rd round
                "select test_kylin_fact.lstg_format_name, sum(price) as GMV, count(seller_id) as TRANS_CNT "
                        + " from kylin_sales as test_kylin_fact where test_kylin_fact.lstg_format_name <= 'ABZ' "
                        + " group by test_kylin_fact.lstg_format_name having count(seller_id) > 2", //
                "SELECT t1.leaf_categ_id, COUNT(*) AS nums " //
                        + "FROM ( " //
                        + "  SELECT f.leaf_categ_id FROM kylin_sales f\n" //
                        + "    INNER JOIN KYLIN_CATEGORY_GROUPINGS o\n" //
                        + "    ON f.leaf_categ_id = o.leaf_categ_id AND f.LSTG_SITE_ID = o.site_id\n" //
                        + "  WHERE f.lstg_format_name = 'ABIN'\n" //
                        + ") t1 INNER JOIN (\n" //
                        + "    SELECT leaf_categ_id FROM kylin_sales f\n" //
                        + "      INNER JOIN KYLIN_ACCOUNT o ON f.buyer_id = o.account_id\n" //
                        + "    WHERE buyer_id > 100\n" //
                        + ") t2 ON t1.leaf_categ_id = t2.leaf_categ_id\n" //
                        + "GROUP BY t1.leaf_categ_id\n" //
                        + "ORDER BY nums, leaf_categ_id" //
        };

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), proj, sqls);

        // validation after initializing NSmartMaster
        NSmartContext smartContext = smartMaster.getContext();
        Assert.assertNotNull(smartContext);

        // analyze SQL
        smartMaster.analyzeSQLs();
        Assert.assertEquals(3, smartContext.getModelContexts().size());
        Assert.assertEquals(11, collectAllOlapContexts(smartContext).size());

        // select model
        smartMaster.selectModel();
        for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNotNull(modelContext.getOrigModel());
            Assert.assertNotNull(modelContext.getTargetModel());
        }

        // optimize model
        smartMaster.optimizeModel();

        // select IndexPlan
        smartMaster.selectIndexPlan();
        for (NSmartContext.NModelContext modelContext : smartContext.getModelContexts()) {
            Assert.assertNotNull(modelContext.getOrigIndexPlan());
        }

        // optimize IndexPlan
        smartMaster.optimizeIndexPlan();
        List<IndexEntity> allProposedIndexes = Lists.newArrayList();
        smartContext.getModelContexts().forEach(modelContext -> {
            allProposedIndexes.addAll(modelContext.getTargetIndexPlan().getAllIndexes());
        });
        Assert.assertEquals(7, allProposedIndexes.size());
        Assert.assertEquals(8, collectAllLayouts(allProposedIndexes).size());

        // validation before save
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertEquals(13, accelerateInfoMap.size());
        Assert.assertEquals(10, accelerateInfoMap.values().stream()
                .filter(accelerateInfo -> !accelerateInfo.getRelatedLayouts().isEmpty()).count());

        // validation after save
        smartMaster.saveModel();
        smartMaster.saveIndexPlan();
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels().size());
        Assert.assertEquals(3, indexPlanManager.listAllIndexPlans().size());
        Assert.assertEquals(3, dataflowManager.listAllDataflows().size());
    }
}
