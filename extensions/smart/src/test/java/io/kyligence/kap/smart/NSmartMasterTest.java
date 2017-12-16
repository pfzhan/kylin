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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.model.ModelTree;
import io.kyligence.kap.smart.query.Utils;

public class NSmartMasterTest {
    private static final String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
    private static final String proj = "learn_kylin";

    private static File tmpMeta;
    private static KylinConfig kylinConfig;

    @Before
    public void setUp() throws Exception {
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);

        kylinConfig = Utils.newKylinConfig(tmpMeta.getAbsolutePath());
        kylinConfig.setProperty("kylin.metadata.data-model-impl", "io.kyligence.kap.metadata.model.NDataModel");
        kylinConfig.setProperty("kylin.metadata.data-model-manager-impl",
                "io.kyligence.kap.metadata.model.NDataModelManager");
        kylinConfig.setProperty("kylin.metadata.realization-providers", "io.kyligence.kap.cube.model.NDataflowManager");

        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
    }

    @After
    public void tearDown() throws Exception {
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
    }

    private void test1stRound() throws IOException {
        TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(kylinConfig);
        NDataModelManager dataModelManager = (NDataModelManager) DataModelManager.getInstance(kylinConfig);
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig);
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("kylin_sales", proj);

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
            Assert.assertEquals(0, dataModelManager.listDataModels().size());
            Assert.assertEquals(0, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(0, dataflowManager.listAllDataflows().size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();

            Assert.assertEquals(1, dataModelManager.listDataModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        }
    }

    private void test2ndRound() throws IOException {
        TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(kylinConfig);
        NDataModelManager dataModelManager = (NDataModelManager) DataModelManager.getInstance(kylinConfig);
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig);
        TableDesc kylinSalesTblDesc = tableMetadataManager.getTableDesc("kylin_sales", proj);

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
            Assert.assertEquals(1, dataModelManager.listDataModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());

            smartMaster.saveModel();
            smartMaster.saveCubePlan();

            Assert.assertEquals(1, dataModelManager.listDataModels().size());
            Assert.assertEquals(1, cubePlanManager.listAllCubePlans().size());
            Assert.assertEquals(1, dataflowManager.listAllDataflows().size());
        }
    }

    @Test
    public void test() throws Exception {
        TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(kylinConfig);
        NDataModelManager dataModelManager = (NDataModelManager) DataModelManager.getInstance(kylinConfig);
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(kylinConfig);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig);

        // 1st round - create model and cube_plan
        test1stRound();

        // 2nd round - update model and cube_plan
        test2ndRound();
    }

    private <T> int countInnerObj(Collection<T>... list) {
        int i = 0;
        for (Collection<T> l : list) {
            i += l.size();
        }
        return i;
    }
}
