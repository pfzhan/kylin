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

package io.kyligence.kap.newten.auto;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelSelectProposer;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.util.AccelerationContextUtil;
import lombok.val;
import lombok.var;

public class ReuseModelCcTest extends AutoTestBase {

    @Override
    public String getProject() {
        return "newten";
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        prepareModels();
        kylinConfig.setProperty("kylin.query.transformers",
                "org.apache.kylin.query.util.PowerBIConverter,org.apache.kylin.query.util.DefaultQueryTransformer,io.kyligence.kap.query.util.EscapeTransformer,org.apache.kylin.query.util.KeywordDefaultDirtyHack,io.kyligence.kap.query.security.RowFilter,io.kyligence.kap.query.security.HackSelectStarWithColumnACL");
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, left join
     * To be accelerated sql statements: 
     *   sql1. select count(item_count + price) from test_kylin_fact group by cal_dt;
     *   sql2. select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt;
     *   sql3. select * from test_kylin_fact;
     *   sql4. select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     *   sql5. select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     *
     * expectation: 
     *   first round: sql1 and sql2 will success, sql3, sql4 and sql5 will pending.
     *   second round: accelerate all pending sql statements, sql3 and sql4 will success, sql5 will pending
     *   third round: accelerate sql5, sql5 still pending for semi-auto mode cannot create new model
     */
    @Test
    public void testReuseModelOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        String[] statements = { "select count(item_count + price) from test_kylin_fact group by cal_dt", // sql1
                "select count(item_count + price), count(item_count * price) from test_kylin_fact group by cal_dt", // sql2
                "select * from test_kylin_fact", // sql3
                "select * from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt", // sql4
                "select * from test_kylin_fact inner join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt" // sql5
        };

        // first round
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(), statements);
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(statements[0]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(statements[1]).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(statements[2]).isPending());
        Assert.assertEquals(ModelSelectProposer.CC_ACROSS_MODELS_PENDING_MSG,
                accelerateInfoMap.get(statements[2]).getPendingMsg());
        Assert.assertTrue(accelerateInfoMap.get(statements[3]).isPending());
        Assert.assertEquals(ModelSelectProposer.CC_ACROSS_MODELS_PENDING_MSG,
                accelerateInfoMap.get(statements[3]).getPendingMsg());
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());

        // mock apply recommendations of ccs, dimensions and measures
        List<AbstractContext.ModelContext> modelContexts = context.getModelContexts();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        modelContexts.forEach(modelContext -> {
            NDataModel targetModel = modelContext.getTargetModel();
            if (targetModel != null) {
                dataModelManager.updateDataModel(targetModel.getUuid(), copyForWrite -> {
                    copyForWrite.setComputedColumnDescs(targetModel.getComputedColumnDescs());
                    copyForWrite.setAllMeasures(targetModel.getAllMeasures());
                    copyForWrite.setAllNamedColumns(targetModel.getAllNamedColumns());
                });
            }
        });

        // second round
        val contextSecond = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(),
                new String[] { statements[2], statements[3], statements[4] });
        smartMaster = new SmartMaster(contextSecond);
        smartMaster.runUtWithContext(null);
        contextSecond.saveMetadata();
        AccelerationContextUtil.onlineModel(contextSecond);

        accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(statements[2]).isNotSucceed());
        Assert.assertFalse(accelerateInfoMap.get(statements[3]).isNotSucceed());
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());

        // third round
        val contextThird = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(),
                new String[] { statements[4] });
        smartMaster = new SmartMaster(contextThird);
        smartMaster.runUtWithContext(null);
        contextThird.saveMetadata();
        AccelerationContextUtil.onlineModel(contextThird);

        accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(statements[4]).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG,
                accelerateInfoMap.get(statements[4]).getPendingMsg());
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, left join
     * To be accelerated sql statements:
     *   sql. select count(item_count + price), count(item_count * price) from test_kylin_fact join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt;
     *
     * expectation: pending
     */
    @Test
    public void testCannotReuseModelOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        String sql = "select count(item_count + price), count(item_count * price) "
                + "from test_kylin_fact join edw.test_cal_dt on test_kylin_fact.cal_dt = test_cal_dt.cal_dt";
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(),
                new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isPending());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    /**
     * model1: AUTO_MODEL_TEST_KYLIN_FACT_1 contains a computed column { item_count + price }, left join
     * model2: AUTO_MODEL_TEST_KYLIN_FACT_2 contains a computed column { item_count * price }, inner join
     * To be accelerated sql statements: select * from test_kylin_fact;
     *
     * expectation: pending
     */
    @Test
    public void testSelectStarReuseLeftJoinOfSemiAutoMode() {
        transferProjectToSemiAutoMode();
        NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, getProject());
        List<String> modelId = Lists.newArrayList();
        modelManager.listAllModels().forEach(dataModel -> {
            if (dataModel.getAlias().equals("AUTO_MODEL_TEST_KYLIN_FACT_2")
                    && dataModel.getRootFactTable().getAlias().equals("TEST_KYLIN_FACT")) {
                modelId.add(dataModel.getUuid());
            }
        });
        Assert.assertEquals(1, modelId.size());
        modelManager.updateDataModel(modelId.get(0), copyForWrite -> {
            copyForWrite.getJoinTables().get(0).getJoin().setType("inner");
        });

        String sql = "select * from test_kylin_fact";
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(),
                new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    /**
     * model1: Model3 contains a computed column {lineitem.l_extendedprice * lineitem.l_discount}, inner join
     * model2: Model4 contains a computed column {lineitem.l_quantity * lineitem.l_extendedprice}, no join, share same fact table
     * To be accelerated sql: select * from tpch.lineitem
     *
     * expectation: success
     */
    @Test
    public void testSelectStarCheckInnerJoinOfSemiAutoMode() {
        prepareMoreModels();
        transferProjectToSemiAutoMode();
        String sql = "select * from TPCH.LINEITEM";
        val context = AccelerationContextUtil.newModelReuseContextOfSemiAutoMode(kylinConfig, getProject(),
                new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(sql).isNotSucceed());
        Assert.assertEquals(ModelSelectProposer.NO_MODEL_MATCH_PENDING_MSG, accelerateInfoMap.get(sql).getPendingMsg());
    }

    private void transferProjectToSemiAutoMode() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(getProject(), copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.query.metadata.expose-computed-column", "true");
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    private void prepareModels() {
        String[] sqls = {
                "select count(item_count + price) from test_kylin_fact left join edw.test_cal_dt "
                        + "on test_kylin_fact.cal_dt = test_cal_dt.cal_dt",
                "select sum(item_count * price) from test_kylin_fact left join test_category_groupings "
                        + "on test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id "
                        + "and test_kylin_fact.lstg_site_id = test_category_groupings.site_id" //
        };
        val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), new String[] { sqls[0] });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        val context2 = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), new String[] { sqls[1] });
        smartMaster = new SmartMaster(context2);
        smartMaster.runUtWithContext(null);
        context2.saveMetadata();
        AccelerationContextUtil.onlineModel(context2);
    }

    private void prepareMoreModels() {
        String[] sqls = { //
                "select count(l_extendedprice * l_discount) from tpch.lineitem "
                        + "inner join tpch.part on lineitem.l_partkey = part.p_partkey",
                "select sum(l_quantity * l_extendedprice) from tpch.lineitem" //
        };
        val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), new String[] { sqls[0] });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);

        val context2 = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), new String[] { sqls[1] });
        smartMaster = new SmartMaster(context2);
        smartMaster.runUtWithContext(null);
        context2.saveMetadata();
        AccelerationContextUtil.onlineModel(context2);
    }
}
