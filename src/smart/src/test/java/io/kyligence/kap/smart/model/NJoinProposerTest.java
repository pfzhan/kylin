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

package io.kyligence.kap.smart.model;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;

public class NJoinProposerTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testMergeReuseExistedModelAndAliasIsNotExactlyMatched() {
        // 1. create existed model
        final String sql = "select sum(ITEM_COUNT) as ITEM_CNT, count(SELLER_ACCOUNT.ACCOUNT_ID + 1), count(BUYER_ACCOUNT.ACCOUNT_ID + 1)\n"
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), getProject(), new String[] { sql });
        smartMaster.runAll();
        Assert.assertFalse(smartMaster.getContext().getAccelerateInfoMap().get(sql).isNotSucceed());
        Assert.assertEquals(1, smartMaster.getContext().getModelContexts().size());
        NDataModel originModel = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(2, originModel.getComputedColumnDescs().size());

        final String sql1 = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ORDER as ORDERS\n" + "ON TEST_KYLIN_FACT.ORDER_ID = ORDERS.ORDER_ID\n"
                + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" + "ON ORDERS.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID";
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), getProject(), new String[] { sql1 });
        smartMaster1.runAll();
        Assert.assertFalse(smartMaster1.getContext().getAccelerateInfoMap().get(sql1).isNotSucceed());
        Assert.assertEquals(1, smartMaster1.getContext().getModelContexts().size());
        NDataModel modelFromPartialJoin = smartMaster1.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertTrue(originModel.getJoinsGraph().match(modelFromPartialJoin.getJoinsGraph(), Maps.newHashMap()));
    }

    @Test
    public void testProposeModel_wontChangeOriginModelJoins_whenExistsSameTable() throws Exception {
        final String proj = "newten";
        // create new Model for this test.
        String sql = "select item_count, lstg_format_name, sum(price)\n" //
                + "from TEST_KYLIN_FACT inner join TEST_ACCOUNT as buyer_account on TEST_KYLIN_FACT.ORDER_ID = buyer_account.account_id\n"
                + "inner join TEST_ACCOUNT as seller_account on TEST_KYLIN_FACT.seller_id = seller_account.account_id\n "
                + "group by item_count, lstg_format_name\n" //
                + "order by item_count, lstg_format_name\n" //
                + "limit 10";
        NSmartMaster master = new NSmartMaster(getTestConfig(), proj, new String[] { sql });
        master.runAll();
        val newModels = NDataModelManager.getInstance(getTestConfig(), proj).listAllModels();
        Assert.assertEquals(1, newModels.size());
        Assert.assertEquals(2, newModels.get(0).getJoinTables().size());
        val originModelGragh = newModels.get(0).getJoinsGraph();

        // secondly propose, still work in SMART-Mode
        NSmartMaster master1 = new NSmartMaster(getTestConfig(), proj, new String[] { sql }, true);
        master1.runAll();
        val modelManager = NDataModelManager.getInstance(getTestConfig(), proj);
        val secondModels = modelManager.listAllModels();
        Assert.assertEquals(1, secondModels.size());
        Assert.assertEquals(2, secondModels.get(0).getJoinTables().size());
        val secondModelGragh = newModels.get(0).getJoinsGraph();
        Assert.assertTrue(originModelGragh.equals(secondModelGragh));

        // set this project to semi-auto-Mode, change the join alias. it will reuse this origin model and will not change this.
        val prjInstance = NProjectManager.getInstance(getTestConfig()).getProject(proj);
        prjInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        getTestConfig().setProperty("kap.metadata.semi-automatic-mode", "true");
        Assert.assertTrue(prjInstance.isSemiAutoMode());

        val originModels = NDataModelManager.getInstance(getTestConfig(), proj).listAllModels();
        val originModel = originModels.get(0);

        NSmartMaster semiAutoMaster = new NSmartMaster(getTestConfig(), proj, new String[] { sql }, true, true);
        semiAutoMaster.runAll();
        val semiAutoModels = modelManager.listAllModels();
        Assert.assertEquals(1, semiAutoModels.size());
        Assert.assertEquals(2, semiAutoModels.get(0).getJoinTables().size());
        Assert.assertTrue(originModel.getJoinsGraph().match(semiAutoModels.get(0).getJoinsGraph(), Maps.newHashMap())
                && semiAutoModels.get(0).getJoinsGraph().match(originModel.getJoinsGraph(), Maps.newHashMap()));

    }
}