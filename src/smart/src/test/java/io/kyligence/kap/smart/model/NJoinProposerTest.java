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
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.NSmartMaster;

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
}