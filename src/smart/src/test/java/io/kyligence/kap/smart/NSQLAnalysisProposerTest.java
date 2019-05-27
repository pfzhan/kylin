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

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.common.NAutoTestOnLearnKylinData;

public class NSQLAnalysisProposerTest extends NAutoTestOnLearnKylinData {

    @Test
    public void testSqlsNotBlockedByCircledJoinSQL() {

        String[] sqls = new String[] { //

                // circled-join will not be accelerated
                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "INNER JOIN TEST_ORDER as TEST_ORDER\n"
                        + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n"
                        + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                        + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                        + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                        + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID AND SELLER_ACCOUNT.ACCOUNT_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"",

                // normal query will be accelerated
                "SELECT \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\" AS \"LSTG_FORMAT_NAME\",\n"
                        + "  SUM(\"TEST_KYLIN_FACT\".\"PRICE\") AS \"sum_price\"\n"
                        + "FROM \"DEFAULT\".\"TEST_KYLIN_FACT\" \"TEST_KYLIN_FACT\"\n"
                        + "GROUP BY \"TEST_KYLIN_FACT\".\"LSTG_FORMAT_NAME\"" };
        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), "newten", sqls);
        smartMaster.runAll();

        final NSmartContext smartContext = smartMaster.getContext();
        final List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        final Map<String, AccelerateInfo> accelerateInfoMap = smartContext.getAccelerateInfoMap();
        Assert.assertTrue(!accelerateInfoMap.get(sqls[1]).isFailed() && !accelerateInfoMap.get(sqls[1]).isPending());
        Assert.assertTrue(accelerateInfoMap.get(sqls[0]).isFailed());
        Assert.assertTrue(accelerateInfoMap.get(sqls[0]).getFailedCause() instanceof IllegalArgumentException);
    }
}