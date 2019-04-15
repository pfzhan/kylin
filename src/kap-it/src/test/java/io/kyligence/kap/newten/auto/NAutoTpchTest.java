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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;

public class NAutoTpchTest extends NAutoTestBase {

    //KAP#7892 fix this
    @Test
    public void testTpch() throws Exception {
        // split batch to verify KAP#9114
        new TestScenario(CompareLevel.SAME, "sql_tpch", 0, 5).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 5, 10).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 10, 15).execute();
        new TestScenario(CompareLevel.SAME, "sql_tpch", 15, 22).execute();
    }

    @Test
    public void testReProposeCase() throws Exception {
        // run twice to verify KAP#7515
        for (int i = 0; i < 2; ++i) {
            new TestScenario(CompareLevel.SAME, "sql_tpch", 1, 2).execute();
        }
    }

    @Test
    public void testBatchProposeSQLAndReuseModel() throws Exception {
        NSmartMaster smartMaster = proposeWithSmartMaster(
                new TestScenario[] { new TestScenario(CompareLevel.SAME, "sql_tpch") }, getProject());
        smartMaster.runAll();

        String sql = "SELECT \"SN\".\"N_NAME\", SUM(l_extendedprice) \"REVENUE\"\n" + "FROM TPCH.\"LINEITEM\"\n"
                + "INNER JOIN TPCH.\"ORDERS\" ON \"L_ORDERKEY\" = \"O_ORDERKEY\"\n"
                + "INNER JOIN TPCH.\"CUSTOMER\" ON \"O_CUSTKEY\" = \"C_CUSTKEY\"\n"
                + "INNER JOIN TPCH.\"NATION\" \"CN\" ON \"C_NATIONKEY\" = \"CN\".\"N_NATIONKEY\"\n"
                + "INNER JOIN TPCH.\"SUPPLIER\" ON \"L_SUPPKEY\" = \"S_SUPPKEY\"\n"
                + "INNER JOIN TPCH.\"NATION\" \"SN\" ON \"S_NATIONKEY\" = \"SN\".\"N_NATIONKEY\"\n"
                + "INNER JOIN TPCH.\"REGION\" ON \"SN\".\"N_REGIONKEY\" = \"R_REGIONKEY\"\n"
                + "WHERE \"R_NAME\" = 'A' AND \"CN\".\"N_NAME\" = \"SN\".\"N_NAME\" AND \"O_ORDERDATE\" >= '2010-01-01' AND \"O_ORDERDATE\" < '2010-01-02'\n"
                + "GROUP BY \"SN\".\"N_NAME\"\n" + "ORDER BY \"REVENUE\" DESC";
        NSmartMaster smartMaster1 = new NSmartMaster(getTestConfig(), getProject(), new String[] { sql });
        smartMaster1.runAll();

        AccelerateInfo accelerateInfo = smartMaster1.getContext().getAccelerateInfoMap().values()
                .toArray(new AccelerateInfo[] {})[0];
        Assert.assertFalse(accelerateInfo.isBlocked());
    }

    @Test
    @Ignore
    public void testTemp() throws Exception {
        new TestScenario(CompareLevel.SAME, "temp").execute();

    }
}
