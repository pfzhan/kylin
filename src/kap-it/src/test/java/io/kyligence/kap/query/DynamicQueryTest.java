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

package io.kyligence.kap.query;

import io.kyligence.kap.newten.auto.NSuggestTestBase;
import io.kyligence.kap.smart.NSmartMaster;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.QueryConnection;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class DynamicQueryTest extends NSuggestTestBase {
    private static final String PROJECT = "newten";

    @Test
    public void testDynamicParamOnAgg() throws Exception {
        proposeAndBuildIndex(new String[] { "select * from test_kylin_fact" });
        getTestConfig().setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");

        String sql = "select max(LSTG_SITE_ID/?) from TEST_KYLIN_FACT";
        val stmt = getConnection().prepareStatement(sql);
        stmt.setInt(1, 2);
        val resultSet = stmt.executeQuery();
        Assert.assertTrue(resultSet.next());
        val result = resultSet.getInt(1);
        Assert.assertEquals(105, result);
    }

    @Test
    public void testDynamicParamOnLimitOffset() throws Exception {
        proposeAndBuildIndex(new String[] {
                "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt) as test_kylin_fact order by cal_dt" });

        String sql = "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt limit ? offset ?) as test_kylin_fact order by cal_dt limit ? offset ?";
        val stmt = getConnection().prepareStatement(sql);
        stmt.setInt(1, 100);
        stmt.setInt(2, 100);
        stmt.setInt(3, 1);
        stmt.setInt(4, 10);
        val resultSet = stmt.executeQuery();
        Assert.assertTrue(resultSet.next());
        val date = resultSet.getString(1);
        val count = resultSet.getInt(2);
        Assert.assertEquals("2012-04-21", date);
        Assert.assertEquals(16, count);
        Assert.assertFalse(resultSet.next());
    }

    private Connection getConnection() throws SQLException {
        return QueryConnection.getConnection(PROJECT);
    }

    private void proposeAndBuildIndex(String[] sqls) throws InterruptedException {
        val smartMaster = new NSmartMaster(KylinConfig.getInstanceFromEnv(), PROJECT, sqls);
        smartMaster.runAll();
        buildAllCubes(KylinConfig.getInstanceFromEnv(), PROJECT);
    }
}
