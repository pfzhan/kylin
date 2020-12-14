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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.newten.auto.NAutoTestBase;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;
import lombok.val;

public class DynamicQueryTest extends NAutoTestBase {
    private static final String PROJECT = "newten";

    @Test
    public void testDynamicParamOnAgg() throws Exception {
        proposeAndBuildIndex(new String[] { "select * from test_kylin_fact" });
        getTestConfig().setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");

        String sql = "select max(LSTG_SITE_ID/?) from TEST_KYLIN_FACT";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        queryExec.setPrepareParam(0, 2);
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertTrue(resultSet.getRows().size() > 0);
        Assert.assertEquals("105.5", resultSet.getRows().get(0).get(0));
    }

    @Test
    public void testDynamicParamOnLimitOffset() throws Exception {
        proposeAndBuildIndex(new String[] {
                "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt) as test_kylin_fact order by cal_dt" });

        String sql = "select * from (select cal_dt, count(*) from test_kylin_fact group by cal_dt order by cal_dt limit ? offset ?) as test_kylin_fact order by cal_dt limit ? offset ?";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        queryExec.setPrepareParam(0, 100);
        queryExec.setPrepareParam(1, 100);
        queryExec.setPrepareParam(2, 1);
        queryExec.setPrepareParam(3, 10);
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertEquals(resultSet.getRows().size(), 1);
        val date = resultSet.getRows().get(0).get(0);
        val count = resultSet.getRows().get(0).get(1);
        Assert.assertEquals("2012-04-21", date);
        Assert.assertEquals("16", count);
    }

    private void proposeAndBuildIndex(String[] sqls) throws InterruptedException {
        val smartContext = AccelerationContextUtil.newSmartContext(KylinConfig.getInstanceFromEnv(), PROJECT, sqls);
        val smartMaster = new SmartMaster(smartContext);
        smartMaster.runUtWithContext(null);
        smartContext.saveMetadata();
        AccelerationContextUtil.onlineModel(smartContext);
        buildAllCubes(KylinConfig.getInstanceFromEnv(), PROJECT);
    }

    @Test
    public void testDynamicParamOnMilliSecTimestamp() throws Exception {
        proposeAndBuildIndex(new String[] { "select time2 from test_measure" });
        String sql = "select time2 from test_measure where time2=?";
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        String ts = "2012-03-21 10:10:10.789";
        Date parsedDate = dateFormat.parse(ts);
        queryExec.setPrepareParam(0, new java.sql.Timestamp(parsedDate.getTime()));
        val resultSet = queryExec.executeQuery(sql);
        Assert.assertEquals(resultSet.getRows().size(), 1);
        val timestamp = resultSet.getRows().get(0).get(0);
        Assert.assertEquals("2012-03-21 10:10:10.789", timestamp);
    }
}
