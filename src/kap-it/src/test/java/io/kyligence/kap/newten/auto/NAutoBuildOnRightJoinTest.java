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

import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.data.QueryResult;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.utils.AccelerationContextUtil;

public class NAutoBuildOnRightJoinTest extends NAutoTestBase {

    @Test
    public void testRightJoin() throws Exception {
        final int TEST_SQL_CNT = 3;
        for (int i = 0; i < TEST_SQL_CNT; i++) {
            List<Pair<String, String>> queries = fetchQueries("query/sql_join/sql_right_join", i, i + 1);
            SmartMaster master = proposeWithSmartMaster(queries);

            List<AbstractContext.ModelContext> modelContexts = master.getContext().getModelContexts();
            // ensure only one model is created for right join
            Assert.assertEquals(1, modelContexts.size());
            AbstractContext.ModelContext modelContext = modelContexts.get(0);
            NDataModel dataModel = modelContext.getTargetModel();
            Assert.assertNotNull(dataModel);
            Assert.assertFalse(dataModel.getJoinTables().isEmpty());
            Assert.assertEquals("LEFT", dataModel.getJoinTables().get(0).getJoin().getType());
            IndexPlan indexPlan = modelContext.getTargetIndexPlan();
            Assert.assertNotNull(indexPlan);
        }
    }

    /**
     * https://olapio.atlassian.net/browse/KE-19473
     * @throws Exception
     */
    @Test
    public void testRightJoinWhenTwoModelHaveSameMeasure() throws Exception {
        // prepare two model have different fact table and same measure
        String query1 = "select sum(test_kylin_fact.price) from TEST_ORDER  "
                + "left JOIN  test_kylin_fact ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID  "
                + "left join test_measure on test_kylin_fact.ITEM_COUNT = test_measure.ID3 "
                + "left join test_account on TEST_KYLIN_FACT.seller_id = test_account.account_id "
                + "where test_kylin_fact.LSTG_FORMAT_NAME = 'FP-GTC' group by test_kylin_fact.CAL_DT";
        String query2 = "select sum(test_kylin_fact.price) from test_kylin_fact";
        AbstractContext context = AccelerationContextUtil.newSmartContext(getTestConfig(), getProject(),
                new String[] { query1, query2 });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        buildAllCubes(kylinConfig, getProject());

        // query
        QueryResult queryResult = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv())
                .executeQuery("select sum(test_kylin_fact.price) from test_kylin_fact "
                        + "right JOIN TEST_ORDER  ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID "
                        + "left  join test_measure on test_kylin_fact.ITEM_COUNT = test_measure.ID3 "
                        + "left join test_account on TEST_KYLIN_FACT.seller_id = test_account.account_id "
                        + "where test_kylin_fact.LSTG_FORMAT_NAME = 'FP-GTC' group by test_kylin_fact.CAL_DT");
        Assert.assertEquals(862.69, Double.parseDouble(queryResult.getRows().get(0).get(0)), 0.01);
    }

    private SmartMaster proposeWithSmartMaster(List<Pair<String, String>> queries) {
        String[] sqls = queries.stream().map(Pair::getSecond).toArray(String[]::new);
        AbstractContext context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(), sqls);
        SmartMaster master = new SmartMaster(context);
        master.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        return master;
    }
}
