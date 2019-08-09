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

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.util.ConvertToComputedColumn;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.util.ComputedColumnEvalUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NAutoComputedColumnTest extends NAutoTestBase {

    @Test
    public void testComputedColumnSingle() {
        String query = "SELECT SUM(PRICE * ITEM_COUNT + 1), AVG(PRICE * ITEM_COUNT + 1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 1",
                computedColumnDesc.getExpression());

        String convertedQuery = convertCC(query);
        String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_1), AVG(TEST_KYLIN_FACT._CC_CC_AUTO_1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        Assert.assertEquals(expectedQuery, convertedQuery);
    }

    @Test
    public void testComputedColumnMultiple() {
        String query = "SELECT SUM(PRICE * ITEM_COUNT + 1), AVG(PRICE * ITEM_COUNT * 0.9), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(2, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT * 0.9",
                computedColumnDesc.getExpression());
        computedColumnDesc = model.getComputedColumnDescs().get(1);
        Assert.assertEquals("CC_AUTO_2", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 1",
                computedColumnDesc.getExpression());

        String convertedQuery = convertCC(query);
        String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_2), AVG(TEST_KYLIN_FACT._CC_CC_AUTO_1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        Assert.assertEquals(expectedQuery, convertedQuery);
    }

    /*
     * test points: 1. support propose more than one cc
     *              2. tolerance of failed sql
     *              3. unsupported sql in current calcite version but can propose
     */
    @Test
    public void testProposeMultiCCToOneModel() {
        // The 'price*item_count' should be replaced by auto_cc_1
        // The 'price+item_count' will produce another cc expression auto_cc_2.
        // (query5: left() supported by CALCITE-3005, left() supported by spark2.3+).
        String query1 = "select price*item_count from test_kylin_fact";
        String query2 = "select sum(price*item_count) from test_kylin_fact"; // one cc
        String query3 = "select sum(price*item_count), price from test_kylin_fact group by price";
        String query4 = "select sum(price+item_count) from test_kylin_fact"; // another cc
        String query5 = "select {fn left(lstg_format_name,-4)} as name, sum(price*item_count) "
                + "from test_kylin_fact group by lstg_format_name"; // left(...) will replaced by substring(...)
        String query6 = "select  {fn CHAR(lstg_format_name)}, sum(price*item_count) "
                + "from test_kylin_fact group by lstg_format_name";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(),
                new String[] { query1, query2, query3, query4, query5, query6 });
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val computedColumns = modelContexts.get(0).getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, computedColumns.size());
        val suggestedCC1 = computedColumns.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC1.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC1.getExpression());
        val suggestedCC2 = computedColumns.get(1);
        Assert.assertEquals("CC_AUTO_2", suggestedCC2.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC2.getExpression());

        val accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(query1).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query2).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query3).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query4).isFailed());
        Assert.assertFalse(accelerateInfoMap.get(query4).isFailed());
        Assert.assertTrue(accelerateInfoMap.get(query6).isFailed());
        Assert.assertTrue(accelerateInfoMap.get(query6).getFailedCause().getMessage()
                .contains("parse failed: Encountered \"{fn CHAR\""));

        val targetIndexPlan = modelContexts.get(0).getTargetIndexPlan();
        final List<IndexEntity> indexes = targetIndexPlan.getIndexes();
        indexes.sort(Comparator.comparing(IndexEntity::getId));
        Assert.assertEquals(0L, indexes.get(0).getId());
        Assert.assertEquals(10000L, indexes.get(1).getId());
        Assert.assertEquals(20000L, indexes.get(2).getId());
        Assert.assertEquals(30000L, indexes.get(3).getId());
        Assert.assertEquals(20000000000L, indexes.get(4).getId());
    }

    @Test
    public void testProposeCCToDifferentModelWithSameRootFactTable() {
        // different model share the same cc for having the same root fact table
        String query1 = "select sum(price * item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        String query2 = "select sum(price * item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "left join test_country on test_account.account_country = test_country.country";
        String query3 = "select sum(price + item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1, query2, query3 });
        smartMaster.runAll();
        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(2, modelContexts.size());

        // case 1: different cc expression
        val computedColumns = modelContexts.get(0).getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, computedColumns.size());
        val suggestedCC1 = computedColumns.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC1.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC1.getExpression());
        val suggestedCC2 = computedColumns.get(1);
        Assert.assertEquals("CC_AUTO_2", suggestedCC2.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC2.getExpression());

        // case 2: same cc expression
        val suggestedCC3 = modelContexts.get(1).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_2", suggestedCC3.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC3.getExpression());
    }

    @Test
    public void testProposedMultiCCToDifferentModelWithDifferentRootFactTable() {
        String query1 = "select sum(price*item_count) from test_kylin_fact";
        String query2 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";
        String query3 = "select sum(price + item_count) from test_order inner join test_kylin_fact "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1, query2, query3 });
        smartMaster.runAll();

        // suggestedCC1, suggestedCC2 and suggestedCC3  will be added to different root fact table
        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(3, modelContexts.size());
        val suggestedCC1 = modelContexts.get(0).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC1.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC1.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC1.getExpression());
        val suggestedCC2 = modelContexts.get(1).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC2.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", suggestedCC2.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC2.getExpression());
        val suggestedCC3 = modelContexts.get(2).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC3.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_ORDER", suggestedCC3.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC3.getExpression());
    }

    @Test
    public void testReproposeUseExistingModel() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1 });
        smartMaster.runAll();
        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val modelContext = modelContexts.get(0);
        val computedColumns = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedColumns.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC.getExpression());

        // case 1: cannot use existing cc for different cc expression
        String query2 = "select sum(price+item_count) from test_kylin_fact"; // another cc
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query2 });
        smartMaster.runAll();
        val modelContextsList1 = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContextsList1.size());
        val modelContext1 = modelContextsList1.get(0);
        Assert.assertNotNull(modelContext1.getOrigModel());
        val suggestCCList1 = modelContext1.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, suggestCCList1.size());
        val suggestedCC10 = suggestCCList1.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC10.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC10.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC10.getExpression());
        val suggestedCC11 = suggestCCList1.get(1);
        Assert.assertEquals("CC_AUTO_2", suggestedCC11.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC11.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC11.getExpression());

        // case 2: can use existing cc for the same cc expression
        String query3 = "select sum(price*item_count) from test_kylin_fact";
        String query4 = "select sum(price+item_count), lstg_format_name from test_kylin_fact group by lstg_format_name";
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query3, query4 });
        smartMaster.runAll();
        val modelContextsList2 = smartMaster.getContext().getModelContexts();
        val modelContext2 = modelContextsList2.get(0);
        Assert.assertNotNull(modelContext2.getOrigModel());
        val suggestCCList2 = modelContext2.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(2, suggestCCList2.size());
        Assert.assertEquals(suggestCCList2, suggestCCList1);
    }

    @Test
    public void testReproposeNewModelWithSameRootFactTable() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1 });
        smartMaster.runAll();
        val modelContext = smartMaster.getContext().getModelContexts().get(0);
        val computedCCList = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedCCList.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC.getExpression());

        // case 1: same cc expression on the same root fact table
        String query2 = "select sum(price*item_count) from test_kylin_fact inner join test_account "
                + "on test_kylin_fact.seller_id = test_account.account_id "
                + "inner join test_country on test_account.account_country = test_country.country";
        String query3 = "select sum(price+item_count) from test_kylin_fact inner join test_order "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query2, query3 });
        smartMaster.runAll();
        val modelContext1 = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext1.getOrigModel());
        val suggestedCCList1 = modelContext1.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList1.size());
        val suggestedCC10 = suggestedCCList1.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC10.getColumnName()); // share 
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC10.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC10.getExpression());

        // case 2: different cc expression on the same root fact table
        val modelContext2 = smartMaster.getContext().getModelContexts().get(1);
        Assert.assertNull(modelContext2.getOrigModel());
        val suggestedCCList2 = modelContext2.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList2.size());
        val suggestedCC20 = suggestedCCList2.get(0);
        Assert.assertEquals("CC_AUTO_2", suggestedCC20.getColumnName()); // share
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC20.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC20.getExpression());
    }

    @Test
    public void testReproposeNewModelWithDifferentFactTable() {
        // init a model with cc
        String query1 = "select sum(price*item_count), price from test_kylin_fact group by price";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1 });
        smartMaster.runAll();
        val modelContext = smartMaster.getContext().getModelContexts().get(0);
        val computedCCList = modelContext.getTargetModel().getComputedColumnDescs();
        val suggestedCC = computedCCList.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", suggestedCC.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC.getExpression());

        // case 3: same cc expression on different root fact table
        String query4 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";
        String query5 = "select sum(price+item_count) from test_order left join test_kylin_fact "
                + "on test_kylin_fact.order_id = test_order.order_id ";
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query4, query5 });
        smartMaster.runAll();
        val modelContext3 = smartMaster.getContext().getModelContexts().get(0);
        Assert.assertNull(modelContext3.getOrigModel());
        val suggestedCCList3 = modelContext3.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList3.size());
        val suggestedCC30 = suggestedCCList3.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC30.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", suggestedCC30.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC30.getExpression());

        // case 4: different cc expression on the different root fact table
        val modelContext4 = smartMaster.getContext().getModelContexts().get(1);
        Assert.assertNull(modelContext4.getOrigModel());
        val suggestedCCList4 = modelContext4.getTargetModel().getComputedColumnDescs();
        Assert.assertEquals(1, suggestedCCList4.size());
        val suggestedCC40 = suggestedCCList4.get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC40.getColumnName());
        Assert.assertEquals("DEFAULT.TEST_ORDER", suggestedCC40.getTableIdentity());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC40.getExpression());
    }

    @Test
    public void testComputedColumnNested() {
        {
            String query = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
            smartMaster.runAll();

            NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertEquals(1, model.getComputedColumnDescs().size());
            ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
            Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
            Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT",
                    computedColumnDesc.getExpression());

            String convertedQuery = convertCC(query);
            String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
            Assert.assertEquals(expectedQuery, convertedQuery);
        }

        {
            String query = "SELECT SUM((PRICE * ITEM_COUNT) + 10), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
            smartMaster.runAll();

            NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertEquals(2, model.getComputedColumnDescs().size());
            ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(1);
            Assert.assertEquals("CC_AUTO_2", computedColumnDesc.getColumnName());
            Assert.assertEquals("TEST_KYLIN_FACT.CC_AUTO_1 + 10", computedColumnDesc.getExpression());
            Assert.assertEquals("(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) + 10",
                    computedColumnDesc.getInnerExpression());

            String convertedQuery = convertCC(query);
            String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_2), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
            Assert.assertEquals(expectedQuery, convertedQuery);
        }
    }

    @Test
    public void testComputedColumnUnnested() {
        String query = "SELECT SUM(PRICE * ITEM_COUNT), AVG((PRICE * ITEM_COUNT) + 10), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(2, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT + 10",
                computedColumnDesc.getExpression());
        computedColumnDesc = model.getComputedColumnDescs().get(1);
        Assert.assertEquals("CC_AUTO_2", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", computedColumnDesc.getExpression());

        String convertedQuery = convertCC(query);
        String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_2), AVG(TEST_KYLIN_FACT._CC_CC_AUTO_1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        Assert.assertEquals(expectedQuery, convertedQuery);
    }

    @Test
    public void testComputedColumnPassOnSumExpr() {
        String query = "SELECT SUM(PRICE_TOTAL), CAL_DT FROM (SELECT PRICE * ITEM_COUNT AS PRICE_TOTAL, CAL_DT FROM TEST_KYLIN_FACT) T GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", computedColumnDesc.getExpression());

        String convertedQuery = convertCC(query);
        String expectedQuery = "SELECT SUM(PRICE_TOTAL), CAL_DT FROM (SELECT TEST_KYLIN_FACT._CC_CC_AUTO_1 AS PRICE_TOTAL, CAL_DT FROM TEST_KYLIN_FACT) T GROUP BY CAL_DT";
        Assert.assertEquals(expectedQuery, convertedQuery);
    }

    @Test
    public void testComputedColumnFailOnSumExpr() {
        String query = "SELECT SUM(PRICE_TOTAL + 1), CAL_DT FROM (SELECT PRICE * ITEM_COUNT AS PRICE_TOTAL, CAL_DT FROM TEST_KYLIN_FACT) T GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerateInfoMap.get(query).isFailed());
    }

    @Test
    public void testComputedColumnFailOnRexOpt() {
        String query = "SELECT SUM(CASE WHEN 9 > 10 THEN 100 ELSE PRICE + 10 END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE + 10", computedColumnDesc.getExpression());

        String convertedQuery = convertCC(query);
        String expectedQuery = "SELECT SUM(TEST_KYLIN_FACT._CC_CC_AUTO_1), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        Assert.assertNotEquals(expectedQuery, convertedQuery);
        Assert.assertNotEquals(query, convertedQuery);
        String actualQuery = "SELECT SUM(CASE WHEN 9 > 10 THEN 100 ELSE TEST_KYLIN_FACT._CC_CC_AUTO_1 END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        Assert.assertEquals(actualQuery, convertedQuery);
    }

    @Test
    public void testComputedColumnsWontImpactFavoriteQuery() {
        // test all named columns rename
        String query = "SELECT SUM(CASE WHEN PRICE > 100 THEN 100 ELSE PRICE END), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE > 100 THEN 100 ELSE TEST_KYLIN_FACT.PRICE END",
                computedColumnDesc.getExpression());
        Assert.assertEquals(1, model.getEffectiveDimensions().size());
        Assert.assertEquals("CAL_DT", model.getEffectiveDimensions().get(0).getName());
        Assert.assertTrue(model.getAllNamedColumns().stream().map(NamedColumn::getName).anyMatch("CC_AUTO_1"::equals));
        Measure measure = model.getEffectiveMeasures().get(100001);
        Assert.assertNotNull(measure);
        Assert.assertTrue(measure.getFunction().isSum());
        Assert.assertEquals("CC_AUTO_1", measure.getFunction().getParameters().get(0).getColRef().getName());

        IndexPlan indexPlan = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllLayouts().get(0).getId());

        // Assert query info is updated
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(query);
        Assert.assertNotNull(accelerateInfo);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }

    @Test
    public void testComputedColumnWithLikeClause() {
        String query = "SELECT 100.00 * SUM(CASE WHEN LSTG_FORMAT_NAME LIKE 'VIP%' THEN 100 ELSE 120 END), CAL_DT "
                + "FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query });
        smartMaster.runAll();

        NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(1, model.getComputedColumnDescs().size());
        ComputedColumnDesc computedColumnDesc = model.getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", computedColumnDesc.getColumnName());
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.LSTG_FORMAT_NAME LIKE 'VIP%' THEN 100 ELSE 120 END",
                computedColumnDesc.getExpression());
        Assert.assertEquals(1, model.getEffectiveDimensions().size());
        Assert.assertEquals("CAL_DT", model.getEffectiveDimensions().get(0).getName());
        Assert.assertTrue(model.getAllNamedColumns().stream().map(NamedColumn::getName).anyMatch("CC_AUTO_1"::equals));
        Measure measure = model.getEffectiveMeasures().get(100001);
        Assert.assertNotNull(measure);
        Assert.assertTrue(measure.getFunction().isSum());
        Assert.assertEquals("CC_AUTO_1", measure.getFunction().getParameters().get(0).getColRef().getName());

        IndexPlan indexPlan = smartMaster.getContext().getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexPlan.getAllLayouts().size());
        Assert.assertEquals(1, indexPlan.getAllLayouts().get(0).getId());

        // Assert query info is updated
        AccelerateInfo accelerateInfo = smartMaster.getContext().getAccelerateInfoMap().get(query);
        Assert.assertNotNull(accelerateInfo);
        Assert.assertFalse(accelerateInfo.isFailed());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }

    @Test
    public void testInferTypesOfCC() {
        String[] sqls = new String[] {
                "select {fn left(lstg_format_name,2)} as name, sum(price*item_count) from test_kylin_fact group by lstg_format_name ",
                "select sum({fn convert(price, SQL_BIGINT)}) as big67 from test_kylin_fact",
                "select sum({fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}) from test_kylin_fact group by lstg_format_name",
                "select {fn year(cast('2012-01-01' as date))} from test_kylin_fact",
                "select {fn convert({fn year(cast('2012-01-01' as date))}, varchar)} from test_kylin_fact",
                "select case when substring(lstg_format_name, 1, 4) like '%ABIN%' then item_count - 10 else item_count end as  item_count_new from test_kylin_fact" //
        };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[1]).isNotSucceed());
        Assert.assertTrue(accelerationInfoMap.get(sqls[2]).isNotSucceed());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerationInfoMap.get(sqls[2]).getFailedCause().getMessage());
        Assert.assertFalse(accelerationInfoMap.get(sqls[3]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[4]).isNotSucceed());
        Assert.assertFalse(accelerationInfoMap.get(sqls[5]).isNotSucceed());

        Assert.assertEquals(3, computedColumns.size());
        Assert.assertEquals("CAST(TEST_KYLIN_FACT.PRICE AS BIGINT)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(0).getDatatype());
        Assert.assertEquals("CAST(LENGTH(substring(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 1, 4)) AS DOUBLE)",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals("DOUBLE", computedColumns.get(1).getDatatype());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT",
                computedColumns.get(2).getInnerExpression().trim());
        Assert.assertEquals("DECIMAL(30,4)", computedColumns.get(2).getDatatype());
    }

    @Test
    public void testInferTypesOfCcWithUnsupportedFunctions() {
        overwriteSystemProp("kylin.query.transformers", "io.kyligence.kap.query.util.ConvertToComputedColumn");
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.RestoreFromComputedColumn");

        String[] sqls = new String[] {
                "select sum(char_length(substring(lstg_format_name from 1 for 4))) from test_kylin_fact",
                "select sum(cast(item_count as bigint) * price) from test_kylin_fact" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CC_AUTO_2", computedColumns.get(0).getColumnName());
        Assert.assertEquals("CAST(TEST_KYLIN_FACT.ITEM_COUNT AS BIGINT) * TEST_KYLIN_FACT.PRICE",
                computedColumns.get(0).getExpression());
        Assert.assertEquals("CAST(TEST_KYLIN_FACT.ITEM_COUNT AS BIGINT) * TEST_KYLIN_FACT.PRICE",
                computedColumns.get(0).getInnerExpression());
        Assert.assertEquals("DECIMAL(38,4)", computedColumns.get(0).getDatatype());

        val accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerationInfoMap.get(sqls[0]).isNotSucceed());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerationInfoMap.get(sqls[0]).getFailedCause().getMessage());
        Assert.assertFalse(accelerationInfoMap.get(sqls[1]).isNotSucceed());
    }

    @Test
    public void testRemoveUnsupportedCC() {
        String[] sqls = new String[] {
                "select {fn left(lstg_format_name,2)} as name, sum(price*item_count) from test_kylin_fact group by lstg_format_name ",
                "select sum({fn convert({fn length(substring(lstg_format_name, 1, 4)) }, double )}) from test_kylin_fact group by lstg_format_name" };

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();
        val modelContexts = smartMaster.getContext().getModelContexts();
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CAST(LENGTH(substring(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 1, 4)) AS DOUBLE)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("CAST(CHAR_LENGTH(SUBSTRING(TEST_KYLIN_FACT.LSTG_FORMAT_NAME FROM 1 FOR 4)) AS DOUBLE)",
                computedColumns.get(0).getExpression().trim());
        Assert.assertEquals("DOUBLE", computedColumns.get(0).getDatatype());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT",
                computedColumns.get(1).getExpression().trim());
        Assert.assertEquals("DECIMAL(30,4)", computedColumns.get(1).getDatatype());

        // set one CC to unsupported
        computedColumns.get(0)
                .setInnerExpression("CAST(LENGTH(SUBSTRING(TEST_KYLIN_FACT.LSTG_FORMAT_NAME FROM 1 FOR 4)) AS DOUBLE)");
        ComputedColumnEvalUtil.evaluateExprAndTypes(targetModel, computedColumns);
        Assert.assertEquals(1, targetModel.getComputedColumnDescs().size());

        // set one CC to unsupported
        computedColumns.get(0).setInnerExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT1");
        try {
            ComputedColumnEvalUtil.evaluateExprAndTypes(targetModel, computedColumns);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(
                    "Auto model failed to evaluate CC TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT1, CC expression not valid.",
                    e.getMessage());
        }
    }

    @Test
    public void testCCOnInnerCol() {
        String[] sqls = new String[] { "select max(cast(LEAF_CATEG_ID*SITE_ID as VARCHAR)) "
                + "from TEST_CATEGORY_GROUPINGS group by META_CATEG_NAME" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CAST(TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID * TEST_CATEGORY_GROUPINGS.SITE_ID AS string)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("VARCHAR", computedColumns.get(0).getDatatype());
    }

    /**
     * test turn on or turn off 'kap.smart.conf.cc-advise-on-sqlnode'
     */
    @Test
    public void testProposeCcmputedColumnOnSqlNode() {
        // turn off
        kylinConfig.setProperty("kap.smart.conf.cc-advise-on-sqlnode", "false");
        String[] sqls = { "select max(timestampdiff(second, time0, time1)) from tdvt.calcs" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProjectTDVT(), sqls);
        smartMaster.runAll();
        List<NSmartContext.NModelContext> modelContextList1 = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContextList1.size());
        Map<String, AccelerateInfo> accelerationInfoMap1 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertTrue(accelerationInfoMap1.get(sqls[0]).isNotSucceed());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerationInfoMap1.get(sqls[0]).getFailedCause().getMessage());

        // turn on
        cleanExistingModelsAndIndexPlans(getProjectTDVT());
        kylinConfig.setProperty("kap.smart.conf.cc-advise-on-sqlnode", "true");
        smartMaster = new NSmartMaster(kylinConfig, getProjectTDVT(), sqls);
        smartMaster.runAll();
        accelerationInfoMap1 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap1.get(sqls[0]).isNotSucceed());
        modelContextList1 = smartMaster.getContext().getModelContexts();
        val targetModel1 = modelContextList1.get(0).getTargetModel();
        val ccList1 = targetModel1.getComputedColumnDescs();
        val cc10 = ccList1.get(0);
        Assert.assertEquals("TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1)", cc10.getExpression());
        Assert.assertEquals("TIMESTAMPDIFF('SECOND', CALCS.TIME0, CALCS.TIME1)", cc10.getInnerExpression());
        Assert.assertEquals("BIGINT", cc10.getDatatype());
    }

    // Unsupported cases at present, see https://github.com/Kyligence/KAP/issues/13866
    @Ignore
    @Test
    public void testCaseWhenRuleWithLenientConformance() {
        // set conformance to LENIENT, after this method it will set to DEFAULT in @After
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");

        /*
         * case 1: group by caseWhenClause contains special function
         */
        String[] sqls = { "select case when int0 > 0 then timestampdiff(second, time0, time1) else null end "
                + "from tdvt.calcs group by 1" };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProjectTDVT(), sqls);
        smartMaster.runAll();
        val modelContextList1 = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContextList1.size());
        val accelerationInfoMap1 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap1.get(sqls[0]).isNotSucceed());
        val targetModel1 = modelContextList1.get(0).getTargetModel();
        val ccList1 = targetModel1.getComputedColumnDescs();
        val cc10 = ccList1.get(0);
        Assert.assertEquals(
                "CASE WHEN CALCS.INT0 > 0 THEN TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1) ELSE NULL END",
                cc10.getExpression());
        Assert.assertEquals(
                "CASE WHEN CALCS.INT0 > 0 THEN TIMESTAMPDIFF('SECOND', CALCS.TIME0, CALCS.TIME1) ELSE NULL END",
                cc10.getInnerExpression());
        Assert.assertEquals("BIGINT", cc10.getDatatype());

        /*
         * case 2: same with case 1 group by column index, should use `kylin.query.calcite.extras-props.conformance=LENIENT`
         */
        cleanExistingModelsAndIndexPlans(getProject());
        sqls = new String[] { "select case when int0 > 0 then timestampdiff(second, time0, time1) else null end as col "
                + "from tdvt.calcs group by col" };
        smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();
        val modelContextList2 = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContextList2.size());
        val accelerationInfoMap2 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap2.get(sqls[0]).isNotSucceed());
        val targetModel2 = modelContextList2.get(0).getTargetModel();
        val ccList2 = targetModel2.getComputedColumnDescs();
        val cc20 = ccList2.get(0);
        Assert.assertEquals(
                "CASE WHEN CALCS.INT0 > 0 THEN TIMESTAMPDIFF(SECOND, CALCS.TIME0, CALCS.TIME1) ELSE NULL END",
                cc20.getExpression());
        Assert.assertEquals(
                "CASE WHEN CALCS.INT0 > 0 THEN TIMESTAMPDIFF('SECOND', CALCS.TIME0, CALCS.TIME1) ELSE NULL END",
                cc20.getInnerExpression());
        Assert.assertEquals("BIGINT", cc20.getDatatype());
    }

    @Test
    public void testCaseWhenWithMoreThanTwoLogicalOperands() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "LENIENT");
        String[] sqls = {"select case when coalesce(item_count, 0) <=10 and coalesce(price, 0) >= 0.0 then 'a'\n"
                + "            when coalesce(item_count, 0) < 0 then 'exception' else null end,\n"
                + "  sum(case when price > 1 and item_count < 10 and seller_id > 20 then 1 else 0 end),\n"
                + "  sum(case when price > 1 and item_count < 5 or seller_id > 10 then price else 0 end),\n"
                + "  sum(case when price + item_count + 1 > 5 then 1 else 0 end)\n"
                + "from test_kylin_fact group by 1"};
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();
        List<NSmartContext.NModelContext> modelContextList1 = smartMaster.getContext().getModelContexts();
        val targetModel1 = modelContextList1.get(0).getTargetModel();
        val ccList1 = targetModel1.getComputedColumnDescs();
        ccList1.sort(Comparator.comparing(ComputedColumnDesc::getExpression));
        Assert.assertEquals(4, ccList1.size());
        val cc10 = ccList1.get(0);
        Assert.assertEquals("CASE WHEN CASE WHEN TEST_KYLIN_FACT.ITEM_COUNT IS NOT NULL THEN TEST_KYLIN_FACT.ITEM_COUNT <= 10 ELSE CAST(TRUE AS BOOLEAN) END AND CASE WHEN TEST_KYLIN_FACT.PRICE IS NOT NULL THEN TEST_KYLIN_FACT.PRICE >= 0.0 ELSE CAST(TRUE AS BOOLEAN) END THEN 'a' WHEN CASE WHEN TEST_KYLIN_FACT.ITEM_COUNT IS NOT NULL THEN TEST_KYLIN_FACT.ITEM_COUNT < 0 ELSE CAST(FALSE AS BOOLEAN) END THEN 'exception' ELSE NULL END",
                cc10.getExpression());
        Assert.assertEquals("CASE WHEN CASE WHEN TEST_KYLIN_FACT.ITEM_COUNT IS NOT NULL THEN TEST_KYLIN_FACT.ITEM_COUNT <= 10 ELSE CAST(TRUE AS BOOLEAN) END AND CASE WHEN TEST_KYLIN_FACT.PRICE IS NOT NULL THEN TEST_KYLIN_FACT.PRICE >= 0.0 ELSE CAST(TRUE AS BOOLEAN) END THEN 'a' WHEN CASE WHEN TEST_KYLIN_FACT.ITEM_COUNT IS NOT NULL THEN TEST_KYLIN_FACT.ITEM_COUNT < 0 ELSE CAST(FALSE AS BOOLEAN) END THEN 'exception' ELSE NULL END",
                cc10.getInnerExpression());
        Assert.assertEquals("VARCHAR", cc10.getDatatype());
        val cc11 = ccList1.get(1);
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT + 1 > 5 THEN 1 ELSE 0 END",
                cc11.getExpression());
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE + TEST_KYLIN_FACT.ITEM_COUNT + 1 > 5 THEN 1 ELSE 0 END",
                cc11.getInnerExpression());
        Assert.assertEquals("INTEGER", cc11.getDatatype());
        val cc12 = ccList1.get(2);
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE > 1 AND TEST_KYLIN_FACT.ITEM_COUNT < 10 "
                + "AND TEST_KYLIN_FACT.SELLER_ID > 20 THEN 1 ELSE 0 END", cc12.getExpression());
        Assert.assertEquals("CASE WHEN TEST_KYLIN_FACT.PRICE > 1 AND TEST_KYLIN_FACT.ITEM_COUNT < 10 "
                + "AND TEST_KYLIN_FACT.SELLER_ID > 20 THEN 1 ELSE 0 END", cc12.getInnerExpression());
        Assert.assertEquals("INTEGER", cc12.getDatatype());
        val cc13 = ccList1.get(3);
        Assert.assertEquals(
                "CASE WHEN TEST_KYLIN_FACT.PRICE > 1 AND TEST_KYLIN_FACT.ITEM_COUNT < 5 "
                        + "OR TEST_KYLIN_FACT.SELLER_ID > 10 THEN TEST_KYLIN_FACT.PRICE ELSE 0 END",
                cc13.getExpression());
        Assert.assertEquals(
                "CASE WHEN TEST_KYLIN_FACT.PRICE > 1 AND TEST_KYLIN_FACT.ITEM_COUNT < 5 "
                        + "OR TEST_KYLIN_FACT.SELLER_ID > 10 THEN TEST_KYLIN_FACT.PRICE ELSE 0 END",
                cc13.getInnerExpression());
        Assert.assertEquals("DECIMAL(19,4)", cc13.getDatatype());
    }

    @Test
    public void testCCOnInnerGroupCol() {
        String[] sqls = new String[] {
                "select is_screen_on, count(1) as num from\n" +
                        "(\n" +
                        "select trans_id,\n" +
                        "  case when TEST_ACCOUNT.ACCOUNT_ID >= 10000336 then 1\n" +
                        "    else 2\n" +
                        "    end as is_screen_on\n" +
                        "from TEST_KYLIN_FACT\n" +
                        "inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n" +
                        ")\n" +
                        "group by is_screen_on"
        };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CASE WHEN TEST_ACCOUNT.ACCOUNT_ID >= 10000336 THEN 1 ELSE 2 END",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("INTEGER", computedColumns.get(0).getDatatype());
    }

    @Test
    public void testCCOnInnerFilterCol() {
        String[] sqls = new String[] {
                "select count(1) as num from\n" +
                        "(\n" +
                        "select trans_id, cal_dt\n" +
                        "from TEST_KYLIN_FACT\n" +
                        "inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n" +
                        "where\n" +
                        "  TEST_ACCOUNT.ACCOUNT_ID + TEST_KYLIN_FACT.ITEM_COUNT >= 10000336\n" +
                        "  and TEST_KYLIN_FACT.ITEM_COUNT > 100\n" +
                        "  or (\n" +
                        "    TEST_KYLIN_FACT.ITEM_COUNT * 100 <> 100000\n" +
                        "    and LSTG_FORMAT_NAME in ('FP-GTC', 'ABIN')\n" +
                        "  )\n" +
                        ")\n" +
                        "group by cal_dt"
        };
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), sqls);
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        Assert.assertEquals(1, modelContexts.size());
        val targetModel = modelContexts.get(0).getTargetModel();
        val computedColumns = targetModel.getComputedColumnDescs();
        val accelerationInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap.get(sqls[0]).isNotSucceed());

        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("TEST_ACCOUNT.ACCOUNT_ID + TEST_KYLIN_FACT.ITEM_COUNT",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("TEST_KYLIN_FACT.ITEM_COUNT * 100",
                computedColumns.get(1).getInnerExpression().trim());
        Assert.assertEquals("BIGINT", computedColumns.get(0).getDatatype());
        Assert.assertEquals("INTEGER", computedColumns.get(1).getDatatype());
    }

    private String convertCC(String originSql) {
        return (new ConvertToComputedColumn()).transform(originSql, getProject(), "DEFAULT");
    }

    private void cleanExistingModelsAndIndexPlans(String project) {
        log.info("clean all existing targetModel in memory.");

        val dataflowMgr = NDataflowManager.getInstance(kylinConfig, project);
        val dataflows = dataflowMgr.listAllDataflows();
        dataflows.forEach(dataflow -> dataflowMgr.dropDataflow(dataflow.getId()));
        Assert.assertTrue(dataflowMgr.listAllDataflows().isEmpty());

        val indexPlanMgr = NIndexPlanManager.getInstance(kylinConfig, project);
        val indexPlans = indexPlanMgr.listAllIndexPlans();
        indexPlans.forEach(indexPlanMgr::dropIndexPlan);
        Assert.assertTrue(indexPlanMgr.listAllIndexPlans().isEmpty());

        val modelMgr = NDataModelManager.getInstance(kylinConfig, project);
        val existingModelIds = modelMgr.listAllModelIds();
        existingModelIds.forEach(modelId -> modelMgr.dropModel(modelMgr.getDataModelDesc(modelId)));
        Assert.assertTrue(modelMgr.listAllModels().isEmpty());
    }

    private String getProjectTDVT() {
        return "tdvt";
    }
}
