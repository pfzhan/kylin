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

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModel.Measure;
import io.kyligence.kap.metadata.model.NDataModel.NamedColumn;
import io.kyligence.kap.query.util.ConvertToComputedColumn;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;
import lombok.val;

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

    @Test
    public void testReuseProposedCC() {
        // initial propose, partially will fail
        String query1 = "select price*item_count from test_kylin_fact"; // will propose a tableIndex
        String query2 = "select sum(price*item_count) from test_kylin_fact";
        String query3 = "select {fn left(lstg_format_name,-4)} as name, sum(price*item_count) "
                + "from test_kylin_fact group by lstg_format_name";
        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1, query2, query3 });
        smartMaster.runAll();
        val modelContexts = smartMaster.getContext().getModelContexts();
        val suggestedCC = modelContexts.get(0).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC.getExpression());
        val accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerateInfoMap.get(query1).isBlocked());
        Assert.assertFalse(accelerateInfoMap.get(query2).isBlocked());
        Assert.assertTrue(accelerateInfoMap.get(query3).isBlocked());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerateInfoMap.get(query3).getBlockingCause().getMessage());

        val targetIndexPlan = modelContexts.get(0).getTargetIndexPlan();
        final List<IndexEntity> indexes = targetIndexPlan.getIndexes();
        Assert.assertEquals(20000000000L, indexes.get(0).getId());
        Assert.assertEquals(0L, indexes.get(1).getId());

        // case 1: incompatible with the proposed model, cannot reuse already existing model and index-plan
        String query4 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query4 });
        smartMaster.runAll();
        val modelContexts1 = smartMaster.getContext().getModelContexts();
        val suggestedCC1 = modelContexts1.get(0).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC1.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC1.getExpression());
        val accelerationInfoMap1 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertFalse(accelerationInfoMap1.get(query4).isBlocked());

        // case2: compatible but partially reused
        // The 'price*item_count' should be replaced by auto_cc_1, query1 will success, however query2 will fail
        // (reason: the second param of left() cannot be a negative number, left() also support by spark2.3+)
        String query5 = "select sum(price*item_count), price from test_kylin_fact group by price";
        String query6 = "select {fn left(lstg_format_name,-2)} as name, sum(price*item_count) "//
                + " from test_kylin_fact group by lstg_format_name";
        smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query5, query6 });
        smartMaster.runAll();
        val modelContexts2 = smartMaster.getContext().getModelContexts();
        val suggestedCC2 = modelContexts2.get(0).getTargetModel().getComputedColumnDescs().get(0);
        val accelerateInfoMap2 = smartMaster.getContext().getAccelerateInfoMap();
        Assert.assertEquals("CC_AUTO_1", suggestedCC2.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC2.getExpression());
        Assert.assertFalse(accelerateInfoMap2.get(query5).isBlocked());
        Assert.assertTrue(accelerateInfoMap2.get(query6).isBlocked());
        Assert.assertEquals("Table not found by UNKNOWN_ALIAS",
                accelerateInfoMap2.get(query6).getBlockingCause().getMessage());
    }

    @Test
    public void testReuseProposedCCInOtherModel() {
        String query1 = "select sum(price*item_count) from test_kylin_fact";
        String query2 = "select sum(price*item_count), account_id from test_account "
                + "left join test_kylin_fact on test_kylin_fact.seller_id = test_account.account_id "
                + "group by account_id";

        NSmartMaster smartMaster = new NSmartMaster(kylinConfig, getProject(), new String[] { query1, query2 });
        smartMaster.runAll();

        val modelContexts = smartMaster.getContext().getModelContexts();
        val suggestedCC1 = modelContexts.get(0).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC1.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC1.getExpression());

        val suggestedCC2 = modelContexts.get(1).getTargetModel().getComputedColumnDescs().get(0);
        Assert.assertEquals("CC_AUTO_1", suggestedCC2.getColumnName());
        Assert.assertEquals("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT", suggestedCC2.getExpression());

        smartMaster.getContext().getAccelerateInfoMap().forEach((sql, accelerationInfo) -> {
            Assert.assertFalse(accelerationInfo.isBlocked());
        });
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
            Assert.assertEquals("(TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT) + 10 ",
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
        Assert.assertTrue(accelerateInfoMap.get(query).isBlocked());
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
        Assert.assertFalse(accelerateInfo.isBlocked());
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
        Assert.assertFalse(accelerateInfo.isBlocked());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().size());
        Assert.assertEquals(1, accelerateInfo.getRelatedLayouts().iterator().next().getLayoutId());
    }

    private String convertCC(String originSql) {
        return (new ConvertToComputedColumn()).transform(originSql, getProject(), "DEFAULT");
    }
}
