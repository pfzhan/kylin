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

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class IndexPlanShrinkProposerTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void testMergeAggIndexOfSameDimension() {
        String sumSql = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        String maxSql = "select max(PRICE)\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";

        val context = ProposerJob.proposeForAutoMode(getTestConfig(), getProject(), new String[] { sumSql, maxSql });

        Assert.assertFalse(context.getAccelerateInfoMap().get(sumSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(maxSql).isNotSucceed());

        Assert.assertEquals(1, context.getModelContexts().size());
        val indexplan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(1, indexplan.getIndexes().size());
        Assert.assertEquals(1, indexplan.getAllLayouts().size());
        Assert.assertEquals(2, indexplan.getAllLayouts().get(0).getDimsIds().size());
        Assert.assertEquals(3, indexplan.getAllLayouts().get(0).getMeasureIds().size());
        Assert.assertEquals("[100000, 100001, 100002]", indexplan.getAllLayouts().get(0).getMeasureIds().toString());
    }

    @Test
    public void testMergeAggIndexOfSameDimensionWithTableIndex() {
        String sumSql = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        String maxSql = "select max(PRICE)\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        String rawQuery = "select SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT\n"
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";

        val context = ProposerJob.proposeForAutoMode(getTestConfig(), getProject(),
                new String[] { sumSql, maxSql, rawQuery });

        Assert.assertFalse(context.getAccelerateInfoMap().get(sumSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(maxSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(rawQuery).isNotSucceed());

        Assert.assertEquals(1, context.getModelContexts().size());
        val indexplan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(2, indexplan.getIndexes().size());
        Assert.assertEquals(2, indexplan.getAllLayouts().size());
    }

    @Test
    public void testMergeIndexOfSameDimWithTableIndexAndManualLayout() {
        String prepareSql = "select count(SELLER_ID) FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        val initialContext = ProposerJob.proposeForAutoMode(getTestConfig(), getProject(), new String[] { prepareSql });
        initialContext.saveMetadata();
        AccelerationContextUtil.onlineModel(initialContext);

        Assert.assertFalse(initialContext.getAccelerateInfoMap().get(prepareSql).isNotSucceed());

        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        Assert.assertEquals(1, initialContext.getModelContexts().size());
        val firstModel = initialContext.getModelContexts().get(0).getTargetModel();
        val indexPlan = initialContext.getModelContexts().get(0).getTargetIndexPlan();
        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            layoutEntity.setManual(true);
            layoutEntity.setAuto(false);
        });

        String sumSql = "select sum(ITEM_COUNT) as ITEM_CNT\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        String maxSql = "select max(PRICE)\n" + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n"
                + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT";
        String rawQuery = "select SELLER_ACCOUNT.ACCOUNT_COUNTRY, CAL_DT\n"
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" + "LEFT JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n"
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" + "LEFT JOIN TEST_ORDER as TEST_ORDER\n"
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" + "LEFT JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n"
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n";

        val context = ProposerJob.genOptRec(getTestConfig(), getProject(), new String[] { sumSql, maxSql, rawQuery });
        AccelerationContextUtil.onlineModel(context);
        Assert.assertFalse(context.getAccelerateInfoMap().get(sumSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(maxSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(rawQuery).isNotSucceed());

        Assert.assertEquals(1, context.getModelContexts().size());
        val secondModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(firstModel.getId(), secondModel.getId());
        val indexplan = context.getModelContexts().get(0).getTargetIndexPlan();
        Assert.assertEquals(3, indexplan.getIndexes().size());
        Assert.assertEquals(3, indexplan.getAllLayouts().size());
    }

    /**
     * first sql -> index1 A B m1 m2
     * second sqls -> index2 A B m1 index3 A B m2, index1 A B m1 m2(first sql generate), B A m1 m2,
     *  finally merge -> index1 A B m1 m2, B A m1 m2
     */

    @Test
    public void testMergeIndexOfSameDimensionWithSameExistIndex() {
        String prepareSql = "select max(C_CUSTKEY), sum(C_CUSTKEY) from TPCH.CUSTOMER where C_PHONE = '1' GROUP BY C_NAME";
        val initialContext = ProposerJob.proposeForAutoMode(getTestConfig(), getProject(), new String[] { prepareSql });
        initialContext.saveMetadata();
        AccelerationContextUtil.onlineModel(initialContext);

        Assert.assertFalse(initialContext.getAccelerateInfoMap().get(prepareSql).isNotSucceed());

        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "true");
        Assert.assertEquals(1, initialContext.getModelContexts().size());
        val firstModel = initialContext.getModelContexts().get(0).getTargetModel();
        val indexPlan = initialContext.getModelContexts().get(0).getTargetIndexPlan();
        indexPlan.getAllLayouts().forEach(layoutEntity -> {
            layoutEntity.setManual(true);
            layoutEntity.setAuto(false);
        });

        String sumSql = "select max(C_CUSTKEY) from TPCH.CUSTOMER where C_PHONE= '1' GROUP BY C_NAME";
        String maxSql = "select sum(C_CUSTKEY) from TPCH.CUSTOMER where C_PHONE= '1' GROUP BY C_NAME";
        String rawQuery = "select max(C_CUSTKEY), sum(C_CUSTKEY) from TPCH.CUSTOMER where C_NAME = '1' GROUP BY C_PHONE";

        val context = ProposerJob.genOptRec(getTestConfig(), getProject(), new String[] { sumSql, maxSql, rawQuery });
        AccelerationContextUtil.onlineModel(context);
        Assert.assertFalse(context.getAccelerateInfoMap().get(sumSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(maxSql).isNotSucceed());
        Assert.assertFalse(context.getAccelerateInfoMap().get(rawQuery).isNotSucceed());

        Assert.assertEquals(1, context.getModelContexts().size());
        val secondModel = context.getModelContexts().get(0).getTargetModel();
        Assert.assertEquals(firstModel.getId(), secondModel.getId());
        val indexplan = context.getModelContexts().get(0).getTargetIndexPlan();

        Assert.assertEquals(1, indexplan.getIndexes().size());
        Assert.assertEquals(2, indexplan.getAllLayouts().size());

    }
}