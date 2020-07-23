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

import java.util.Collections;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.val;

public class GreedyModelTreesBuilderTest extends NLocalWithSparkSessionTest {

    @Ignore("Cannot propose in PureExpertMode")
    @Test
    public void testPartialJoinInExpertMode() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { sqls[0] });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        val originalIndexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        Assert.assertEquals(1, originalIndexPlan.getAllIndexes().size());

        AccelerationContextUtil.transferProjectToPureExpertMode(getTestConfig(), "newten");

        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runWithContext();

        val indexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        val layouts = indexPlan.getAllLayouts();
        val columns = Lists.<String> newArrayList();

        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());
        Assert.assertEquals(3, layouts.size());
        layouts.forEach(layout -> {
            Assert.assertTrue(layout.getIndex().isTableIndex());
            Assert.assertEquals(1, layout.getColumns().size());
            columns.add(layout.getColumns().get(0).getIdentity());
        });
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });
        Collections.sort(columns);
        Assert.assertArrayEquals(columns.toArray(), new String[] { "TEST_KYLIN_FACT.CAL_DT",
                "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.TRANS_ID" });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    /**
     *  acceleration A , A <-> B , A <-> B <-> C when exist model A <-> B <-> C
     *
     *  expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeContainEachOther() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { sqls[0] });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();


        val originalIndexPlan = NIndexPlanManager.getInstance(getTestConfig(), "newten").listAllIndexPlans().get(0);
        Assert.assertEquals(1, originalIndexPlan.getAllIndexes().size());

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");

        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();
        val recommMap = context2.getRecommendationMap();
        val model = smartMaster2.getContext().getModelContexts().get(0).getTargetModel();
        val layoutRecomms = recommMap.get(model).getLayoutRecommendations();
        val columns = Lists.<String> newArrayList();

        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());
        Assert.assertNotNull(recommMap);
        Assert.assertEquals(4, recommMap.get(model).getRecommendationsCount());
        Assert.assertEquals(2, recommMap.get(model).getLayoutRecommendations().size());
        layoutRecomms.forEach(layoutRecomm -> {
            Assert.assertFalse(layoutRecomm.isAggIndex());
            Assert.assertEquals(1, layoutRecomm.getLayout().getColOrder().size());
            val columnId = layoutRecomm.getLayout().getColOrder().get(0);
            columns.add(model.getColRef(columnId).getIdentity());
        });
        Collections.sort(columns);
        Assert.assertArrayEquals(columns.toArray(),
                new String[] { "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "TEST_KYLIN_FACT.TRANS_ID" });
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    /**
     *  acceleration A <-> B , A <-> C, A <-> D when exist model A <-> B <-> C
     *
     *  expect A <-> B , A <-> C accelerate succeed, A <-> D accelerate failed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeContainFail() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID"};

        // create model A join B join C
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY"
                });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results
        Assert.assertEquals(2, smartMaster2.getContext().getModelContexts().size());

        Assert.assertFalse(smartMaster2.getContext().getAccelerateInfoMap().get(sqls[0]).isNotSucceed());
        Assert.assertFalse(smartMaster2.getContext().getAccelerateInfoMap().get(sqls[1]).isNotSucceed());
        Assert.assertTrue(smartMaster2.getContext().getAccelerateInfoMap().get(sqls[2]).isNotSucceed());

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    /**
     *  acceleration A <-> B , A <-> C, A <-> D when exist model A <-> B <-> C <-> D
     *
     *  expect A <-> B , A <-> C, A <-> D accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeNotContainEachOther() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID"};

        // create model A join B join C join D
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY inner join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID"
                });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());

        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    /**
     *  acceleration A <-> B , A <-> C, D when exist model A <-> B <-> C, D
     *
     *  expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeWithMultiModel() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select TEST_ORDER.ORDER_ID from TEST_ORDER"};

        // create model A join B join C, D
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                        "select TEST_ORDER.ORDER_ID from TEST_ORDER"
                });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results, all accelerate succeed.
        Assert.assertEquals(2, smartMaster2.getContext().getModelContexts().size());

        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }


    /**
     *  acceleration A <-> B , A -> D when exist model A <-> B <-> C -> D
     *
     *  expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeMixInnerJoinAndLeftJoin() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY",
                "select test_kylin_fact.CAL_DT from test_kylin_fact left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" };

        // create model A join B join C left join D
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten", new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY left join TEST_ORDER on test_kylin_fact.ORDER_ID = TEST_ORDER.ORDER_ID" });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    /**
     *  acceleration A <-> B , A <-> B' when exist model B' <-> A <-> B
     *
     *  expect all accelerate succeed.
     */
    @Test
    public void testPartialJoinInSemiAutoModeTableOfSameName() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account1 on test_kylin_fact.seller_id = account1.account_id",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account2 on test_kylin_fact.ORDER_ID = account2.ACCOUNT_SELLER_LEVEL" };

        // create model A join B join C
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account as account1 on test_kylin_fact.seller_id = account1.account_id inner join test_account as account2 on test_kylin_fact.ORDER_ID = account2.ACCOUNT_SELLER_LEVEL "
                });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results, all accelerate succeed.
        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    @Test
    public void testPartialJoinInSemiAutoModeWithCC() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select count(test_kylin_fact.trans_ID+1),count(test_kylin_fact.trans_id+1) from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select count(test_kylin_fact.TRANS_ID+1) from test_kylin_fact inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY"};

        // create model A join B join C
        val context1 = AccelerationContextUtil.newModelCreateContext(getTestConfig(), "newten",
                new String[] { "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_kylin_fact.LSTG_FORMAT_NAME = test_country.COUNTRY"
                });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        // accelerate
        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), "newten");
        val context2 = AccelerationContextUtil.newModelReuseContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runSuggestModel();

        // validation results
        Assert.assertEquals(1, smartMaster2.getContext().getModelContexts().size());
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }

    @Test
    public void testPartialJoinInSmartMode() {
        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "true");

        String[] sqls = new String[] {
                "select test_kylin_fact.cal_dt from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id inner join test_country on test_account.account_country = test_country.country",
                "select test_kylin_fact.lstg_format_name from test_kylin_fact inner join test_account on test_kylin_fact.seller_id = test_account.account_id",
                "select test_kylin_fact.trans_id from test_kylin_fact" };

        // create model A join B join C
        val context1 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", new String[] { sqls[0] });
        val smartMaster = new NSmartMaster(context1);
        smartMaster.runWithContext();

        val originalModels = NDataModelManager.getInstance(getTestConfig(), "newten").listAllModels();
        Assert.assertEquals(1, originalModels.size());

        val context2 = AccelerationContextUtil.newSmartContext(getTestConfig(), "newten", sqls);
        val smartMaster2 = new NSmartMaster(context2);
        smartMaster2.runWithContext();

        Assert.assertEquals(3, smartMaster2.getContext().getModelContexts().size());
        Assert.assertEquals(3, NDataModelManager.getInstance(getTestConfig(), "newten").listAllModels().size());
        smartMaster2.getContext().getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            Assert.assertFalse(accelerateInfo.isNotSucceed());
        });

        getTestConfig().setProperty("kylin.query.match-partial-inner-join-model", "false");
    }
}
