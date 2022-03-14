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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;
import lombok.val;

public class AutoLookupTest extends AutoTestBase {

    @Test
    @Ignore
    public void testLookup() throws Exception {
        {
            String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                    + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                    + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                    + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                    + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                    + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                    + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                    + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n" //
                    + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                    + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                    + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                    + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                    + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n"
                    + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                    + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                    + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                    + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
            val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                    new String[] { modelQuery });
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(null);
            context.saveMetadata();
            AccelerationContextUtil.onlineModel(context);
            Assert.assertEquals(1, smartMaster.getContext().getModelContexts().size());
            Assert.assertNotNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(1, models.size());
            NDataModel model = models.get(0);
            Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));

            buildAllModels(kylinConfig, getProject());
            Assert.assertEquals(1, ExecAndComp.queryModelWithoutCompute(getProject(), modelQuery).toDF().count());
        }

        {
            String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
            val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                    new String[] { lookupQuery });
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(null);
            context.saveMetadata();
            AccelerationContextUtil.onlineModel(context);
            Assert.assertEquals(1, smartMaster.getContext().getModelContexts().size());
            Assert.assertNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(1, models.size());
            NDataModel model = models.get(0);
            Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));

            // Use snapshot, no need to build
            Assert.assertEquals(1, ExecAndComp.queryModelWithoutCompute(getProject(), lookupQuery).toDF().count());
        }
    }

    @Test
    @Ignore
    public void testLookupByStep() {
        {
            String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                    + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                    + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                    + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                    + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                    + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                    + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                    + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                    + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n"
                    + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                    + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                    + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                    + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                    + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n" //
                    + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                    + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                    + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                    + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
            val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                    new String[] { modelQuery });
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(null);
            context.saveMetadata();
            AccelerationContextUtil.onlineModel(context);
            Assert.assertEquals(1, smartMaster.getContext().getModelContexts().size());
            Assert.assertNotNull(smartMaster.getContext().getModelContexts().get(0).getTargetModel());

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(1, models.size());
            NDataModel model = models.get(0);
            Assert.assertTrue(model.isLookupTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));
        }

        {
            String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
            val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                    new String[] { lookupQuery });
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(null);
            context.saveMetadata();
            AccelerationContextUtil.onlineModel(context);
            Assert.assertEquals(1, smartMaster.getContext().getModelContexts().size());
            NDataModel model = smartMaster.getContext().getModelContexts().get(0).getTargetModel();
            Assert.assertNotNull(model);
            Assert.assertTrue(model.isFactTable("DEFAULT.TEST_CATEGORY_GROUPINGS"));

            List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject())
                    .listUnderliningDataModels();
            Assert.assertEquals(2, models.size());
        }
    }

    @Test
    public void testNoLookupInBatch() throws Exception {
        String modelQuery = "select sum(ITEM_COUNT) as ITEM_CNT\n" //
                + "FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT\n" //
                + "INNER JOIN TEST_ORDER as TEST_ORDER\n" //
                + "ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT\n" //
                + "ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT\n" //
                + "ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID\n" //
                + "INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT\n" //
                + "ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT\n" //
                + "INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS\n" //
                + "ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID\n"
                + "INNER JOIN EDW.TEST_SITES as TEST_SITES\n" //
                + "ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID\n" //
                + "INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM\n" //
                + "ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD\n" //
                + "INNER JOIN TEST_COUNTRY as BUYER_COUNTRY\n" //
                + "ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY\n" //
                + "INNER JOIN TEST_COUNTRY as SELLER_COUNTRY\n" //
                + "ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY limit 1";
        String lookupQuery = "select leaf_categ_id from test_category_groupings group by leaf_categ_id limit 1";
        val context = AccelerationContextUtil.newSmartContext(kylinConfig, getProject(),
                new String[] { modelQuery, lookupQuery });
        SmartMaster smartMaster = new SmartMaster(context);
        smartMaster.runUtWithContext(null);
        context.saveMetadata();
        AccelerationContextUtil.onlineModel(context);
        Assert.assertEquals(2, smartMaster.getContext().getModelContexts().size());

        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, getProject()).listUnderliningDataModels();
        Assert.assertEquals(2, models.size());

        buildAllModels(kylinConfig, getProject());
        Assert.assertEquals(1, ExecAndComp.queryModelWithoutCompute(getProject(), modelQuery).toDF().count());
        Assert.assertEquals(1, ExecAndComp.queryModelWithoutCompute(getProject(), lookupQuery).toDF().count());
    }
}
