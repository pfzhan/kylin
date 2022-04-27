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

package io.kyligence.kap.metadata.recommendation.entity;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;

public class CCRecItemV2Test extends NLocalFileMetadataTestCase {

    private static final String DEFAULT = "default";
    private static final String DF_NAME = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGenDependIdsNormal() {
        CCRecItemV2 itemV2 = new CCRecItemV2();
        ComputedColumnDesc ccDesc = new ComputedColumnDesc();
        ccDesc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc.setTableAlias("TEST_KYLIN_FACT");
        ccDesc.setColumnName("DEAL_AMOUNT");
        ccDesc.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");
        ccDesc.setInnerExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");
        ccDesc.setDatatype("decimal(30,4)");
        ccDesc.setUuid("tmp1594282502295");
        itemV2.setCc(ccDesc);
        NDataModel dateModel = new NDataModel();

        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT);
        IndexPlan indexPlan = indePlanManager.getIndexPlan(DF_NAME);
        BiMap<Integer, TblColRef> effectiveDimCols = indexPlan.getEffectiveDimCols();
        dateModel.setEffectiveCols(ImmutableBiMap.of(11, effectiveDimCols.get(11), 12, effectiveDimCols.get(12)));
        int[] dependIds = itemV2.genDependIds(dateModel);
        Assert.assertEquals(11, dependIds[0]);
        Assert.assertEquals(12, dependIds[1]);
    }
}