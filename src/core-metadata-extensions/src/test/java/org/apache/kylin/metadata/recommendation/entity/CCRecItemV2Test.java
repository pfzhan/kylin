/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.recommendation.entity;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;


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
