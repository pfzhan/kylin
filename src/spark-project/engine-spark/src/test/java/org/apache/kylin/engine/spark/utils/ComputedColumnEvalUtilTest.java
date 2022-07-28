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

package org.apache.kylin.engine.spark.utils;

import static org.apache.kylin.common.exception.QueryErrorCode.CC_EXPRESSION_ILLEGAL;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ComputedColumnEvalUtilTest extends NLocalWithSparkSessionTest {

    @Test
    public void testRemoveUnsupportedCCWithEvenCase() {
        List<ComputedColumnDesc> computedColumns = Lists.newArrayList();

        ComputedColumnDesc computedColumnDesc1 = new ComputedColumnDesc();
        computedColumnDesc1
                .setInnerExpression("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)");
        computedColumnDesc1.setColumnName("cc_1");

        ComputedColumnDesc computedColumnDesc2 = new ComputedColumnDesc();
        computedColumnDesc2.setInnerExpression("INITCAPB(TEST_KYLIN_FACT.LSTG_FORMAT_NAME)");
        computedColumnDesc1.setColumnName("cc_2");

        ComputedColumnDesc computedColumnDesc3 = new ComputedColumnDesc();
        computedColumnDesc3.setInnerExpression("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)");
        computedColumnDesc1.setColumnName("cc_3");

        ComputedColumnDesc computedColumnDesc4 = new ComputedColumnDesc();
        computedColumnDesc4.setInnerExpression("TO_CHAR(TEST_KYLIN_FACT.CAL_DT, 'YEAR')");
        computedColumnDesc1.setColumnName("cc_4");

        computedColumns.add(computedColumnDesc1);
        computedColumns.add(computedColumnDesc2);
        computedColumns.add(computedColumnDesc3);
        computedColumns.add(computedColumnDesc4);

        NDataModel nDataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(nDataModel, computedColumns);
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)",
                computedColumns.get(1).getInnerExpression().trim());
    }

    @Test
    public void testRemoveUnsupportedCCWithOddCase() {
        List<ComputedColumnDesc> computedColumns = Lists.newArrayList();

        ComputedColumnDesc computedColumnDesc1 = new ComputedColumnDesc();
        computedColumnDesc1
                .setInnerExpression("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)");
        computedColumnDesc1.setColumnName("cc_1");

        ComputedColumnDesc computedColumnDesc3 = new ComputedColumnDesc();
        computedColumnDesc3.setInnerExpression("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)");
        computedColumnDesc1.setColumnName("cc_3");

        ComputedColumnDesc computedColumnDesc4 = new ComputedColumnDesc();
        computedColumnDesc4.setInnerExpression("TO_CHAR(TEST_KYLIN_FACT.CAL_DT, 'YEAR')");
        computedColumnDesc1.setColumnName("cc_4");

        computedColumns.add(computedColumnDesc1);
        computedColumns.add(computedColumnDesc3);
        computedColumns.add(computedColumnDesc4);

        NDataModel nDataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(nDataModel, computedColumns);
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)",
                computedColumns.get(1).getInnerExpression().trim());
    }

    @Test
    public void testRemoveUnsupportedCCWithSingleCase() {
        List<ComputedColumnDesc> computedColumns = Lists.newArrayList();

        ComputedColumnDesc computedColumnDesc1 = new ComputedColumnDesc();
        computedColumnDesc1
                .setInnerExpression("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)");
        computedColumnDesc1.setColumnName("cc_1");

        computedColumns.add(computedColumnDesc1);

        NDataModel nDataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(nDataModel, computedColumns);
        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
    }

    @Test
    public void testUnsupportedCCInManualMaintainType() {

        NDataModel dataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        // case 1: resolve column failed, but table schema not changed.
        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setInnerExpression("TEST_KYLIN_FACT.LSTG_FORMAT_NAME2 + '1'");
        cc1.setColumnName("CC_1");
        try {
            ComputedColumnEvalUtil.evaluateExprAndType(dataModel, cc1);
            Assert.fail();
        } catch (org.apache.kylin.common.exception.KylinException e) {
            Assert.assertEquals(CC_EXPRESSION_ILLEGAL.toErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testResolveCCName() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String project = "default";
        List<NDataModel> otherModels = Lists.newArrayList();
        NDataModel dataModel = NDataModelManager.getInstance(config, project)
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertTrue(dataModel.getComputedColumnDescs().isEmpty());

        // add a good computed column
        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setColumnName(ComputedColumnUtil.DEFAULT_CC_NAME);
        cc1.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        cc1.setExpression("SUBSTRING(LSTG_FORMAT_NAME FROM 1 FOR 4)");
        cc1.setInnerExpression("SUBSTRING(LSTG_FORMAT_NAME, 1, 4)");
        cc1.setDatatype("ANY");
        dataModel.getComputedColumnDescs().add(cc1);
        Assert.assertTrue(ComputedColumnEvalUtil.resolveCCName(cc1, dataModel, otherModels));
        Assert.assertEquals("CC_AUTO_1", cc1.getColumnName());

        // add a bad computed column
        ComputedColumnDesc cc2 = new ComputedColumnDesc();
        cc2.setColumnName(ComputedColumnUtil.DEFAULT_CC_NAME);
        cc2.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        cc2.setExpression("CASE(IN($3, 'Auction', 'FP-GTC'), 'Auction', $3)");
        cc2.setInnerExpression("CASE(IN($3, 'Auction', 'FP-GTC'), 'Auction', $3)");
        cc2.setDatatype("ANY");
        dataModel.getComputedColumnDescs().add(cc2);
        boolean rst = ComputedColumnEvalUtil.resolveCCName(cc2, dataModel, otherModels);
        Assert.assertFalse(rst);
        Assert.assertEquals("CC_AUTO_1", cc2.getColumnName());
        Assert.assertEquals(2, dataModel.getComputedColumnDescs().size());
        dataModel.getComputedColumnDescs().remove(cc2); // same logic code in NComputedColumnProposer
        Assert.assertEquals(1, dataModel.getComputedColumnDescs().size());

        // add a good computed column again
        ComputedColumnDesc cc3 = new ComputedColumnDesc();
        cc3.setColumnName(ComputedColumnUtil.DEFAULT_CC_NAME);
        cc3.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        cc3.setExpression("YEAR(TEST_KYLIN_FACT.CAL_DT)");
        cc3.setInnerExpression("YEAR(TEST_KYLIN_FACT.CAL_DT)");
        cc3.setDatatype("ANY");
        dataModel.getComputedColumnDescs().add(cc3);
        Assert.assertTrue(ComputedColumnEvalUtil.resolveCCName(cc3, dataModel, otherModels));
        Assert.assertEquals("CC_AUTO_2", cc3.getColumnName());
    }

    @Test
    public void testRemoveUnsupportedCCWithAllSuccessCase() {
        List<ComputedColumnDesc> computedColumns = Lists.newArrayList();

        ComputedColumnDesc computedColumnDesc1 = new ComputedColumnDesc();
        computedColumnDesc1
                .setInnerExpression("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)");
        computedColumnDesc1.setColumnName("cc_1");

        ComputedColumnDesc computedColumnDesc3 = new ComputedColumnDesc();
        computedColumnDesc3.setInnerExpression("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)");
        computedColumnDesc1.setColumnName("cc_3");

        computedColumns.add(computedColumnDesc1);
        computedColumns.add(computedColumnDesc3);

        NDataModel nDataModel = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");

        ComputedColumnEvalUtil.evaluateExprAndTypeBatch(nDataModel, computedColumns);
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)",
                computedColumns.get(1).getInnerExpression().trim());
    }

    @Test
    public void testCreateNewCCName() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String project = "default";
        List<NDataModel> otherModels = Lists.newArrayList();
        NDataModel dataModel = NDataModelManager.getInstance(config, project)
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertTrue(dataModel.getComputedColumnDescs().isEmpty());
        otherModels = NDataModelManager.getInstance(config, project).listAllModels();
        // first CC will named CC_AUTO_1
        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        final String ccExp1 = "SUBSTRING(TEST_KYLIN_FACT.LSTG_FORMAT_NAME FROM 1 FOR 4)";
        cc1.setColumnName("CC_AUTO_1");
        cc1.setExpression(ccExp1);
        dataModel.getComputedColumnDescs().add(cc1);
        String sharedName = ComputedColumnUtil.shareCCNameAcrossModel(cc1, dataModel, otherModels);
        Assert.assertEquals("CC_AUTO_1", sharedName);
    }

    @Test
    public void testNotShareExpressionUnmatchingSubgraph() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String project = "default";
        NDataModel dataModel = NDataModelManager.getInstance(config, project)
                .getDataModelDesc("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertTrue(dataModel.getComputedColumnDescs().isEmpty());
        // first CC will named CC_AUTO_1
        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        final String ccExp1 = "TEST_ORDER.BUYER_ID + 1";
        cc1.setColumnName("CC_AUTO_1");
        cc1.setExpression(ccExp1);

        NDataModel copyModel = JsonUtil.readValue(JsonUtil.writeValueAsIndentString(dataModel), NDataModel.class);
        copyModel.getJoinTables().get(0).getJoin().setType("inner");
        dataModel.getComputedColumnDescs().add(cc1);
        copyModel.setProject(project);
        copyModel.init(KylinConfig.getInstanceFromEnv());

        String sharedName = ComputedColumnUtil.shareCCNameAcrossModel(cc1, copyModel, Arrays.asList(dataModel));
        Assert.assertEquals(null, sharedName);
    }

}
