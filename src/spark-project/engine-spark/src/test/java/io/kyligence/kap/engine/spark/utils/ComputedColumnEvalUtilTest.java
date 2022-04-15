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

package io.kyligence.kap.engine.spark.utils;

import static org.apache.kylin.common.exception.QueryErrorCode.CC_EXPRESSION_ILLEGAL;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;
import io.kyligence.kap.metadata.project.NProjectManager;

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
        // set maintain model type to manual
        final NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        final ProjectInstance projectUpdate = projectManager
                .copyForWrite(projectManager.getProject(dataModel.getProject()));
        projectUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectUpdate);

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
        copyModel.init(KylinConfig.getInstanceFromEnv(),
                NTableMetadataManager.getInstance(config, project).getAllTablesMap());

        String sharedName = ComputedColumnUtil.shareCCNameAcrossModel(cc1, copyModel, Arrays.asList(dataModel));
        Assert.assertEquals(null, sharedName);
    }

}