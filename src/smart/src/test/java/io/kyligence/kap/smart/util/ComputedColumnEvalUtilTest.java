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

package io.kyligence.kap.smart.util;

import com.google.common.collect.Lists;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

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

        ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, computedColumns);
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

        ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, computedColumns);
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

        ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, computedColumns);
        Assert.assertEquals(1, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
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

        ComputedColumnEvalUtil.evaluateExprAndTypes(nDataModel, computedColumns);
        Assert.assertEquals(2, computedColumns.size());
        Assert.assertEquals("CONCAT(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, TEST_KYLIN_FACT.LSTG_FORMAT_NAME)",
                computedColumns.get(0).getInnerExpression().trim());
        Assert.assertEquals("SUBSTR(TEST_KYLIN_FACT.LSTG_FORMAT_NAME, 2)",
                computedColumns.get(1).getInnerExpression().trim());
    }

}