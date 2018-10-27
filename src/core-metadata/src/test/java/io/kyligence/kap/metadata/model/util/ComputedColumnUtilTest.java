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

package io.kyligence.kap.metadata.model.util;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class ComputedColumnUtilTest extends NLocalFileMetadataTestCase {

    NDataModelManager modelManager;

    @Before
    public void setUp() {
        this.createTestMetadata();
        modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetCCUsedColsInProject() {
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic_inner");
        TableRef firstTable = model.findFirstTable("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc ccColDesc = firstTable.getColumn("DEAL_YEAR").getColumnDesc();
        Set<String> ccUsedColsInProject = ComputedColumnUtil.getCCUsedColsWithProject("default", ccColDesc);
        Assert.assertTrue(ccUsedColsInProject.size() == 1);
        Assert.assertTrue(ccUsedColsInProject.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));
    }

    @Test
    public void testGetCCUsedColsInModel() {
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic_inner");
        //test param type (model,ColumnDesc)
        TableRef firstTable = model.findFirstTable("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc ccColDesc = firstTable.getColumn("DEAL_YEAR").getColumnDesc();
        Set<String> ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccColDesc);
        Assert.assertTrue(ccUsedColsInModel.size() == 1);
        Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));

        //test param (model, ComputedColumnDesc)
        List<ComputedColumnDesc> computedColumnDescs = model.getComputedColumnDescs();
        for (ComputedColumnDesc ccCol : computedColumnDescs) {
            if (ccCol.getColumnName().equals("DEAL_AMOUNT")) {
                ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccCol);
                Assert.assertTrue(ccUsedColsInModel.size() == 2);
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.PRICE"));
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT"));
            }
            if (ccCol.getColumnName().equals("DEAL_YEAR")) {
                ccUsedColsInModel = ComputedColumnUtil.getCCUsedColsWithModel(model, ccCol);
                Assert.assertTrue(ccUsedColsInModel.size() == 1);
                Assert.assertTrue(ccUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT"));
            }
        }
    }

    @Test
    public void testGetAllCCUsedColsInModel() {
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic_inner");
        Set<String> allCCUsedColsInModel = ComputedColumnUtil.getAllCCUsedColsInModel(model);
        Assert.assertTrue(allCCUsedColsInModel.size() == 6);
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.PRICE")); //belong to cc "DEAL_AMOUNT"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT")); //belong to cc "DEAL_AMOUNT"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.CAL_DT")); //belong to cc "DEAL_YEAR"
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST1"));
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST2"));
        Assert.assertTrue(allCCUsedColsInModel.contains("DEFAULT.TEST_KYLIN_FACT.NEST3"));
    }
}
