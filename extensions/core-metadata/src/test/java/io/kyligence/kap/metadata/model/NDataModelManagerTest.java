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

package io.kyligence.kap.metadata.model;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel.Measure;

public class NDataModelManagerTest extends NLocalFileMetadataTestCase {
    private NDataModelManager mgrDefault;
    private String projectDefualt = "default";
    private String modelBasic = "nmodel_basic";
    private String modelTest = "model_test";
    private String ownerTest = "owner_test";

    @Before
    public void setUp() {
        createTestMetadata();
        mgrDefault = NDataModelManager.getInstance(getTestConfig(), projectDefualt);
    }

    @Test
    public void testGetInstance() {
        NDataModelManager mgrSsb = NDataModelManager.getInstance(getTestConfig(), "ssb");
        Assert.assertNotEquals(mgrDefault, mgrSsb);

        NDataModelManager mgrInvalid = NDataModelManager.getInstance(getTestConfig(), "not_exist_prj");
        Assert.assertEquals(0, mgrInvalid.getModels().size());
    }

    @Test
    public void testIsTableInAnyModel() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), projectDefualt);
        TableDesc table1 = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TableDesc table2 = tableMgr.getTableDesc("DEFAULT.TEST_ACCOUNT");
        Assert.assertTrue(mgrDefault.isTableInAnyModel(table1));
        Assert.assertFalse(mgrDefault.isTableInAnyModel(table2));
    }

    @Test
    public void testBasicModel() {
        DataModelDesc bm = mgrDefault.getDataModelDesc(modelBasic);
        Assert.assertEquals(3, bm.getJoinTables().length);
    }

    @Test
    public void testListDataModels() {
        Assert.assertEquals(2, mgrDefault.listAllDataModels().size());
    }

    @Test
    public void testGetDataModelDesc() {
        DataModelDesc dataModel = mgrDefault.getDataModelDesc(modelBasic);
        Assert.assertEquals(modelBasic, dataModel.getName());
        Assert.assertEquals(projectDefualt, dataModel.getProject());
    }

    @Test
    public void testGetModels() {
        List<DataModelDesc> models = mgrDefault.getModels();
        Assert.assertEquals(1, models.size());

        NDataModelManager mgrSsb;
        mgrSsb = NDataModelManager.getInstance(getTestConfig(), "ssb");
        List<DataModelDesc> models2 = mgrSsb.getModels();
        Assert.assertEquals(1, models2.size());
    }

    @Test
    public void testDropModel() throws IOException {
        DataModelDesc toDrop = mgrDefault.getDataModelDesc(modelBasic);
        DataModelDesc dropped = mgrDefault.dropModel(toDrop);
        Assert.assertEquals(toDrop, dropped);
    }

    @Test
    public void testCreateDataModelDesc() throws IOException {
        DataModelDesc model = mockModel();
        DataModelDesc result = mgrDefault.createDataModelDesc(model, ownerTest);

        Assert.assertEquals(projectDefualt, result.getProject());
        Assert.assertEquals(ownerTest, result.getOwner());
        Assert.assertEquals(result, mgrDefault.getDataModelDesc(modelTest));
    }

    @Test
    public void testUpdateDataModelDesc() throws IOException {
        DataModelDesc model = mockModel();
        mgrDefault.createDataModelDesc(model, ownerTest);

        String newVersion = "v8.8.8";
        model.setVersion(newVersion);
        DataModelDesc updated = mgrDefault.updateDataModelDesc(model);

        Assert.assertEquals(newVersion, updated.getVersion());
        Assert.assertEquals(mgrDefault.getDataModelDesc(modelTest), updated);
    }

    @After
    public void tearDownClass() {
        cleanAfterClass();
    }

    private DataModelDesc mockModel() {
        NDataModel model = new NDataModel();
        model.setName(modelTest);
        model.setUuid(UUID.randomUUID().toString());
        model.setOwner(ownerTest);
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");
        Measure measure = new Measure();
        measure.setName("test_measure");
        measure.setFunction(
                FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), "bigint"));
        model.setAllMeasures(Lists.newArrayList(measure));

        return model;
    }
}
