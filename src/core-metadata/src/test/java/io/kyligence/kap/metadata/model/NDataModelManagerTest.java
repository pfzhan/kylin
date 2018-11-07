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

import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel.Measure;

public class NDataModelManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataModelManager mgrDefault;
    private String projectDefault = "default";
    private String modelBasic = "nmodel_basic";
    private String modelTest = "model_test";
    private String ownerTest = "owner_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        mgrDefault = NDataModelManager.getInstance(getTestConfig(), projectDefault);

    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetInstance() {
        NDataModelManager mgrSsb = NDataModelManager.getInstance(getTestConfig(), "ssb");
        Assert.assertNotEquals(mgrDefault, mgrSsb);

        NDataModelManager mgrInvalid = NDataModelManager.getInstance(getTestConfig(), "not_exist_prj");
        Assert.assertEquals(0, mgrInvalid.listModels().size());
    }

    @Test
    public void testIsTableInAnyModel() {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), projectDefault);
        TableDesc table1 = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TableDesc table2 = tableMgr.getTableDesc("DEFAULT.STREAMING_TABLE");
        Assert.assertTrue(mgrDefault.isTableInAnyModel(table1));
        Assert.assertFalse(mgrDefault.isTableInAnyModel(table2));
    }

    @Test
    public void testBasicModel() {
        NDataModel bm = mgrDefault.getDataModelDesc(modelBasic);
        Assert.assertEquals(9, bm.getJoinTables().size());
    }

    @Test
    public void testListDataModels() {
        Assert.assertEquals(6, mgrDefault.getDataModels().size());
    }

    @Test
    public void testGetDataModelDesc() {
        NDataModel dataModel = mgrDefault.getDataModelDesc(modelBasic);
        Assert.assertEquals(modelBasic, dataModel.getName());
        Assert.assertEquals(projectDefault, dataModel.getProject());
    }

    @Test
    public void testGetModels() {
        List<NDataModel> models = mgrDefault.listModels();
        Assert.assertEquals(6, models.size());

        NDataModelManager mgrSsb;
        mgrSsb = NDataModelManager.getInstance(getTestConfig(), "ssb");
        List<NDataModel> models2 = mgrSsb.listModels();
        Assert.assertEquals(1, models2.size());
    }

    @Test
    public void testDropModel() throws IOException {
        NDataModel toDrop = mgrDefault.getDataModelDesc(modelBasic);
        NDataModel dropped = mgrDefault.dropModel(toDrop);
        Assert.assertEquals(toDrop, dropped);
    }

    @Test
    public void testCreateDataModelDesc() throws IOException {
        NDataModel model = mockModel();
        NDataModel result = mgrDefault.createDataModelDesc(model, ownerTest);

        Assert.assertEquals(projectDefault, result.getProject());
        Assert.assertEquals(ownerTest, result.getOwner());
        Assert.assertEquals(result, mgrDefault.getDataModelDesc(modelTest));
    }

    @Test
    public void testUpdateDataModelDesc() throws IOException {
        NDataModel model = mockModel();
        mgrDefault.createDataModelDesc(model, ownerTest);

        String newVersion = "v8.8.8";
        model.setVersion(newVersion);
        NDataModel updated = mgrDefault.updateDataModelDesc(model);

        Assert.assertEquals(newVersion, updated.getVersion());
        Assert.assertEquals(mgrDefault.getDataModelDesc(modelTest), updated);
    }

    private NDataModel mockModel() {
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

    @Test
    public void createDataModelDesc_duplicateModelName_fail() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("DataModelDesc 'nmodel_basic' already exists");
        NDataModel nDataModel = JsonUtil.deepCopy((NDataModel) mgrDefault.getDataModelDesc("nmodel_basic"),
                NDataModel.class);

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void createDataModelDesc_simpleModel_succeed() throws IOException {
        int modelNum = mgrDefault.listModels().size();
        NDataModel nDataModel = JsonUtil.deepCopy((NDataModel) mgrDefault.getDataModelDesc("nmodel_basic"),
                NDataModel.class);

        nDataModel.setName("nmodel_basic2");
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setLastModified(0L);
        mgrDefault.createDataModelDesc(nDataModel, "root");

        Assert.assertEquals(modelNum + 1, mgrDefault.listModels().size());
    }

    @Test
    public void createDataModelDesc_duplicateNamedColumn_fail() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Multiple entries with same value");

        NDataModel nDataModel = JsonUtil.deepCopy((NDataModel) mgrDefault.getDataModelDesc("nmodel_basic"),
                NDataModel.class);

        nDataModel.setName("nmodel_basic2");
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setLastModified(0L);

        //add a duplicate
        List<NDataModel.NamedColumn> allNamedColumns = nDataModel.getAllNamedColumns();
        NDataModel.NamedColumn e = JsonUtil.deepCopy(allNamedColumns.get(0), NDataModel.NamedColumn.class);
        e.setId(allNamedColumns.size());
        allNamedColumns.add(e);

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void createDataModelDesc_duplicateNameColumnName_succeed() throws IOException {

        NDataModel nDataModel = JsonUtil.deepCopy((NDataModel) mgrDefault.getDataModelDesc("nmodel_basic"),
                NDataModel.class);

        nDataModel.setName("nmodel_basic2");
        nDataModel.setUuid(UUID.randomUUID().toString());
        nDataModel.setLastModified(0L);

        //make conflict on NamedColumn.name
        List<NDataModel.NamedColumn> allNamedColumns = nDataModel.getAllNamedColumns();
        allNamedColumns.get(1).setName(allNamedColumns.get(0).getName());

        mgrDefault.createDataModelDesc(nDataModel, "root");
    }

    @Test
    public void saveDataModelDesc_MultipleDataLoadingRange_exception() throws IOException {

        NDataModel nDataModel = mgrDefault.copyForWrite(mgrDefault.getDataModelDesc("nmodel_basic"));
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val table = tableManager.getTableDesc("DEFAULT.TEST_ACCOUNT");
        table.setFact(true);
        tableManager.updateTableDesc(table);
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Only one incrementing loading table can be setted in model!");
        mgrDefault.updateDataModelDesc(nDataModel);
    }
}
