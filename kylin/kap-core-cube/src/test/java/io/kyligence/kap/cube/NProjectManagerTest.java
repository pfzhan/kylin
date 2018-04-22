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

package io.kyligence.kap.cube;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class NProjectManagerTest extends NLocalFileMetadataTestCase {

    private String defaultPrj = "default";
    private String modelBasic = "nmodel_basic";
    private String modelTest = "model_test";
    private String cubeBasic = "ncube_basic";
    private String cubeTest = "ncube_basic_copy";
    private String ownerTest = "owner_test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testProject() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        ProjectInstance projectInstance = projectManager.getProject(defaultPrj);
        Assert.assertTrue(projectManager.listAllProjects().contains(projectInstance));

        Assert.assertNotNull(projectManager.getPrjByUuid("1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b"));
        Assert.assertTrue(projectInstance.getOverrideKylinProps().keySet().contains("kylin.storage.hbase.owner-tag"));
    }

    @Test
    public void testModel() throws IOException {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), defaultPrj);
        NDataModel modelDesc = dataModelManager.getDataModelDesc(modelBasic);
        Preconditions.checkNotNull(modelDesc);
        Assert.assertTrue(projectManager.getProject(defaultPrj).getModels().contains(modelDesc.getName()));

        NDataModel newDataModel = mockModel();
        dataModelManager.createDataModelDesc(newDataModel, ownerTest);

        dataModelManager.dropModel(newDataModel);
        List<String> list = projectManager.getProject(defaultPrj).getModels();
        Assert.assertFalse(list.contains(newDataModel.getName()));

    }

    private NDataModel mockModel() {
        NDataModel model = new NDataModel();
        model.setName(modelTest);
        model.setUuid(UUID.randomUUID().toString());
        model.setOwner(ownerTest);
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName("test_measure");
        measure.setFunction(
                FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), "bigint"));
        model.setAllMeasures(Lists.newArrayList(measure));

        return model;
    }

    @Test
    public void testDataflow() throws IOException {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), defaultPrj);
        NDataflow dataflow = dataflowManager.getDataflow(cubeBasic);
        Assert.assertTrue(dataflow != null);

        Preconditions.checkArgument(dataflowManager.getDataflow(cubeTest) == null);
        NCubePlanManager planManager = NCubePlanManager.getInstance(getTestConfig(), defaultPrj);
        NDataflow newData = dataflowManager.createDataflow(cubeTest, defaultPrj, planManager.getCubePlan(cubeBasic),
                ownerTest);
        Preconditions.checkNotNull(newData);

        List<RealizationEntry> realizationEntries = projectManager.getProject(defaultPrj).getRealizationEntries();
        List<String> realizationNames = new ArrayList<>();
        for (RealizationEntry entry : realizationEntries) {
            realizationNames.add(entry.getRealization());
        }
        Assert.assertTrue(realizationNames.contains(newData.getName()));

        dataflowManager.dropDataflow(newData.getName());
        List<RealizationEntry> dataflowList = projectManager.getProject(defaultPrj).getRealizationEntries();
        List<String> dataflows = new ArrayList<>();
        for (RealizationEntry entry : dataflowList) {
            dataflows.add(entry.getRealization());
        }
        Assert.assertFalse(dataflows.contains(newData.getName()));
    }

    @Test
    public void testTable() throws IOException, InterruptedException {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        String tableIdentity = "DEFAULT.TEST_ORDER";

        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), defaultPrj);
        TableDesc tableDesc = tableManager.getTableDesc(tableIdentity);
        Assert.assertTrue(tableDesc != null);

        Preconditions.checkNotNull(tableManager.getTableDesc(tableIdentity));

        tableManager.removeSourceTable(tableIdentity, defaultPrj);
        Thread.sleep(1000);
        Assert.assertFalse(projectManager.getProject(defaultPrj).getTables().contains(tableIdentity));

        tableDesc.setLastModified(0L);
        tableManager.saveSourceTable(tableDesc, defaultPrj);
        Assert.assertTrue(projectManager.getProject(defaultPrj).getTables().contains(tableIdentity));

    }

}
