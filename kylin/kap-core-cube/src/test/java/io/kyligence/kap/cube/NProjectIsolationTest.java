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

import java.io.IOException;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NProjectIsolationTest extends NLocalFileMetadataTestCase {
    private final static String PRJ_SEP1 = "prj_sep1";
    private final static String PRJ_SEP2 = "prj_sep2";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata("src/test/resources/isolation_metadata");
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testDataModel() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataModelManager dm = NDataModelManager.getInstance(testConfig, PRJ_SEP1);
        NDataModel model = dm.getDataModelDesc("nmodel_basic");
        model.setDescription("new_description");

        // update
        dm.updateDataModelDesc(model);
        validate();

        // delete
        dm.dropModel(model);
        validate();

        // create
        NDataModel model2 = new NDataModel();
        model2.setName("nmodel_basic");
        model2.setUuid(UUID.randomUUID().toString());
        model2.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");
        NDataModel.Measure measure = new NDataModel.Measure();
        measure.setName("test_measure");
        measure.setFunction(
                FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), "bigint"));
        model2.setAllMeasures(Lists.newArrayList(measure));
        dm.createDataModelDesc(model2, "admin");
        validate();
    }

    @Test
    public void testDataflow() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager dfm = NDataflowManager.getInstance(testConfig, PRJ_SEP1);
        NDataflow df = dfm.getDataflow("ncube_basic");
        NCubePlan cubePlan = df.getCubePlan();
        Assert.assertEquals("ncube_basic", df.getName());
        validate();

        // update
        NDataflowUpdate update = new NDataflowUpdate("ncube_basic");
        update.setDescription("new_description");
        df = dfm.updateDataflow(update);
        Assert.assertEquals("ncube_basic", df.getName());
        validate();

        // remove
        dfm.dropDataflow("ncube_basic");
        Assert.assertEquals(0, dfm.listAllDataflows().size());
        validate();

        // create
        dfm.createDataflow("ncube_basic", PRJ_SEP1, cubePlan, "admin");
        validate();
    }

    @Test
    public void testDataflowDetail() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager dfm = NDataflowManager.getInstance(testConfig, PRJ_SEP1);
        NDataflow df = dfm.getDataflow("ncube_basic");
        validate();

        // test cuboid remove
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveCuboids(df.getSegment(0).getCuboid(1001L));
        dfm.updateDataflow(update);
        validate();

        // test cuboid add
        NDataSegment seg = df.getSegment(0);
        update = new NDataflowUpdate(df.getName());
        update.setToAddOrUpdateCuboids(//
                NDataCuboid.newDataCuboid(df, seg.getId(), 1001L), // to add
                NDataCuboid.newDataCuboid(df, seg.getId(), 1002L) // existing, will update with warning
        );
        dfm.updateDataflow(update);
        validate();
    }

    @Test
    public void testCubePlan() throws IOException {
        KylinConfig testConfig = getTestConfig();

        NCubePlanManager cpm = NCubePlanManager.getInstance(testConfig, PRJ_SEP1);
        NCubePlan cube = cpm.getCubePlan("ncube_basic");

        // update
        cube = cpm.updateCubePlan(cube.getName(), new NCubePlanManager.NCubePlanUpdater() {
            @Override
            public void modify(NCubePlan copyForWrite) {
                copyForWrite.setDescription("new_description");
            }
        });
        Assert.assertEquals("new_description", cube.getDescription());
        validate();

        // delete
        cpm.removeCubePlan(cube);
        Assert.assertEquals(0, cpm.listAllCubePlans().size());
        validate();

        // create
        NCubePlan cube2 = new NCubePlan();
        cube2.setName("ncube_basic");
        cube2.setModelName("nmodel_basic");
        cube2.setUuid(UUID.randomUUID().toString());
        cube2.setDescription("test_description");
        cube2.setProject(PRJ_SEP1);
        cpm.createCubePlan(cube2);
        Assert.assertEquals(1, cpm.listAllCubePlans().size());
    }

    @Test
    public void testTable() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NTableMetadataManager tm = NTableMetadataManager.getInstance(testConfig, PRJ_SEP1);
        TableDesc table = tm.getTableDesc("DEFAULT.TEST_COUNTRY");
        tm.removeSourceTable("DEFAULT.TEST_COUNTRY", PRJ_SEP1);
        validate();

        table.setLastModified(0L);
        tm.saveSourceTable(table, PRJ_SEP1);
        validate();
    }

    private void validate() {
        KylinConfig testConfig = getTestConfig();
        NDataModelManager dm = NDataModelManager.getInstance(testConfig, PRJ_SEP2);
        NDataModel model = dm.getDataModelDesc("nmodel_basic");
        Assert.assertNotNull(model);
        Assert.assertNull(model.getDescription());

        NDataflowManager dfm = NDataflowManager.getInstance(testConfig, PRJ_SEP2);
        NDataflow df = dfm.getDataflow("ncube_basic");
        Assert.assertEquals(1, dfm.listAllDataflows().size());
        Assert.assertEquals("ncube_basic", df.getName());
        Assert.assertEquals("test_description", df.getDescription());
        Assert.assertEquals(4, df.getSegment(0).getCuboidsMap().size());

        NCubePlanManager cpm = NCubePlanManager.getInstance(testConfig, PRJ_SEP2);
        NCubePlan cube = cpm.getCubePlan("ncube_basic");
        Assert.assertEquals(1, cpm.listAllCubePlans().size());
        Assert.assertEquals("ncube_basic", cube.getName());
        Assert.assertEquals("test_description", cube.getDescription());

        NTableMetadataManager tm = NTableMetadataManager.getInstance(testConfig, PRJ_SEP2);
        Assert.assertEquals(9, tm.listAllTables(PRJ_SEP2).size());
    }
}
