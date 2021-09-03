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

package io.kyligence.kap.query.engine.view;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.engine.ProjectSchemaFactory;
import io.kyligence.kap.query.engine.QueryExec;
import lombok.val;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.LinkedHashMap;

public class ModelViewGeneratorTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        overwriteSystemProp("kylin.query.auto-model-view-enabled", "TRUE");
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private void createProject(String project) {
        val projMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (projMgr.getProject(project) == null) {
            NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject(
                    project, "ADMIN", "", new LinkedHashMap<>(), MaintainModelType.MANUAL_MAINTAIN
            );
        }
        // copy tables from project default
        val ssbMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableDesc tableDesc : ssbMgr.listAllTables()) {
            if (tableDesc.getDatabase().equalsIgnoreCase("SSB")) {
                if (tableMgr.getTableDesc(tableDesc.getName()) == null) {
                    val clone = new TableDesc(tableDesc);
                    clone.setMvcc(-1);
                    tableMgr.saveSourceTable(clone);
                }
            }
        }
    }

    private NDataModel createSSBModel(String project, String modelAlias) throws IOException {
        val mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val serializer = mgr.getDataModelSerializer();
        val contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/view/" + modelAlias + ".json").toPath(),
                Charset.defaultCharset()), "\n");
        val bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        val deserialized = serializer.deserialize(new DataInputStream(bais));
        val model = mgr.createDataModelDesc(deserialized, "ADMIN");

        val emptyIndex = new IndexPlan();
        emptyIndex.setUuid(model.getUuid());
        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .createIndexPlan(emptyIndex);

        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .createDataflow(emptyIndex, model.getOwner());
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);

        return model;
    }

    private void assertView(String project, String modelAlias) throws IOException {
        val model = createSSBModel(project, modelAlias);

        // assert generated sql
        val expectedSQL = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/view/" + modelAlias + ".sql").toPath(),
                Charset.defaultCharset()), "")
                .replace("  ", " "); // remove extra spaces
        val generated = new ModelViewGenerator(model).generateViewSQL()
                .replace("  ", " "); // remove extra spaces
        Assert.assertEquals(
                String.format("%s view sql generated unexpected sql", modelAlias),
                expectedSQL.trim(), generated);

        // assert schema
        val rootSchema =
                new ProjectSchemaFactory(project, KylinConfig.getInstanceFromEnv()).createProjectRootSchema();
        Assert.assertNotNull(
                String.format("%s view sql generated unexpected schema", modelAlias),
                rootSchema.getSubSchema(project, false).getTableBasedOnNullaryFunction(modelAlias, false).getTable());

        // check view parsing
        try {
            new QueryExec(project, KylinConfig.getInstanceFromEnv())
                    .parseAndOptimize(String.format("select * from %s.%s", project, modelAlias));
        } catch (SqlParseException e) {
            Assert.fail(String.format("%s failed sql parsing %s", modelAlias, e));
        }
    }

    @Test
    public void testConfig() throws IOException {
        overwriteSystemProp("kylin.query.auto-model-view-enabled", "FALSE");
        val projectName = "SSB_TEST";
        createProject(projectName);
        createSSBModel(projectName, "model_single_table");
        val schemaBefore =
                new ProjectSchemaFactory(projectName, KylinConfig.getInstanceFromEnv())
                        .createProjectRootSchema().getSubSchema(projectName, false);
        Assert.assertNull(schemaBefore);

        overwriteSystemProp("kylin.query.auto-model-view-enabled", "TRUE");
        val schemaAfter =
                new ProjectSchemaFactory(projectName, KylinConfig.getInstanceFromEnv())
                        .createProjectRootSchema().getSubSchema(projectName, false);
        Assert.assertNotNull(schemaAfter);
    }


    @Test
    public void testModelViews() throws IOException {
        val views = Lists.newArrayList(
                "model_single_table",
                "model_joins",
                "model_cc"
        );
        val projectName = "SSB_TEST";
        createProject(projectName);
        for (String view : views) {
            assertView(projectName, view);
        }
    }

    @Test
    public void testDBNameCollision() throws IOException {
        // same db, different table name
        val views = Lists.newArrayList(
                "model_single_table",
                "model_joins",
                "model_cc"
        );
        val projectName = "SSB";
        createProject(projectName);
        for (String view : views) {
            assertView(projectName, view);
        }

        // same db, same name
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        val model = modelMgr.getDataModelDescByAlias("model_single_table");
        modelMgr.updateDataModel(model.getId(), (m) -> m.setAlias("LINEORDER"));
        val rootSchema =
                new ProjectSchemaFactory(projectName, KylinConfig.getInstanceFromEnv())
                        .createProjectRootSchema().getSubSchema(projectName, false);
        // assert model view disappears
        Assert.assertNull(rootSchema.getTable("model_single_table", false));
        // assert lineorder is not the model view table
        Assert.assertNotEquals(ViewTable.class, rootSchema.getTable("LINEORDER", false).getTable().getClass());
    }
}