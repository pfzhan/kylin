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

package io.kyligence.kap.tool;

import static io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore.datasourceParameters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import io.kyligence.kap.common.persistence.ImageDesc;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class MetadataToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBackupProject() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool();
        tool.execute(new String[] { "-backup", "-project", "default", "-dir", junitFolder.getAbsolutePath(), "-folder",
                "prj_bak" });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        Assertions.assertThat(archiveFolder.list()).isNotEmpty().containsOnly("default", "UUID", "_global");

        val projectFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("default"));
        assertProjectFolder(projectFolder, archiveFolder);
    }

    private boolean assertProjectFolder(File projectFolder, File archiveFolder) {
        Assertions.assertThat(projectFolder.list()).containsAnyOf("dataflow", "dataflow_details", "cube_plan",
                "model_desc", "table");
        Assertions.assertThat(projectFolder.listFiles()).filteredOn(f -> !f.getName().startsWith("."))
                .allMatch(f -> f.listFiles().length > 0);

        val projectName = projectFolder.toPath().getFileName().toString();
        val globalFolder = findFile(archiveFolder.listFiles(), f -> f.getName().equals("_global"));
        val projects = findFile(globalFolder.listFiles(), f -> f.getName().equals("project"));
        Assertions.assertThat(findFile(projects.listFiles(), f -> f.getName().startsWith(projectName))).exists()
                .isFile();
        return true;
    }

    @Test
    public void testBackupAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        val tool = new MetadataTool();
        tool.execute(new String[] { "-backup", "-dir", junitFolder.getAbsolutePath() });

        Assertions.assertThat(junitFolder.listFiles()).hasSize(1);
        val archiveFolder = junitFolder.listFiles()[0];
        Assertions.assertThat(archiveFolder).exists();

        Assertions.assertThat(archiveFolder.list()).isNotEmpty().containsOnlyOnce("UUID").containsAnyOf("default",
                "ssb", "tdvt");
        Assertions.assertThat(archiveFolder.listFiles())
                .filteredOn(f -> !f.getName().equals("UUID") && !f.getName().startsWith("_"))
                .allMatch(projectFolder -> assertProjectFolder(projectFolder, archiveFolder));

    }

    @Test
    public void testRestoreAll() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

        //there is a project that destResourceStore contains and srcResourceStore doesn't contain
        FileUtils.forceDelete(Paths.get(junitFolder.getAbsolutePath(), "/demo").toFile());
        FileUtils.deleteQuietly(Paths.get(junitFolder.getAbsolutePath(), "_global", "project", "demo.json").toFile());

        //there is a project that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();
        assertBeforeRestoreTest();
        val tool = new MetadataTool();
        tool.execute(new String[] { "-restore", "-dir", junitFolder.getAbsolutePath() });
        assertAfterRestoreTest();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    @Test
    public void testRestoreAllWithSrcOrDestIsEmpty() throws IOException {
        val emptyFolder = temporaryFolder.newFolder();
        val restoreFolder = temporaryFolder.newFolder();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), restoreFolder, "/");

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
        val tool = new MetadataTool();
        tool.execute(new String[] { "-restore", "-dir", emptyFolder.getAbsolutePath() });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).listAllProjects()).isEmpty();

        tool.execute(new String[] { "-restore", "-dir", restoreFolder.getAbsolutePath() });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("default")).isNotNull();
    }

    @Test
    public void testRestoreProject() throws Exception {
        val resourceStore = getStore();
        val jdbcTemplate = getJdbcTemplate();
        resourceStore.getMetadataStore().setAuditLogStore(new JdbcAuditLogStore(getTestConfig(), jdbcTemplate,
                new DataSourceTransactionManager(jdbcTemplate.getDataSource()), "test_audit_log"));
        val junitFolder = temporaryFolder.getRoot();
        MetadataToolTestFixture.fixtureRestoreTest(getTestConfig(), junitFolder, "/");

        assertBeforeRestoreTest();
        val tool = new MetadataTool();
        tool.execute(new String[] { "-restore", "-project", "default", "-dir", junitFolder.getAbsolutePath() });
        assertAfterRestoreTest();

        val path = HadoopUtil.getBackupFolder(getTestConfig());
        val fs = HadoopUtil.getFileSystem(path);
        val rootPath = Stream.of(fs.listStatus(new Path(path)))
                .max(Comparator.comparing(FileStatus::getModificationTime)).map(FileStatus::getPath)
                .orElse(new Path(path + "/backup_0/"));
        try (val in = fs.open(new Path(rootPath, "_image"))) {
            val image = JsonUtil.readValue(IOUtils.toByteArray(in), ImageDesc.class);
            Assert.assertEquals(resourceStore.listResourcesRecursively("/default").size() + 2,
                    image.getOffset().longValue());
        }
    }

    @Test
    public void testRestoreProjectWithSrcOrDestIsEmpty() throws IOException {
        val junitFolder = temporaryFolder.getRoot();
        ResourceTool.copy(getTestConfig(), KylinConfig.createInstanceFromUri(junitFolder.getAbsolutePath()), "/");
        val tool = new MetadataTool();

        //there is a project metadata that destResourceStore contains and srcResourceStore doesn't contain
        FileUtils.forceDelete(Paths.get(junitFolder.getAbsolutePath(), "demo").toFile());
        FileUtils.deleteQuietly(Paths.get(junitFolder.getAbsolutePath(), "_global", "project", "demo.json").toFile());

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNotNull();
        tool.execute(new String[] { "-restore", "-project", "demo", "-dir", junitFolder.getAbsolutePath() });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("demo")).isNull();

        //there is a project metadata that destResourceStore doesn't contain and srcResourceStore contains
        val destResourceStore = ResourceStore.getKylinMetaStore(getTestConfig());
        val destResources = destResourceStore.getMetadataStore().list("/ssb");
        for (String res : destResources) {
            destResourceStore.deleteResource("/ssb" + res);
        }
        destResourceStore.deleteResource("/_global/project/ssb.json");

        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNull();
        tool.execute(new String[] { "-restore", "-project", "ssb", "-dir", junitFolder.getAbsolutePath() });
        Assertions.assertThat(NProjectManager.getInstance(getTestConfig()).getProject("ssb")).isNotNull();
    }

    private void assertBeforeRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(getTestConfig(), "default");

        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        Assertions.assertThat(dataModel1).isNotNull().hasFieldOrPropertyWithValue("owner", "who")
                .hasFieldOrPropertyWithValue("mvcc", 1L);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        Assertions.assertThat(dataModel2).isNull();

        val dataModel3 = dataModelMgr.getDataModelDescByAlias("data_model_3");
        Assertions.assertThat(dataModel3).isNotNull().hasFieldOrPropertyWithValue("owner", "who")
                .hasFieldOrPropertyWithValue("mvcc", 0L);
    }

    private void assertAfterRestoreTest() {
        val dataModelMgr = NDataModelManager.getInstance(getTestConfig(), "default");

        val dataModel1 = dataModelMgr.getDataModelDescByAlias("nmodel_basic");
        Assertions.assertThat(dataModel1).isNotNull().hasFieldOrPropertyWithValue("owner", "ADMIN")
                .hasFieldOrPropertyWithValue("mvcc", 2L);

        val dataModel2 = dataModelMgr.getDataModelDescByAlias("nmodel_basic_inner");
        Assertions.assertThat(dataModel2).isNotNull().hasFieldOrPropertyWithValue("mvcc", 0L);

        val dataModel3 = dataModelMgr.getDataModelDescByAlias("data_model_3");
        Assertions.assertThat(dataModel3).isNull();
    }

    private File findFile(File[] files, Predicate<File> predicate) {
        Assertions.assertThat(files).anyMatch(predicate);
        return Arrays.stream(files).filter(predicate).findFirst().get();
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = StorageURL.valueOf(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }
}
