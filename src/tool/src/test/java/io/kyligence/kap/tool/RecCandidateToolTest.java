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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.OptionsHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import lombok.val;

@RunWith(MockitoJUnitRunner.class)
public class RecCandidateToolTest extends NLocalFileMetadataTestCase {

    private static final Option OPERATE_BACKUP = OptionBuilder.getInstance()
            .withDescription("Backup rec candidate to local path").isRequired(false).create("backup");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(false).create("dir");

    private static final Option OPTION_MODEL_ID = OptionBuilder.getInstance().hasArg().withArgName("MODEL_ID")
            .withDescription("Specify model id for backup (optional)").isRequired(false).create("model");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project name for backup (optional)").isRequired(false).create("project");

    private static final Option OPTION_TABLE = OptionBuilder.getInstance().hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table for restore (optional)").isRequired(false).create("table");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private JdbcRawRecStore jdbcRawRecStore;
    private JdbcTemplate jdbcTemplate;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        jdbcRawRecStore = new JdbcRawRecStore(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void teardown() {
        if (jdbcTemplate != null) {
            jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        }
        cleanupTestMetadata();
    }

    private void prepare() {
        // prepare
        CCRecItemV2 ccRecItemV2 = new CCRecItemV2();
        RawRecItem recItem1 = new RawRecItem("gc_test", "6381db2-802f-4a25-98f0-bfe021c304ed", 1,
                RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem1.setState(RawRecItem.RawRecState.INITIAL);
        recItem1.setUniqueFlag("innerExp");
        recItem1.setRecEntity(ccRecItemV2);
        recItem1.setDependIDs(new int[] { 0 });
        RawRecItem recItem2 = new RawRecItem("gc_test", "6381db2-802f-4a25-98f0-bfe021c304ed", 1,
                RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem2.setState(RawRecItem.RawRecState.INITIAL);
        recItem2.setUniqueFlag("innerExp");
        recItem2.setRecEntity(ccRecItemV2);
        recItem2.setDependIDs(new int[] { 0 });
        RawRecItem recItem3 = new RawRecItem("gc_test", "6381db2-802f-4a25-98f0-bfe021c304ed", 1,
                RawRecItem.RawRecType.COMPUTED_COLUMN);
        recItem3.setState(RawRecItem.RawRecState.INITIAL);
        recItem3.setUniqueFlag("innerExp");
        recItem3.setRecEntity(ccRecItemV2);
        recItem3.setDependIDs(new int[] { 0 });
        recItem1.setProject("gc_test");
        recItem2.setProject("gc_test");
        recItem3.setProject("gc_test");
        jdbcRawRecStore.save(recItem1);
        jdbcRawRecStore.save(recItem2);
        jdbcRawRecStore.save(recItem3);
    }

    @Test
    public void testExtractFull() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("broken_test", 4);
        map.put("gc_test", 2);
        map.put("cc_test", 2);

        val junitFolder = temporaryFolder.getRoot();
        prepare();
        RecCandidateTool tool = new RecCandidateTool();
        tool.execute(new String[] { "-backup", "-dir", junitFolder.getAbsolutePath() });
        File file = new File(junitFolder.getAbsolutePath());
        file = file.listFiles()[0];
        assertTrue(file.listFiles().length >= 3);
        int count = 0;
        for (val project : file.listFiles()) {
            if (map.containsKey(project.getName())) {
                count++;
                assertEquals((int) map.get(project.getName()), project.listFiles().length);
            }
        }
        assertEquals(map.size(), count);
    }

    @Test
    @Ignore
    public void testExtractModelAndRestore() throws Exception {
        val junitFolder = temporaryFolder.getRoot();
        prepare();
        //backup
        RecCandidateTool tool = new RecCandidateTool();
        tool.execute(new String[] { "-backup", "-dir", junitFolder.getAbsolutePath(), "-model",
                "6381db2-802f-4a25-98f0-bfe021c304ed" });
        File file = new File(junitFolder.getAbsolutePath()).listFiles()[0];
        File[] projects = file.listFiles();
        assertEquals(1, projects.length);
        File project = projects[0];
        File[] models = project.listFiles();
        assertEquals(1, models.length);
        File model = models[0];
        String string = FileUtils.readFileToString(model);
        String[] lines = string.split("\n");
        assertEquals(3, lines.length);
        //restore
        jdbcRawRecStore.deleteAll();
        assertEquals(0, jdbcRawRecStore.queryAll().size());
        tool = new RecCandidateTool();
        tool.execute(new String[] { "-restore", "-dir", file.getAbsolutePath(), "-table", "test_opt_rec_candidate" });
        assertEquals(3, jdbcRawRecStore.queryAll().size());
    }

    @Test
    @Ignore
    public void testExtractProjectAndRestore() throws Exception {
        val junitFolder = temporaryFolder.getRoot();
        prepare();
        //backup
        RecCandidateTool tool = new RecCandidateTool();
        tool.execute(new String[] { "-backup", "-dir", junitFolder.getAbsolutePath(), "-project", "gc_test" });
        File file = new File(junitFolder.getAbsolutePath()).listFiles()[0];
        File[] projects = file.listFiles();
        assertEquals(1, projects.length);
        File project = projects[0];
        File[] models = project.listFiles();
        assertEquals(2, models.length);
        //restore
        jdbcRawRecStore.deleteAll();
        assertEquals(0, jdbcRawRecStore.queryAll().size());
        tool = new RecCandidateTool();
        tool.execute(new String[] { "-restore", "-dir", file.getAbsolutePath(), "-table", "test_opt_rec_candidate" });
        assertEquals(3, jdbcRawRecStore.queryAll().size());
    }

    @Test
    @Ignore
    public void testInvalidParameter() {
        try {
            new RecCandidateTool().execute(new String[] { "-restore" });
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof KylinException);
            assertTrue(e.getCause().getMessage().contains("table name shouldn't be empty."));
        }

        try {
            new RecCandidateTool().execute(new String[] { "-restore", "-table", "abc" });
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof KylinException);
            assertTrue(e.getCause().getMessage().contains("The parameter -dir must be set when restore."));
        }

        try {
            new RecCandidateTool().execute(new String[] { "-restore", "-table", "abc", "-dir", "abc" });
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof KylinException);
            assertTrue(e.getCause().getMessage().contains("Directory not exists"));
        }
    }

    @Test
    public void testRestore_throwsException() {
        RecCandidateTool recCandidateTool = new RecCandidateTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);

        // test throwing PARAMETER_EMPTY "table"
        when(optionsHelper.getOptionValue(OPTION_TABLE)).thenReturn(null);
        try {
            ReflectionTestUtils.invokeMethod(recCandidateTool, "restore", optionsHelper);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"table\" is empty.", e.toString());
        }

        // test throwing PARAMETER_NOT_SPECIFY "-dir"
        when(optionsHelper.getOptionValue(OPTION_TABLE)).thenReturn("TEST_TABLE");
        when(optionsHelper.getOptionValue(OPTION_DIR)).thenReturn(null);
        try {
            ReflectionTestUtils.invokeMethod(recCandidateTool, "restore", optionsHelper);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040202: \"-dir\" is not specified.", e.toString());
        }

        // test throwing PATH_NOT_EXISTS path
        when(optionsHelper.getOptionValue(OPTION_TABLE)).thenReturn("TEST_TABLE");
        Path path = mock(Path.class);
        File file = mock(File.class);
        try (MockedStatic<Paths> pathsMockedStatic = mockStatic(Paths.class)) {
            pathsMockedStatic.when(() -> Paths.get(anyString())).thenReturn(path);
            when(path.toFile()).thenReturn(file);
            when(file.exists()).thenReturn(false);
            when(optionsHelper.getOptionValue(OPTION_DIR)).thenReturn("/path/for/test");

            ReflectionTestUtils.invokeMethod(recCandidateTool, "restore", optionsHelper);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050041201: The path does not exist: /path/for/test.", e.toString());
        }
    }

    @Test
    public void testExecute_throwsException() {
        RecCandidateTool recCandidateTool = new RecCandidateTool();
        OptionsHelper optionsHelper = mock(OptionsHelper.class);

        // test throwing PARAMETER_NOT_SPECIFY "-backup"
        when(optionsHelper.hasOption(OPERATE_BACKUP)).thenReturn(false);
        try {
            ReflectionTestUtils.invokeMethod(recCandidateTool, "execute", optionsHelper);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040202: \"-backup\" is not specified.", e.toString());
        }
    }

    @Test
    public void testGetProjectByModelId_throwsException() {
        RecCandidateTool recCandidateTool = new RecCandidateTool();

        // test throwing 1st PARAMETER_EMPTY "model"
        String modelId = null;
        try {
            ReflectionTestUtils.invokeMethod(recCandidateTool, "getProjectByModelId", modelId);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"model\" is empty.", e.toString());
        }

        // test throwing 2nd PARAMETER_EMPTY "model"
        modelId = "TEST_MODEL_ID";
        try (MockedStatic<NProjectManager> nProjectManagerMockedStatic = mockStatic(NProjectManager.class)) {
            NProjectManager nProjectManager = mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(any())).thenReturn(nProjectManager);
            when(nProjectManager.listAllProjects()).thenReturn(Collections.emptyList());
            ReflectionTestUtils.invokeMethod(recCandidateTool, "getProjectByModelId", modelId);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-050040201: \"model\" is empty.", e.toString());
        }
    }

}
