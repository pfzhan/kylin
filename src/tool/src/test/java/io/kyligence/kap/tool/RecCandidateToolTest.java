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

import java.io.File;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.jdbc.core.JdbcTemplate;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import lombok.val;

public class RecCandidateToolTest extends NLocalFileMetadataTestCase {

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
        Assert.assertTrue(file.listFiles().length >= 3);
        int count = 0;
        for (val project : file.listFiles()) {
            if (map.containsKey(project.getName())) {
                count++;
                Assert.assertEquals((int) map.get(project.getName()), project.listFiles().length);
            }
        }
        Assert.assertEquals(map.size(), count);
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
        Assert.assertEquals(1, projects.length);
        File project = projects[0];
        File[] models = project.listFiles();
        Assert.assertEquals(1, models.length);
        File model = models[0];
        String string = FileUtils.readFileToString(model);
        String[] lines = string.split("\n");
        Assert.assertEquals(3, lines.length);
        //restore
        jdbcRawRecStore.deleteAll();
        Assert.assertEquals(0, jdbcRawRecStore.queryAll().size());
        tool = new RecCandidateTool();
        tool.execute(new String[] { "-restore", "-dir", file.getAbsolutePath(), "-table", "test_opt_rec_candidate" });
        Assert.assertEquals(3, jdbcRawRecStore.queryAll().size());
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
        Assert.assertEquals(1, projects.length);
        File project = projects[0];
        File[] models = project.listFiles();
        Assert.assertEquals(2, models.length);
        //restore
        jdbcRawRecStore.deleteAll();
        Assert.assertEquals(0, jdbcRawRecStore.queryAll().size());
        tool = new RecCandidateTool();
        tool.execute(new String[] { "-restore", "-dir", file.getAbsolutePath(), "-table", "test_opt_rec_candidate" });
        Assert.assertEquals(3, jdbcRawRecStore.queryAll().size());
    }

    @Test
    @Ignore
    public void testInvalidParameter() {
        try {
            new RecCandidateTool().execute(new String[] { "-restore" });
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertTrue(e.getCause().getMessage().contains("table name shouldn't be empty."));
        }

        try {
            new RecCandidateTool().execute(new String[] { "-restore", "-table", "abc" });
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertTrue(e.getCause().getMessage().contains("The parameter -dir must be set when restore."));
        }

        try {
            new RecCandidateTool().execute(new String[] { "-restore", "-table", "abc", "-dir", "abc" });
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertTrue(e.getCause().getMessage().contains("Directory not exists"));
        }
    }

}
