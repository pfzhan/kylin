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
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.ZipFileUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import io.kyligence.kap.tool.obf.KylinConfObfuscatorTest;
import io.kyligence.kap.tool.util.JobMetadataWriter;
import lombok.val;

public class JobDiagInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinConfObfuscatorTest.prepare();
    }

    public void createTestMetadata(String... overlay) {
        super.createTestMetadata(overlay);
        JobMetadataWriter.writeJobMetaData(getTestConfig());
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetJobByJobId() {
        val job = new JobDiagInfoTool().getJobByJobId("dd5a6451-0743-4b32-b84d-2ddc8052429f");
        Assert.assertEquals("newten", job.getProject());
        Assert.assertEquals(1574130051721L, job.getCreateTime());
        Assert.assertEquals(1574818631319L, job.getEndTime());
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("job") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
                HashSet<String> appFiles = new HashSet<>();
                val zipFile = new ZipFile(file2);
                zipFile.stream().map(ZipEntry::getName).filter(file -> (file.contains("job_history/application_")))
                        .forEach(appFiles::add);
                Assert.assertEquals(3, appFiles.size());
            }
        }

    }

    @Test
    public void testExecuteWithFalseIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // includeMeta false
        new JobDiagInfoTool().execute(new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir",
                mainDir.getAbsolutePath(), "-includeMeta", "false" });

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                        .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));

        Assert.assertFalse(hasMetadataFile);
    }

    @Test
    public void testExecuteWithDefaultIncludeMeta() throws IOException {
        // default includeMeta(true)
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });

        boolean hasMetadataFile = new ZipFile(
                Objects.requireNonNull(Objects.requireNonNull(mainDir.listFiles())[0].listFiles())[0]).stream()
                        .anyMatch(zipEntry -> zipEntry.getName().contains("metadata"));
        Assert.assertTrue(hasMetadataFile);
    }

    @Test
    public void testWithNotExistsJobId() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        thrown.expect(new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object o) {
                if (!(o instanceof Exception)) {
                    return false;
                }

                Throwable e = ((Exception) o).getCause();

                if (!e.getClass().equals(RuntimeException.class)) {
                    return false;
                }

                if (!e.getMessage().equals("Can not find the jobId: 9462fee8-e6cd-4d18-a5fc-b598a3c5edb5")) {
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        new JobDiagInfoTool().execute(
                new String[] { "-job", "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5", "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new JobDiagInfoTool().execute(
                new String[] { "-job", "dd5a6451-0743-4b32-b84d-2ddc8052429f", "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtils.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = io.kyligence.kap.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

    }

}
