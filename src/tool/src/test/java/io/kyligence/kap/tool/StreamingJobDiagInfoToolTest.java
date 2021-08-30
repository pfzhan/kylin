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
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import io.kyligence.kap.tool.util.ZipFileUtil;
import lombok.val;

public class StreamingJobDiagInfoToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String PROJECT = "streaming_test";
    private static final String JOB_ID = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
    private static final String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private static final long DAY = 24 * 3600 * 1000L;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        copyConf();
        createStreamingExecutorLog(PROJECT, JOB_ID);
        createStreamingDriverLog(PROJECT, JOB_ID);
        createCheckpoint(MODEL_ID);
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public void createStreamingDriverLog(String project, String jobId) throws IOException {

        File file = temporaryFolder.newFile("driver." + 1628560620000L + ".log");
        File file2 = temporaryFolder.newFile("driver." + 1628477820000L + ".log");

        String jobLogDir = KylinConfig.getInstanceFromEnv().getStreamingJobTmpOutputStorePath(project, jobId);

        Path jobLogDirPath = new Path(jobLogDir);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(jobLogDirPath);
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), jobLogDirPath);
        fs.copyFromLocalFile(new Path(file2.getAbsolutePath()), jobLogDirPath);
    }

    public void createCheckpoint(String modelId) throws IOException {

        File file = temporaryFolder.newFile("medadata");

        String hdfsStreamCheckpointPath = String.format(Locale.ROOT, "%s%s%s",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectoryWithoutScheme(), "streaming/checkpoint/",
                modelId);

        Path checkPointModelDirPath = new Path(hdfsStreamCheckpointPath);
        Path commitsPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "commits"));
        Path offsetsPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "offsets"));
        Path sourcesPath = new Path(String.format(Locale.ROOT, "%s/%s", hdfsStreamCheckpointPath, "sources"));

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(checkPointModelDirPath);
        fs.mkdirs(commitsPath);
        fs.mkdirs(offsetsPath);
        fs.mkdirs(sourcesPath);

        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), checkPointModelDirPath);
    }

    public void createStreamingExecutorLog(String project, String jobId) throws IOException {

        File file = temporaryFolder.newFile("executor.1.log");

        String hdfsStreamLogProjectPath = String.format(Locale.ROOT, "%s%s%s/%s/%s",
                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectoryWithoutScheme(), "streaming/spark_logs/",
                project, new DateTime(System.currentTimeMillis()).toString("yyyy-MM-dd"), jobId);

        Path jobLogDirPath = new Path(hdfsStreamLogProjectPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.mkdirs(jobLogDirPath);
        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), jobLogDirPath);
    }

    public void copyConf() throws IOException {
        StorageURL metadataUrl = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(metadataUrl.getIdentifier());
        File confDir = new File(path.getParentFile(), "conf");
        FileUtils.forceMkdir(confDir);

        File confFile = new File(path.getParentFile(), "kylin.properties");
        Files.copy(confFile.toPath(), new File(confDir, "kylin.properties").toPath());

    }

    @Test
    public void testGetJobByJobId() {
        StreamingJobDiagInfoTool streamingJobDiagInfoTool = new StreamingJobDiagInfoTool();
        val job = streamingJobDiagInfoTool.getJobById(JOB_ID);
        Assert.assertEquals(PROJECT, job.getProject());
        StreamingJobMeta job2 = streamingJobDiagInfoTool.getJobById("");
        Assert.assertNull(job2);
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        new StreamingJobDiagInfoTool().execute(new String[] { "-streamingJob",
                "e78a89dd-847f-4574-8afa-8768b4228b72_build", "-destDir", mainDir.getAbsolutePath() });
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
                zipFile.stream().map(ZipEntry::getName).filter(file -> (file.contains("streaming_spark_logs/spark_") && !file.endsWith(".crc")))
                        .forEach(appFiles::add);
                Assert.assertEquals(6, appFiles.size());
            }
        }

        thrown.expect(KylinException.class);
        thrown.expectMessage("error parsing args");
        new StreamingJobDiagInfoTool().execute(new String[] { "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testExecuteAll() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File mainDir2 = new File(temporaryFolder.getRoot(), testName.getMethodName().concat("2"));
        FileUtils.forceMkdir(mainDir2);

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        DiagClientTool diagClientTool = new DiagClientTool();

        // > 31 day
        diagClientTool.execute(new String[] { "-destDir", mainDir2.getAbsolutePath(), "-startTime", "1604999712000",
                "-endTime", String.valueOf(System.currentTimeMillis()) });

        // normal
        Long endTime = new DateTime(System.currentTimeMillis()).getMillis();
        Long startTime = endTime - (20 * DAY);
        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-startTime",
                String.valueOf(startTime), "-endTime", String.valueOf(endTime) });

        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("full") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
                HashSet<String> appFiles = new HashSet<>();
                val zipFile = new ZipFile(file2);
                zipFile.stream().map(ZipEntry::getName).filter(file -> (file.contains("streaming_spark_logs/spark_") && !file.endsWith(".crc")))
                        .forEach(appFiles::add);
                Assert.assertEquals(6, appFiles.size());
            }
        }
    }

    @Test
    public void testExecuteWithFalseIncludeMeta() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        // includeMeta false
        new StreamingJobDiagInfoTool().execute(new String[] { "-streamingJob", JOB_ID, "-destDir",
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

        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID, "-destDir", mainDir.getAbsolutePath() });

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
        new StreamingJobDiagInfoTool().execute(new String[] { "-streamingJob", "9462fee8-e6cd-4d18-a5fc-b598a3c5edb5",
                "-destDir", mainDir.getAbsolutePath() });

    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        new StreamingJobDiagInfoTool()
                .execute(new String[] { "-streamingJob", JOB_ID, "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtil.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = io.kyligence.kap.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

    }

}
