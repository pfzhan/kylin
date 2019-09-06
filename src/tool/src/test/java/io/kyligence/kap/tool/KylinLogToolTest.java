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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.util.ToolUtil;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

public class KylinLogToolTest extends NLocalFileMetadataTestCase {

    private static final String logs1 = "2019-09-02 02:35:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n" +
            "2019-09-02 02:40:06,220 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_00 from RUNNING to SUCCEED\n" +
            "2019-09-02 02:40:06,619 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 1c2792bd-448e-4450-9944-32229523895d updates 1 metadata items\n" +
            "2019-09-02 02:40:06,635 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 1c2792bd-448e-4450-9944-32229523895d takes 422ms to complete";

    private static final String logs2 = "2019-09-02 02:41:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n" +
            "2019-09-02 02:41:40,178 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 started on project expert_01\n" +
            "2019-09-02 02:41:40,185 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_01 from RUNNING to SUCCEED\n" +
            "2019-09-02 02:41:40,561 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 updates 1 metadata items\n" +
            "2019-09-02 02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 takes 390ms to complete\n" +
            "2019-09-02 02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 started on project expert_01\n" +
            "2019-09-02 02:41:40,569 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8 from RUNNING to SUCCEED\n" +
            "2019-09-02 02:41:40,877 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 02e4d49e-80f8-4c58-aa44-6be1dab98e77 updates 1 metadata items\n" +
            "2019-09-02 02:41:40,885 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 takes 317ms to complete\n" +
            "2019-09-02 02:41:40,890 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n" +
            "2019-09-02 02:41:41,886 DEBUG [EventChecker(project:expert_01)-p-4-t-4] manager.EventOrchestrator : project expert_01 contains 1 events\n" +
            "2019-09-02 02:41:41,888 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e started on project expert_01\n" +
            "2019-09-02 02:41:41,889 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : transaction ca43de24-b825-4414-beeb-3b966651524e updates 1 metadata items\n" +
            "2019-09-02 02:41:41,889 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n" +
            "2019-09-02 02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e takes 13ms to complete\n" +
            "2019-09-02 02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork 297a3ec5-ca3b-4f19-a0a3-40079c3db372 started on project expert_01\n" +
            "2019-09-02 02:41:41,903 INFO  [EventChecker(project:expert_01)-p-4-t-4] handle.AbstractEventHandler : handling event: \n" +
            " {\n" +
            "  \"@class\" : \"io.kyligence.kap.event.model.PostAddCuboidEvent\",\n" +
            "  \"uuid\" : \"9d1cce98-29ab-4fd7-8025-11ac27c92b56\",\n" +
            "  \"last_modified\" : 1567392101889,\n" +
            "  \"create_time\" : 1567391988091,\n" +
            "  \"version\" : \"4.0.0.0\",\n" +
            "  \"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\",\n" +
            "  \"isGlobal\" : false,\n" +
            "  \"params\" : { },\n" +
            "  \"msg\" : null,\n" +
            "  \"sequence_id\" : 1,\n" +
            "  \"owner\" : \"ADMIN\",\n" +
            "  \"runTimes\" : 1,\n" +
            "  \"job_id\" : \"d0f45b72-db2f-407b-9d6f-7cfe6f6624e8\"\n" +
            "}";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExtractOtherLogs() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File accessLog = new File(ToolUtil.getLogFolder(), "access_log.1");
        File gcLog = new File(ToolUtil.getLogFolder(), "kylin.gc.1");
        File shellLog = new File(ToolUtil.getLogFolder(), "shell.1");

        FileUtils.writeStringToFile(accessLog, "111");
        FileUtils.writeStringToFile(gcLog, "111");
        FileUtils.writeStringToFile(shellLog, "111");

        KylinLogTool.extractOtherLogs(mainDir);

        FileUtils.deleteQuietly(accessLog);
        FileUtils.deleteQuietly(gcLog);
        FileUtils.deleteQuietly(shellLog);
        Assert.assertTrue(new File(mainDir, "logs/access_log.1").exists());
        Assert.assertTrue(new File(mainDir, "logs/kylin.gc.1").exists());
        Assert.assertTrue(new File(mainDir, "logs/shell.1").exists());
    }

    @Test
    public void testExtractKylinLogJob() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs1);
        FileUtils.writeStringToFile(kylinLog1, logs2);

        String jobId = "d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        KylinLogTool.extractKylinLog(mainDir, jobId);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertFalse(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log")).contains("2019-09-02 02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log")).contains("2019-09-02 02:40:06,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractKylinLogFull() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs1);
        FileUtils.writeStringToFile(kylinLog1, logs2);

        long startTime = DateTime.parse("2019-09-01").withTimeAtStartOfDay().getMillis();
        long endTime = DateTime.parse("2019-09-03").withTimeAtStartOfDay().getMillis();

        KylinLogTool.extractKylinLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log")).contains("2019-09-02 02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log")).contains("2019-09-02 02:40:06,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractSparkLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        String hdfsPath = ToolUtil.getSparkLogsDir(project);

        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;
        File sparkLogDir = new File(new File(normPath, DateTime.now().toString("yyyy-MM-dd")), jobId);
        FileUtils.forceMkdir(sparkLogDir);

        File tFile = new File(sparkLogDir, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractSparkLog(mainDir, project, jobId);

        FileUtils.deleteQuietly(new File(hdfsPath));
        Assert.assertTrue(new File(mainDir, "spark_logs/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testExtractJobTmp() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        String hdfsPath = ToolUtil.getJobTmpDir(project, jobId);
        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;

        FileUtils.forceMkdir(new File(normPath));
        File tFile = new File(normPath, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractJobTmp(mainDir, project, jobId);

        FileUtils.deleteQuietly(new File(hdfsPath));
        Assert.assertTrue(new File(mainDir, "job_tmp/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testExtractSparderLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String hdfsPath = ToolUtil.getSparderLogsDir();
        String normPath = hdfsPath.startsWith("file://") ? hdfsPath.substring(7) : hdfsPath;

        String[] childDirs = { "2019-08-29/application_1563861406192_0139", "2019-08-30/application_1563861406192_0139",
                "2019-08-31/application_1563861406192_0144" };
        for (String childDir : childDirs) {
            File sparderLogDir = new File(normPath, childDir);
            FileUtils.forceMkdir(sparderLogDir);

            File tFile = new File(sparderLogDir, "executor-1.log");
            FileUtils.writeStringToFile(tFile, "111");
        }

        long startTime = DateTime.parse("2019-08-29").withTimeAtStartOfDay().getMillis() + 3600_000L;
        long endTime = DateTime.parse("2019-08-30").withTimeAtStartOfDay().getMillis() + 3600_000L;

        KylinLogTool.extractSparderLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(new File(hdfsPath));

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir +"/executor-1.log").exists());
        }
    }
}
