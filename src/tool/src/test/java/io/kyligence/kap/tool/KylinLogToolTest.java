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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TimeZone;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.SetAndUnsetSystemProp;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.util.ToolUtil;

public class KylinLogToolTest extends NLocalFileMetadataTestCase {

    private static final String logs1 = "2019-09-02 02:35:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n"
            + "2019-09-02 02:38:16,868 INFO  [FetchJobWorker(project:a_test)-p-9-t-9] runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8 tttt\n"
            + "2019-09-02 02:39:17,868 INFO  test1\n" + "2019-09-02 02:39:18,868 INFO  test2\n"
            + "2019-09-02 02:40:06,220 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_00 from RUNNING to SUCCEED\n"
            + "2019-09-02 02:40:06,619 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 1c2792bd-448e-4450-9944-32229523895d updates 1 metadata items\n"
            + "2019-09-02 02:40:07,635 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 1c2792bd-448e-4450-9944-32229523895d takes 422ms to complete";

    private static final String logs2 = "2019-09-02 02:41:19,868 INFO [dag-scheduler-event-loop] scheduler.DAGScheduler : Got job 459 (countAsync at SparkContextCanary.java:126) with 2 output partitions\n"
            + "2019-09-02 02:41:40,178 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 started on project expert_01\n"
            + "2019-09-02 02:41:40,185 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8_01 from RUNNING to SUCCEED\n"
            + "2019-09-02 02:41:40,561 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 updates 1 metadata items\n"
            + "2019-09-02 02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork a01fdc0f-ce3e-4094-9f9b-d1b71e8d9069 takes 390ms to complete\n"
            + "2019-09-02 02:41:40,568 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 started on project expert_01\n"
            + "2019-09-02 02:41:40,569 INFO  [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] execution.NExecutableManager : Job id: d0f45b72-db2f-407b-9d6f-7cfe6f6624e8 from RUNNING to SUCCEED\n"
            + "2019-09-02 02:41:40,877 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : transaction 02e4d49e-80f8-4c58-aa44-6be1dab98e77 updates 1 metadata items\n"
            + "2019-09-02 02:41:40,885 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 02e4d49e-80f8-4c58-aa44-6be1dab98e77 takes 317ms to complete\n"
            + "2019-09-02 02:41:40,890 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n"
            + "2019-09-02 02:41:41,886 DEBUG [EventChecker(project:expert_01)-p-4-t-4] manager.EventOrchestrator : project expert_01 contains 1 events\n"
            + "2019-09-02 02:41:41,888 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e started on project expert_01\n"
            + "2019-09-02 02:41:41,889 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : transaction ca43de24-b825-4414-beeb-3b966651524e updates 1 metadata items\n"
            + "2019-09-02 02:41:41,889 INFO  [FetchJobWorker(project:expert_01)-p-12-t-12] threadpool.NDefaultScheduler : Job Status in project expert_01: 0 should running, 0 actual running, 0 stopped, 0 ready, 12 already succeed, 0 error, 0 discarded, 0 suicidal,  0 others\n"
            + "2019-09-02 02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork ca43de24-b825-4414-beeb-3b966651524e takes 13ms to complete\n"
            + "2019-09-02 02:41:41,901 DEBUG [EventChecker(project:expert_01)-p-4-t-4] transaction.UnitOfWork : UnitOfWork 297a3ec5-ca3b-4f19-a0a3-40079c3db372 started on project expert_01\n"
            + "2019-09-02 02:41:42,903 INFO  [EventChecker(project:expert_01)-p-4-t-4] handle.AbstractEventHandler : handling event: \n"
            + " {\n" + "  \"@class\" : \"io.kyligence.kap.event.model.PostAddCuboidEvent\",\n"
            + "  \"uuid\" : \"9d1cce98-29ab-4fd7-8025-11ac27c92b56\",\n" + "  \"last_modified\" : 1567392101889,\n"
            + "  \"create_time\" : 1567391988091,\n" + "  \"version\" : \"4.0.0.0\",\n"
            + "  \"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\",\n" + "  \"isGlobal\" : false,\n"
            + "  \"params\" : { },\n" + "  \"msg\" : null,\n" + "  \"sequence_id\" : 1,\n"
            + "  \"owner\" : \"ADMIN\",\n" + "  \"runTimes\" : 1,\n"
            + "  \"job_id\" : \"d0f45b72-db2f-407b-9d6f-7cfe6f6624e8\"\n" + "}";

    private static final String kgLogMsg = "2020-06-08 05:23:22,410 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...\n"
            + "2020-06-08 05:24:22,410 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: start to run health checkers ...\n"
            + "2020-06-08 05:24:22,410 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.KEProcessChecker] start to do check ...\n"
            + "2020-06-08 05:24:22,432 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.KEProcessChecker], do check finished! \n"
            + "2020-06-08 05:24:22,432 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.KEProcessChecker] check result is NORMAL, message: \n"
            + "2020-06-08 05:24:22,432 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:24:22,432 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker] start to do check ...\n"
            + "2020-06-08 05:24:22,614 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker], do check finished! \n"
            + "2020-06-08 05:24:22,614 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.UpGradeStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker] check result is QUERY_UPGRADE, messa\n"
            + "ge: Full gc time duration ratio in 300 seconds is less than 20.00%\n"
            + "2020-06-08 05:24:22,617 INFO  [ke-guardian-process] handler.UpGradeStateHandler : Upgrade query service success ...\n"
            + "2020-06-08 05:24:22,617 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.UpGradeStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:24:22,617 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.KEStatusChecker] start to do check ...\n"
            + "2020-06-08 05:24:22,621 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.KEStatusChecker], do check finished! \n"
            + "2020-06-08 05:24:22,621 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.KEStatusChecker] check result is NORMAL, message: \n"
            + "2020-06-08 05:24:22,621 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:24:22,621 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...\n"
            + "2020-06-08 05:25:22,621 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: start to run health checkers ...";

    private static final String kgLog1Msg = "2020-06-08 05:25:22,621 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.KEProcessChecker] start to do check ...\n"
            + "2020-06-08 05:25:22,642 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.KEProcessChecker], do check finished! \n"
            + "2020-06-08 05:25:22,642 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.KEProcessChecker] check result is NORMAL, message: \n"
            + "2020-06-08 05:25:22,642 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:25:22,642 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker] start to do check ...\n"
            + "2020-06-08 05:25:22,830 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker], do check finished! \n"
            + "2020-06-08 05:25:22,831 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.UpGradeStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker] check result is QUERY_UPGRADE, messa\n"
            + "ge: Full gc time duration ratio in 300 seconds is less than 20.00%\n"
            + "2020-06-08 05:25:22,834 INFO  [ke-guardian-process] handler.UpGradeStateHandler : Upgrade query service success ...\n"
            + "2020-06-08 05:25:22,834 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.UpGradeStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:25:22,834 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker:[io.kyligence.kap.tool.daemon.checker.KEStatusChecker] start to do check ...\n"
            + "2020-06-08 05:25:22,838 INFO  [ke-guardian-process] checker.AbstractHealthChecker : Checker: [io.kyligence.kap.tool.daemon.checker.KEStatusChecker], do check finished! \n"
            + "2020-06-08 05:25:22,838 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler], Health Checker: [io.kyligence.kap.tool.daemon.checker.KEStatusChecker] check result is NORMAL, message: \n"
            + "2020-06-08 05:25:22,838 INFO  [ke-guardian-process] handler.AbstractCheckStateHandler : Handler: [io.kyligence.kap.tool.daemon.handler.NormalStateHandler] handle the check result success ...\n"
            + "2020-06-08 05:25:22,838 INFO  [ke-guardian-process] daemon.KapGuardian : Guardian Process: health check finished ...";

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
        TimeZone timeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        File accessLog1 = new File(ToolUtil.getLogFolder(), "access_log.2020-01-01.log");
        File accessLog2 = new File(ToolUtil.getLogFolder(), "access_log.2020-01-02.log");
        File gcLog = new File(ToolUtil.getLogFolder(), "kylin.gc.1");
        File shellLog = new File(ToolUtil.getLogFolder(), "shell.1");
        File jstackLog1 = new File(ToolUtil.getLogFolder(), "jstack.timed.log.1577894300000");
        File jstackLog2 = new File(ToolUtil.getLogFolder(), "jstack.timed.log.1577894400000");

        FileUtils.writeStringToFile(accessLog1, "111");
        FileUtils.writeStringToFile(accessLog2, "111");
        FileUtils.writeStringToFile(gcLog, "111");
        FileUtils.writeStringToFile(shellLog, "111");
        FileUtils.writeStringToFile(jstackLog1, "111");
        FileUtils.writeStringToFile(jstackLog2, "111");

        KylinLogTool.extractOtherLogs(mainDir, 1577894400000L, 1577894400000L);

        FileUtils.deleteQuietly(accessLog1);
        FileUtils.deleteQuietly(accessLog2);
        FileUtils.deleteQuietly(gcLog);
        FileUtils.deleteQuietly(shellLog);
        FileUtils.deleteQuietly(jstackLog1);
        FileUtils.deleteQuietly(jstackLog2);

        Assert.assertFalse(new File(mainDir, "logs/access_log.2020-01-01.log").exists());
        Assert.assertTrue(new File(mainDir, "logs/access_log.2020-01-02.log").exists());
        Assert.assertTrue(new File(mainDir, "logs/kylin.gc.1").exists());
        Assert.assertTrue(new File(mainDir, "logs/shell.1").exists());
        Assert.assertFalse(new File(mainDir, "logs/jstack.timed.log.1577894300000").exists());
        Assert.assertTrue(new File(mainDir, "logs/jstack.timed.log.1577894400000").exists());
        TimeZone.setDefault(timeZone);
    }

    @Test
    public void testExtractKylinLogJob() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs2);
        FileUtils.writeStringToFile(kylinLog1, logs1);

        String jobId = "d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        KylinLogTool.extractKylinLog(mainDir, jobId);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertFalse(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02 02:35:19,868"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1"))
                .contains("runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("test1"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("test2"));
        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02 02:40:07,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log"))
                .contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractKylinLogFull() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kylinLog = new File(ToolUtil.getLogFolder(), "kylin.log");
        File kylinLog1 = new File(ToolUtil.getLogFolder(), "kylin.log.1");

        FileUtils.writeStringToFile(kylinLog, logs2);
        FileUtils.writeStringToFile(kylinLog1, logs1);

        long startTime = DateTime.parse("2019-09-01").withTimeAtStartOfDay().getMillis();
        long endTime = DateTime.parse("2019-09-03").withTimeAtStartOfDay().getMillis();

        KylinLogTool.extractKylinLog(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(kylinLog);
        FileUtils.deleteQuietly(kylinLog1);

        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02 02:35:19,868"));
        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/kylin.log.1")).contains("2019-09-02 02:40:07,635"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/kylin.log"))
                .contains("\"model_id\" : \"756bd0bc-f9f1-401d-bcc8-39a1a78557e6\""));
    }

    @Test
    public void testExtractSparkLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        String sourceLogsPath = SparkLogExtractorFactory.create(getTestConfig()).getSparkLogsDir(project,
                getTestConfig());

        String normPath = sourceLogsPath.startsWith("file://") ? sourceLogsPath.substring(7) : sourceLogsPath;
        File sparkLogDir = new File(new File(normPath, DateTime.now().toString("yyyy-MM-dd")), jobId);
        FileUtils.forceMkdir(sparkLogDir);

        File tFile = new File(sparkLogDir, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");

        KylinLogTool.extractSparkLog(mainDir, project, jobId);
        FileUtils.deleteQuietly(new File(sourceLogsPath));
        Assert.assertTrue(new File(mainDir, "spark_logs/" + jobId + "/a.txt").exists());
    }

    @Test
    public void testMountSparkLogExtractor() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        String project = "expert_01";
        String jobId = "2e3be2a5-3d96-4797-a39f-3cfa88383efa";
        File sparkLogDir = new File(new File(mainDir, "expert_01/spark_logs/" + DateTime.now().toString("yyyy-MM-dd")),
                jobId);
        FileUtils.forceMkdir(sparkLogDir);

        File tFile = new File(sparkLogDir, "a.txt");
        FileUtils.writeStringToFile(tFile, "111");
        try (SetAndUnsetSystemProp extractor = new SetAndUnsetSystemProp("kylin.tool.spark-log-extractor",
                "io.kyligence.kap.tool.MountSparkLogExtractor");
                SetAndUnsetSystemProp diagTmpDir = new SetAndUnsetSystemProp("kylin.tool.mount-spark-log-dir",
                        mainDir.getAbsolutePath());
                SetAndUnsetSystemProp clearTmp = new SetAndUnsetSystemProp("kylin.tool.clean-diag-tmp-file", "true")) {
            KylinLogTool.extractSparkLog(mainDir, project, jobId);
        }
        Assert.assertFalse(sparkLogDir.exists());
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

        String sourceLogsPath = SparkLogExtractorFactory.create(getTestConfig()).getSparderLogsDir(getTestConfig());
        String normPath = sourceLogsPath.startsWith("file://") ? sourceLogsPath.substring(7) : sourceLogsPath;

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

        FileUtils.deleteQuietly(new File(sourceLogsPath));

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
        }
    }

    @Test
    public void testMountSparderLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        String[] childDirs = { "2019-08-29/application_1563861406192_0139", "2019-08-30/application_1563861406192_0139",
                "2019-08-31/application_1563861406192_0144" };
        try (SetAndUnsetSystemProp extractor = new SetAndUnsetSystemProp("kylin.tool.spark-log-extractor",
                "io.kyligence.kap.tool.MountSparkLogExtractor");
                SetAndUnsetSystemProp diagTmpDir = new SetAndUnsetSystemProp("kylin.tool.mount-spark-log-dir",
                        mainDir.getAbsolutePath());
                SetAndUnsetSystemProp clearTmp = new SetAndUnsetSystemProp("kylin.tool.clean-diag-tmp-file", "true")) {
            File sparkLogDir = new File(mainDir, "_sparder_logs");
            for (String childDir : childDirs) {
                File sparderLogDir = new File(sparkLogDir, childDir);
                FileUtils.forceMkdir(sparderLogDir);

                File tFile = new File(sparderLogDir, "executor-1.log");
                FileUtils.writeStringToFile(tFile, "111");
            }

            long startTime = DateTime.parse("2019-08-29").withTimeAtStartOfDay().getMillis() + 3600_000L;
            long endTime = DateTime.parse("2019-08-30").withTimeAtStartOfDay().getMillis() + 3600_000L;

            KylinLogTool.extractSparderLog(mainDir, startTime, endTime);
        }

        for (String childDir : childDirs) {
            Assert.assertTrue(new File(mainDir, "spark_logs/" + childDir + "/executor-1.log").exists());
        }
    }

    @Test
    public void testExtractKGLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File kgLog = new File(ToolUtil.getLogFolder(), "guardian.log");
        File kgLog1 = new File(ToolUtil.getLogFolder(), "guardian.log.1");

        FileUtils.writeStringToFile(kgLog, kgLogMsg);
        FileUtils.writeStringToFile(kgLog1, kgLog1Msg);

        long startTime = new DateTime(2020, 6, 8, 5, 24, 0).getMillis();
        long endTime = new DateTime(2020, 6, 8, 5, 26, 0).getMillis();

        KylinLogTool.extractKGLogs(mainDir, startTime, endTime);

        FileUtils.deleteQuietly(kgLog);
        FileUtils.deleteQuietly(kgLog1);

        Assert.assertTrue(
                FileUtils.readFileToString(new File(mainDir, "logs/guardian.log")).contains("2020-06-08 05:24:22,410"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "logs/guardian.log.1"))
                .contains("2020-06-08 05:25:22,838"));
        Assert.assertFalse(
                FileUtils.readFileToString(new File(mainDir, "logs/guardian.log")).contains("2020-06-08 05:23:22,410"));
    }

    @Test
    public void testGetJobLogPatternAndJobTimeString() {
        String jobId = "d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        String patternString = KylinLogTool.getJobLogPattern(jobId);
        String log;
        Assert.assertEquals(
                "^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})(.*JobWorker.*jobid:d0f45b72.*)|^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*d0f45b72-db2f-407b-9d6f-7cfe6f6624e8",
                patternString);
        Pattern pattern = Pattern.compile(patternString);
        log = "2019-09-02 02:38:16,868 INFO  [FetchJobWorker(project:a_test)-p-9-t-9] runners.FetcherRunner : fetcher schedule d0f45b72-db2f-407b-9d6f-7cfe6f6624e8";
        Matcher matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2019-09-02 02:38:16", KylinLogTool.getJobTimeString(matcher));
        log = "2019-09-02 02:40:07,635 DEBUG [JobWorker(prj:expert_01,jobid:d0f45b72)-11100] transaction.UnitOfWork : UnitOfWork 1c2792bd-448e-4450-9944-32229523895d takes 422ms to complete";
        matcher = pattern.matcher(log);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals("2019-09-02 02:40:07", KylinLogTool.getJobTimeString(matcher));
    }
}
