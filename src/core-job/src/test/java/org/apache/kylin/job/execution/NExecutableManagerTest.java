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

package org.apache.kylin.job.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;
import org.junit.rules.TemporaryFolder;

/**
 *
 */
public class NExecutableManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NExecutableManager manager;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");

        for (String jobPath : manager.getJobs()) {
            System.out.println("deleting " + jobPath);
            manager.deleteJob(jobPath);
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        assertNotNull(manager);
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        long createTime = manager.getJob(executable.getId()).getCreateTime();
        Assert.assertTrue(createTime != 0L);
        List<AbstractExecutable> result = manager.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = manager.getJob(executable.getId());
        assertJobEqual(executable, another);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, "test output");
        Assert.assertTrue(manager.getJob(executable.getId()).getStartTime() != 0L);
        Assert.assertEquals(createTime, manager.getJob(executable.getId()).getCreateTime());
        assertJobEqual(executable, manager.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        job.addTask(executable1);

        manager.addJob(job);
        assertEquals(2, job.getTasks().size());
        assertNotNull(job.getTask(SucceedTestExecutable.class));
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        assertEquals(DefaultChainedExecutable.class, anotherJob.getClass());
        assertEquals(2, ((DefaultChainedExecutable) anotherJob).getTasks().size());
        assertNotNull(((DefaultChainedExecutable) anotherJob).getTask(SucceedTestExecutable.class));

        job.setProject("default");
        executable.setProject("default");
        executable1.setProject("default");

        assertJobEqual(job, anotherJob);
    }

    @Test
    public void testValidStateTransfer() throws Exception {
        SucceedTestExecutable job = new SucceedTestExecutable();
        String id = job.getId();
        UnitOfWork.doInTransactionWithRetry(() -> {
            manager.addJob(job);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.ERROR);
            manager.updateJobOutput(id, ExecutableState.READY);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.READY);
            manager.updateJobOutput(id, ExecutableState.RUNNING);
            manager.updateJobOutput(id, ExecutableState.SUCCEED);
            return null;
        }, "default");
    }

    @Test(expected = IllegalStateException.class)
    public void testDropJobException() throws IOException {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        manager.deleteJob(executable.getId());
    }

    @Test
    public void testDropJobSucceed() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED);
        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testDropJobSuicidal() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUICIDAL);
        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testDiscardAndDropJob() throws IOException, InterruptedException {
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        manager.addJob(executable);
        manager.discardJob(executable.getId());

        val duration = executable.getDuration();
        Thread.sleep(3000);
        Assert.assertEquals(duration, executable.getDuration());

        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.DISCARDED));
        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testResumeAndPauseJob() throws IOException, InterruptedException {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        job.addTask(executable1);
        manager.addJob(job);
        manager.pauseJob(job.getId());
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.PAUSED));
        manager.resumeJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.READY));
        manager.pauseJob(job.getId());
        val duration = job.getDuration();
        Thread.sleep(3000);
        Assert.assertEquals(duration, job.getDuration());
        manager.resumeJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.READY));
    }

    @Test(expected = IllegalStateTranferException.class)
    public void testInvalidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        manager.addJob(job);
        manager.updateJobOutput(job.getId(), ExecutableState.ERROR);
        manager.updateJobOutput(job.getId(), ExecutableState.PAUSED);
    }

    @Test
    public void testResumeAllRunningJobsHappyCase() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);

        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(job.getStatus(), ExecutableState.RUNNING);

        manager.resumeAllRunningJobs();

        job = manager.getJob(executable.getId());
        Assert.assertEquals(job.getStatus(), ExecutableState.READY);
    }

    @Test
    public void testResumeAllRunningJobsIsolationWithProject() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(job.getStatus(), ExecutableState.RUNNING);

        // another NExecutableManager in project ssb
        NExecutableManager ssbManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "ssb");
        BaseTestExecutable ssbExecutable = new SucceedTestExecutable();
        ssbManager.addJob(ssbExecutable);
        ssbManager.updateJobOutput(ssbExecutable.getId(), ExecutableState.RUNNING);

        AbstractExecutable ssbJob = ssbManager.getJob(ssbExecutable.getId());
        Assert.assertEquals(ssbJob.getStatus(), ExecutableState.RUNNING);

        manager.resumeAllRunningJobs();

        job = manager.getJob(executable.getId());
        // it only resume running jobs in project default, so the status of the job convert to ready
        Assert.assertEquals(job.getStatus(), ExecutableState.READY);

        job = ssbManager.getJob(ssbExecutable.getId());
        // the status of jobs in project ssb is still running
        Assert.assertEquals(job.getStatus(), ExecutableState.RUNNING);

    }

    private static void assertJobEqual(Executable one, Executable another) {
        assertEquals(one.getClass(), another.getClass());
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());

        assertTrue((one.getParams() == null && another.getParams() == null)
                || (one.getParams() != null && another.getParams() != null));

        if (one.getParams() != null) {
            assertEquals(one.getParams().size(), another.getParams().size());
            for (String key : one.getParams().keySet()) {
                assertEquals(one.getParams().get(key), another.getParams().get(key));
            }
        }
        if (one instanceof ChainedExecutable) {
            assertTrue(another instanceof ChainedExecutable);
            List<? extends Executable> onesSubs = ((ChainedExecutable) one).getTasks();
            List<? extends Executable> anotherSubs = ((ChainedExecutable) another).getTasks();
            assertTrue((onesSubs == null && anotherSubs == null) || (onesSubs != null && anotherSubs != null));
            if (onesSubs != null) {
                assertEquals(onesSubs.size(), anotherSubs.size());
                for (int i = 0; i < onesSubs.size(); ++i) {
                    assertJobEqual(onesSubs.get(i), anotherSubs.get(i));
                }
            }
        }
    }

    @Test
    public void testResumeJob_AllStep() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("test");
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable2 = new SucceedTestExecutable();
        job.addTask(executable2);
        manager.addJob(job);
        manager.pauseJob(job.getId());
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED);
        manager.updateJobOutput(executable2.getId(), ExecutableState.PAUSED);

        manager.restartJob(job.getId());
        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.READY));

        job1.getTasks().forEach(task -> {
            Assert.assertEquals(ExecutableState.READY, task.getStatus());
        });
    }

    @Test
    public void testPauseJob_IncBuildJobDataFlowStatusChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INC_BUILD.toString());
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.PAUSED));

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testPauseJob_IndexBuildJobDataFlowStatusNotChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.PAUSED));

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dataflow.getStatus());
    }

    @Test
    public void testEmptyType_ThrowException() {
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setProject("default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        val po = NExecutableManager.toPO(job, "default");
        po.setType(null);

        val executableDao = NExecutableDao.getInstance(getTestConfig(), "default");
        val savedPO = executableDao.addJob(po);

        Assert.assertNull(manager.getJob(savedPO.getId()));
    }

    @Test
    public void testEmailNotificationContent() {
        val project = "default";
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setProject(project);
        val start = "2015-01-01 00:00:00";
        val end = "2015-02-01 00:00:00";
        job.setParam(NBatchConstants.P_DATA_RANGE_START, SegmentRange.dateToLong(start) + "");
        job.setParam(NBatchConstants.P_DATA_RANGE_END, SegmentRange.dateToLong(end) + "");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        EmailNotificationContent content = EmailNotificationContent.createContent(JobIssueEnum.JOB_ERROR, job);
        Assert.assertTrue(content.getEmailTitle().contains(JobIssueEnum.JOB_ERROR.getDisplayName()));
        Assert.assertTrue(!content.getEmailBody().contains("$"));
        Assert.assertTrue(content.getEmailBody().contains(project));
        Assert.assertTrue(content.getEmailBody().contains(job.getName()));

        content = EmailNotificationContent.createContent(JobIssueEnum.LOAD_EMPTY_DATA, job);
        Assert.assertTrue(content.getEmailBody().contains(job.getTargetModelAlias()));

        content = EmailNotificationContent.createContent(JobIssueEnum.SOURCE_RECORDS_CHANGE, job);
        Assert.assertTrue(content.getEmailBody().contains(start));
        Assert.assertTrue(content.getEmailBody().contains(end));

    }

    @Test
    public void testGetSampleDataFromHDFS() throws IOException {
        final String junitFolder = temporaryFolder.getRoot().getAbsolutePath();
        final String mainFolder = junitFolder + "/testGetSampleDataFromHDFS";
        File file = new File(mainFolder);
        if (!file.exists()) {
            Assert.assertTrue(file.mkdir());
        } else {
            Assert.fail("exist the test case folder: " + mainFolder);
        }

        int nLines = 100;
        for (Integer logLines : Arrays.asList(0, 1, 70, 150, 230, 1024, nLines)) {
            String hdfsPath = mainFolder + "/hdfs.log" + logLines;
            List<String> text = Lists.newArrayList();
            for (int i = 0; i < logLines; i++) {
                text.add("INFO: this is line " + i);
            }

            FileUtils.writeLines(new File(hdfsPath), text);

            Assert.assertTrue(manager.isHdfsPathExists(hdfsPath));

            String sampleLog = manager.getSampleDataFromHDFS(hdfsPath, nLines);

            String[] logArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(sampleLog, "\n");

            int expectedLines;
            if (logLines <= nLines) {
                expectedLines = logLines;
            } else if (logLines < nLines * 2) {
                expectedLines = logLines + 1;
            } else {
                expectedLines = nLines * 2 + 1;
            }

            Assert.assertEquals(expectedLines, logArray.length);
            if (logLines > 0) {
                Assert.assertEquals("INFO: this is line 0", logArray[0]);
                Assert.assertEquals("INFO: this is line " + (logLines - 1), logArray[logArray.length - 1]);
            }
        }
    }
}
