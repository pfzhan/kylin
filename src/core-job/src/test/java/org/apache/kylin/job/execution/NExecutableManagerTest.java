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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobIssueEnum;
import org.apache.kylin.job.exception.IllegalStateTranferException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;

/**
 */
public class NExecutableManagerTest extends NLocalFileMetadataTestCase {

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
        List<AbstractExecutable> result = manager.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = manager.getJob(executable.getId());
        assertJobEqual(executable, another);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, "test output");
        assertJobEqual(executable, manager.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
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
        manager.addJob(job);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.ERROR, null, null);
        manager.updateJobOutput(id, ExecutableState.READY, null, null);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.READY, null, null);
        manager.updateJobOutput(id, ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(id, ExecutableState.SUCCEED, null, null);
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
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null);
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
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUICIDAL, null, null);
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

        val duration = AbstractExecutable.getDurationIncludingPendingTime(executable.getCreateTime(),
                executable.getEndTime(), executable.getInterruptTime());
        Thread.sleep(3000);
        Assert.assertEquals(duration, AbstractExecutable.getDurationIncludingPendingTime(executable.getCreateTime(),
                executable.getEndTime(), executable.getInterruptTime()));

        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.DISCARDED));
        manager.deleteJob(executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testResumeAndPauseJob() throws IOException, InterruptedException {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable1 = new SucceedTestExecutable();
        job.addTask(executable1);
        job.setProject("default");
        manager.addJob(job);
        manager.pauseJob(job.getId());
        AbstractExecutable anotherJob = manager.getJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.STOPPED));
        manager.resumeJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.READY));
        manager.pauseJob(job.getId());
        Assert.assertTrue(job.getEndTime() != 0);
        val duration = AbstractExecutable.getDurationIncludingPendingTime(job.getCreateTime(), job.getEndTime(),
                job.getInterruptTime());
        Thread.sleep(3000);
        Assert.assertEquals(duration, AbstractExecutable.getDurationIncludingPendingTime(job.getCreateTime(),
                job.getEndTime(), job.getInterruptTime()));
        manager.resumeJob(job.getId());
        Assert.assertTrue(anotherJob.getStatus().equals(ExecutableState.READY));
    }

    @Test(expected = IllegalStateTranferException.class)
    public void testInvalidStateTransfer() {
        SucceedTestExecutable job = new SucceedTestExecutable();
        manager.addJob(job);
        manager.updateJobOutput(job.getId(), ExecutableState.ERROR, null, null);
        manager.updateJobOutput(job.getId(), ExecutableState.STOPPED, null, null);
    }

    @Test
    public void testResumeAllRunningJobsHappyCase() {
        BaseTestExecutable executable = new SucceedTestExecutable();
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);

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
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(job.getStatus(), ExecutableState.RUNNING);

        // another NExecutableManager in project ssb
        NExecutableManager ssbManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "ssb");
        BaseTestExecutable ssbExecutable = new SucceedTestExecutable();
        ssbManager.addJob(ssbExecutable);
        ssbManager.updateJobOutput(ssbExecutable.getId(), ExecutableState.RUNNING, null, null);

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
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetModel("test");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        SucceedTestExecutable executable2 = new SucceedTestExecutable();
        job.addTask(executable2);
        job.setProject("default");
        manager.addJob(job);
        manager.pauseJob(job.getId());
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null);
        manager.updateJobOutput(executable2.getId(), ExecutableState.STOPPED, null, null);

        manager.resumeJob(job.getId(), true);
        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.READY));

        job1.getTasks().forEach(task -> {
            Assert.assertEquals(ExecutableState.READY, task.getStatus());
        });
    }

    @Test
    public void testPauseJob_IncBuildJobDataFlowStatusChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setName(JobTypeEnum.INC_BUILD.toString());
        job.setJobType(JobTypeEnum.INC_BUILD);
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        job.setProject("default");
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.STOPPED));

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testPauseJob_IndexBuildJobDataFlowStatusNotChange() {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        job.addTask(executable);
        job.setProject("default");
        manager.addJob(job);
        manager.pauseJob(job.getId());

        DefaultChainedExecutable job1 = (DefaultChainedExecutable) manager.getJob(job.getId());
        Assert.assertTrue(job1.getStatus().equals(ExecutableState.STOPPED));

        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dataflow.getStatus());
    }

    @Test
    public void testEmailNotificationContent() {
        val project = "default";
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setProject(project);
        job.initConfig(getTestConfig());
        val start = "2015-01-01 00:00:00";
        val end = "2015-02-01 00:00:00";
        job.setDataRangeStart(SegmentRange.dateToLong(start));
        job.setDataRangeEnd(SegmentRange.dateToLong(end));
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
}
