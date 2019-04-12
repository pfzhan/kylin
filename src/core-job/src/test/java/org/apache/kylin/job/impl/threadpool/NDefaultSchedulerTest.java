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

package org.apache.kylin.job.impl.threadpool;

import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.JobProcessContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ErrorTestExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FailedTestExecutable;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.NoErrorStatusExecutable;
import org.apache.kylin.job.execution.SelfStopExecutable;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class NDefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultSchedulerTest.class);

    public NDefaultSchedulerTest() {
        super("default");
    }

    @Override
    public void after() throws Exception {
        super.after();
        System.clearProperty("kylin.job.retry");
        System.clearProperty("kylin.job.retry-exception-classes");
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSingleTaskJob() {
        logger.info("testSingleTaskJob");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() {
        logger.info("testSucceed");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        assertTimeLegal(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
        //in case hdfs write is not finished yet
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(task1.getId()).getVerboseMsg())
                    .contains("succeed");
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(task2.getId()).getVerboseMsg())
                    .contains("succeed");
        });
        assertTimeSucceed(createTime, job.getId());
        testJobStopped(job.getId());

    }

    private void assertTimeSucceed(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertJobRun(createTime, job);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            for (AbstractExecutable task : chainedExecutable.getTasks()) {
                assertJobRun(createTime, job);
                Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task.getId()).getState());
            }
        }
    }

    private void assertTimeError(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertJobRun(createTime, job);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        boolean hasError = false;
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            for (AbstractExecutable task : chainedExecutable.getTasks()) {
                Assert.assertEquals(createTime, task.getCreateTime());
                Assert.assertTrue(task.getDuration() >= 0L);
                Assert.assertTrue(task.getDuration() >= 0L);
                if (task.getStatus() == ExecutableState.ERROR) {
                    hasError = true;
                }
            }
        }
        Assert.assertTrue(hasError);
    }

    private void assertTimeSuicide(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        Assert.assertNotNull(job);
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertTrue(job.getStartTime() > 0L);
        Assert.assertTrue(job.getEndTime() > 0L);
        Assert.assertTrue(job.getDuration() >= 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
        Assert.assertEquals(ExecutableState.SUICIDAL, executableManager.getOutput(job.getId()).getState());
    }

    private void assertTimeRunning(long createTime, String id) {
        AbstractExecutable job = executableManager.getJob(id);
        Assert.assertNotNull(job);
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
        Assert.assertTrue(job.getDuration() > 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            for (AbstractExecutable task : chainedExecutable.getTasks()) {
                if (task.getStatus().isFinalState()) {
                    Assert.assertTrue(task.getStartTime() > 0L);
                    Assert.assertTrue(task.getEndTime() > 0L);
                }
                Assert.assertEquals(createTime, task.getCreateTime());
                Assert.assertTrue(task.getDuration() >= 0L);
                Assert.assertTrue(task.getWaitTime() >= 0L);
            }
        }
    }

    private void assertTimeLegal(String id) {
        AbstractExecutable job = executableManager.getJob(id);
        assertTimeLegal(job);
        if (job instanceof DefaultChainedExecutable) {
            DefaultChainedExecutable chainedExecutable = (DefaultChainedExecutable) job;
            for (AbstractExecutable task : chainedExecutable.getTasks()) {
                assertTimeLegal(task);
            }
        }
    }

    private void assertTimeLegal(AbstractExecutable job) {
        Assert.assertNotNull(job);
        Assert.assertTrue(job.getCreateTime() > 0L);
        Assert.assertTrue(job.getStartTime() >= 0L);
        Assert.assertTrue(job.getEndTime() >= 0L);
        Assert.assertTrue(job.getDuration() >= 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
    }

    private void assertJobRun(long createTime, AbstractExecutable job) {
        Assert.assertNotNull(job);
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertTrue(job.getStartTime() > 0L);
        Assert.assertTrue(job.getEndTime() > 0L);
        Assert.assertTrue(job.getDuration() > 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
    }


    @Test
    public void testSucceedAndFailed() {
        logger.info("testSucceedAndFailed");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new FailedTestExecutable();
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task2.getId()).getState());
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(job.getId()).getVerboseMsg())
                    .contains("org.apache.kylin.job.execution.MockJobException");
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(task1.getId()).getVerboseMsg())
                    .contains("succeed");
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(task2.getId()).getVerboseMsg())
                    .contains("org.apache.kylin.job.execution.MockJobException");
        });
        assertTimeError(createTime, job.getId());
        testJobPending(job.getId());
    }

    @Test
    public void testSucceedAndError() {
        logger.info("testSucceedAndError");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new ErrorTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, executableManager.getOutput(task2.getId()).getState());
        //in case hdfs write is not finished yet
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(job.getId()).getVerboseMsg())
                    .contains("test error");
            Assertions.assertThat(executableManager.getOutputFromHDFSByJobId(task1.getId()).getVerboseMsg())
                    .contains("test error");
        });
        assertTimeError(createTime, job.getId());
        testJobPending(job.getId());

    }

    @Test
    public void testDiscard() {
        logger.info("testDiscard");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        SelfStopExecutable task1 = new SelfStopExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        // give time to launch job/task1
        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        executableManager.discardJob(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(task1.getId()).getState());
        task1.waitForDoWork();
        testJobStopped(job.getId());

    }

    private void testJobStopped(String jobId) {
        long[] durations = getAllDurations(jobId);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long[] durations2 = getAllDurations(jobId);
        Assert.assertArrayEquals(durations, durations2);
    }

    private long[] getAllDurations(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = 2 * (stopJob.getTasks().size() + 1);
        long[] durations = new long[size];
        durations[0] = stopJob.getDuration();
        durations[1] = stopJob.getWaitTime();
        int i = 2;
        for (AbstractExecutable task: stopJob.getTasks()) {
            durations[i] = task.getDuration();
            durations[i + 1] = task.getWaitTime();
            i += 2;
        }
        return durations;
    }

    private void testJobPending(String jobId) {
        long[] durations = getDurations(jobId);
        double[] pendingDurations = getPendingDurations(jobId);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long[] durations2 = getDurations(jobId);
        double[] pendingDurations2 = getPendingDurations(jobId);
        Assert.assertArrayEquals(durations, durations2);
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        pendingDurations[0] = pendingDurations[0] + 1000;
        for (int i = 1; i < pendingDurations.length; i++) {
            ExecutableState state = stopJob.getTasks().get(i - 1).getStatus();
            if (state == ExecutableState.ERROR || state == ExecutableState.PAUSED) {
                pendingDurations[i] = pendingDurations[i] + 1000;
            }
        }
        Assert.assertArrayEquals(pendingDurations, pendingDurations2, 10);
    }

    private long[] getDurations(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = (stopJob.getTasks().size() + 1);
        long[] durations = new long[size];
        durations[0] = stopJob.getDuration();
        int i = 1;
        for (AbstractExecutable task: stopJob.getTasks()) {
            durations[i] = task.getDuration();
            i += 1;
        }
        return durations;
    }

    private double[] getPendingDurations(String jobId) {
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(jobId);
        int size = (stopJob.getTasks().size() + 1);
        double[] durations = new double[size];
        durations[0] = stopJob.getWaitTime();
        int i = 1;
        for (AbstractExecutable task: stopJob.getTasks()) {
            durations[i] = task.getWaitTime();
            i += 1;
        }
        return durations;
    }

    @Test
    public void testIllegalState() {
        logger.info("testIllegalState");
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateJobOutput(task2.getId(),
                ExecutableState.RUNNING);
        waitForJobFinish(job.getId(), 10000);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSuicide_RemoveSegment() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        //in case hdfs write is not finished yet
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val output = executableManager.getOutputFromHDFSByJobId(job.getId());
            Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
            Assert.assertTrue(output.getVerboseMsg().contains("suicide"));
        });
        assertTimeSuicide(createTime, job.getId());
        testJobStopped(job.getId());
    }

    @Test
    public void testSuicide_RemoveSegmentAfterRunning() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        val task2 = new SucceedTestExecutable();
        task2.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> {
            return ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        //in case hdfs write is not finished yet
        assertTimeSuicide(createTime, job.getId());
        testJobStopped(job.getId());
    }

    @Test
    public void testSuicide_RemoveLayout() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L, 10001L), LayoutEntity::equals, true, true);
        });

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
    }

    @Test
    public void testSuccess_RemoveSomeLayout() {
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), LayoutEntity::equals, true, true);
        });

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, output.getState());
    }

    private AbstractExecutable initNoErrorJob(String modelId) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel(modelId);
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetModel(modelId);
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        task.setParam(NBatchConstants.P_LAYOUT_IDS, "1,10001");
        job.addTask(task);
        executableManager.addJob(job);
        return job;
    }

    @Test
    public void testSuicide_AfterSuccess() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
    }

    @Test
    public void testSuicide_JobCuttingIn() throws InterruptedException {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);

        Thread.sleep(100);
        NoErrorStatusExecutable job2 = new NoErrorStatusExecutable();
        job2.setProject("default");
        job2.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task2 = new SucceedTestExecutable();
        task2.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

        job2.addTask(task2);
        executableManager.addJob(job2);

        waitForJobFinish(job.getId());
        //in case hdfs write is not finished yet
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val output = executableManager.getOutputFromHDFSByJobId(job.getId());
            Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
            Assert.assertTrue(output.getVerboseMsg().contains("suicide"));
        });

    }

    @Test
    public void testIncBuildJobError_ModelBasedDataFlowOnline() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        val updateDf = dfMgr.getDataflow(job.getTargetModel());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIncBuildJobError_TableOrientedDataFlowLagBehind() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        val updateDf = dfMgr.getDataflow(job.getTargetModel());
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_TableOrientedDataFlowOnline() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        val updateDf = dfMgr.getDataflow(job.getTargetModel());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_ModelBasedDataFlowOnline() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        val updateDf = dfMgr.getDataflow(job.getTargetModel());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    private DefaultChainedExecutable testDataflowStatusWhenJobError(ManagementType tableOriented,
            JobTypeEnum indexBuild) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), project);
        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(tableOriented);
        });
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(indexBuild.toString());
        job.setJobType(indexBuild);
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new ErrorTestExecutable();
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);
        return job;
    }

    @Test
    public void testCheckJobStopped_TaskSucceed() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetSegments(targetSegs);
        job.setTargetModel(modelId);
        val task = new SucceedTestExecutable();
        task.setProject("default");
        task.setTargetModel(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        await().atMost(1500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
                    String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
                    return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus) && runningStatus.equals("inRunning");
                });
        executableManager.pauseJob(job.getId());

        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.SUCCEED, task.getStatus());
        });

        thrown.expect(JobStoppedException.class);
        task.checkJobPaused();
    }

    @Test
    public void testCheckJobStopped_TaskError() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetSegments(targetSegs);
        job.setTargetModel(modelId);
        val task = new ErrorTestExecutable();
        task.setProject("default");
        task.setTargetModel(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        await().atMost(1500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
                    String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
                    return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus) && runningStatus.equals("inRunning");
                });
        executableManager.pauseJob(job.getId());

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.READY, task.getStatus());
        });
    }

    @Test
    @Ignore("reopen it after #10272")
    public void testFinishJob_EventStoreDownAndUp() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable(2);
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);

        val mq = (MockMQ2) MessageQueue.getInstance(getTestConfig());
        val clazz = mq.getClass();
        val field = clazz.getDeclaredField("inmemQueue");
        field.setAccessible(true);
        field.set(mq, null);

        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
        });

        field.set(mq, new ArrayBlockingQueue<>(100));

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, output.getState());
    }

    @Test
    @Ignore("reopen it after #10272")
    public void testFinishJob_EventStoreDownForever() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutable job = new NoErrorStatusExecutable();
        job.setProject("default");
        job.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetModel());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable(2);
        task.setTargetModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);

        val mq = (MockMQ2) MessageQueue.getInstance(getTestConfig());
        val clazz = mq.getClass();
        val field = clazz.getDeclaredField("inmemQueue");
        field.setAccessible(true);
        field.set(mq, null);

        await().atMost(10000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
        });
    }

    @Test
    public void testSchedulerStop() {
        logger.info("testSchedulerStop");

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        // make sure the job is running
        await().atMost(2 * 1000, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        //scheduler failed due to some reason
        scheduler.shutdown();

        AbstractExecutable job1 = executableManager.getJob(job.getId());
        ExecutableState status = job1.getStatus();
        Assert.assertEquals(ExecutableState.SUCCEED, status);
    }

    @Test
    public void testSchedulerStopCase2() {
        logger.info("testSchedulerStop case 2");

        thrown.expect(ConditionTimeoutException.class);

        // testSchedulerStopCase2 shutdown first, then the job added will not be scheduled
        scheduler.shutdown();

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        waitForJobFinish(job.getId(), 6000);
    }

    @Test
    public void testSchedulerRestart() {
        logger.info("testSchedulerRestart");

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondSucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 2s to make sure SucceedTestExecutable is running
        await().atMost(2000, TimeUnit.MILLISECONDS).until(() -> {
            return ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0).getStatus() == ExecutableState.RUNNING;
        });
        //scheduler failed due to some reason
        NDefaultScheduler.shutdownByProject("default");
        //make sure the first scheduler has already stopped
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Predicate<ExecutablePO> updater = (po) -> {
          po.getOutput().setStatus(ExecutableState.RUNNING.toString());
          po.getOutput().setEndTime(0);
          po.getTasks().get(0).getOutput().setStatus(ExecutableState.RUNNING.toString());
          po.getTasks().get(0).getOutput().setEndTime(0);
          po.getTasks().get(1).getOutput().setStatus(ExecutableState.READY.toString());
          po.getTasks().get(1).getOutput().setStartTime(0);
          po.getTasks().get(1).getOutput().setWaitTime(0);
          po.getTasks().get(1).getOutput().setEndTime(0);
          return true;
        };
        executableDao.updateJob(stopJob.getId(), updater);
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, executableManager.getOutput(task2.getId()).getState());
        //restart
        startScheduler();
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> {
            return ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(1).getStatus() == ExecutableState.RUNNING;
        });
        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
        assertTimeSucceed(createTime, job.getId());
    }

    @Test
    public void testJobPauseAndResume() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //pause job due to some reason
        executableManager.pauseJob(job.getId());
        //sleep 5s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.PAUSED, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.SUCCEED, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        long task1PendingDuration = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);
        Assert.assertEquals(0, task1PendingDuration);
        Assert.assertEquals(0, task2PendingDuration);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration2 = stopJob.getDuration();
        long task1Duration2 = stopJob.getTasks().get(0).getDuration();
        long task2Duration2 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration2 = stopJob.getWaitTime();
        long task1PendingDuration2 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration2 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertEquals(totalDuration, totalDuration2);
        Assert.assertEquals(task1Duration, task1Duration2);
        Assert.assertEquals(0, task2Duration2);
        Assert.assertEquals(totalPendingDuration + 1000, totalPendingDuration2, 100);
        Assert.assertEquals(0, task1PendingDuration2);
        Assert.assertEquals(0, task2PendingDuration2);

        //resume
        executableManager.resumeJob(job.getId());
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId()))
                .getTasks().get(1).getStatus() == ExecutableState.RUNNING);
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(totalDuration < totalDuration3);
        Assert.assertEquals(task1Duration, task1Duration3);
        Assert.assertTrue(0 < task2Duration3);
        Assert.assertTrue(totalPendingDuration2 <= totalPendingDuration3);
        Assert.assertEquals(0, task1PendingDuration3);
        Assert.assertEquals(0, task2PendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());

        assertTimeSucceed(createTime, job.getId());
    }

    @Test
    public void testJobRestart() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.RUNNING, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.RUNNING, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        long task1PendingDuration = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration = stopJob.getTasks().get(1).getWaitTime();
        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);
        Assert.assertEquals(0, task1PendingDuration);
        Assert.assertEquals(0, task2PendingDuration);

        //restart
        executableManager.restartJob(job.getId());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        Assert.assertEquals(0, totalDuration3, 100);
        Assert.assertEquals(0, task1Duration3, 100);
        Assert.assertEquals(0, task2Duration3, 100);

        await().atMost(8000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.RUNNING);
        assertTimeRunning(newCreateTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
    }

    @Test
    public void testJobPauseAndRestart() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //pause job due to some reason
        executableManager.pauseJob(job.getId());
        //sleep 5s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.PAUSED, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.SUCCEED, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        long task1PendingDuration = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration = stopJob.getTasks().get(1).getWaitTime();
        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);
        Assert.assertEquals(0, task1PendingDuration);
        Assert.assertEquals(0, task2PendingDuration);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration2 = stopJob.getDuration();
        long task1Duration2 = stopJob.getTasks().get(0).getDuration();
        long task2Duration2 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration2 = stopJob.getWaitTime();
        long task1PendingDuration2 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration2 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertEquals(totalDuration, totalDuration2);
        Assert.assertEquals(task1Duration, task1Duration2);
        Assert.assertEquals(0, task2Duration2);
        Assert.assertEquals(totalPendingDuration + 1000, totalPendingDuration2, 100);
        Assert.assertEquals(0, task1PendingDuration2);
        Assert.assertEquals(0, task2PendingDuration2);

        //restart
        executableManager.restartJob(job.getId());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        Assert.assertEquals(0, totalDuration3, 100);
        Assert.assertEquals(0, task1Duration3, 100);
        Assert.assertEquals(0, task2Duration3, 100);

        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.RUNNING);
        assertTimeRunning(newCreateTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
    }

    @Test
    public void testJobErrorAndResume() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new ErrorTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 3s to make sure SucceedTestExecutable is running
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.ERROR);

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.ERROR, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        long task1PendingDuration = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);
        Assert.assertTrue(task1PendingDuration > 0);
        Assert.assertEquals(0, task2PendingDuration);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration2 = stopJob.getDuration();
        long task1Duration2 = stopJob.getTasks().get(0).getDuration();
        long task2Duration2 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration2 = stopJob.getWaitTime();
        long task1PendingDuration2 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration2 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertEquals(totalDuration, totalDuration2);
        Assert.assertEquals(task1Duration, task1Duration2);
        Assert.assertEquals(0, task2Duration2);
        Assert.assertEquals(totalPendingDuration + 1000, totalPendingDuration2, 100);
        Assert.assertEquals(task1PendingDuration + 1000, task1PendingDuration2, 100);
        Assert.assertEquals(0, task2PendingDuration2);

        //resume
        executableManager.resumeJob(job.getId());
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId()))
                .getTasks().get(0).getStatus() == ExecutableState.RUNNING);
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(totalDuration < totalDuration3);
        Assert.assertTrue(task1Duration < task1Duration3);
        Assert.assertEquals(0, task2Duration3);
        Assert.assertEquals(totalPendingDuration2, totalPendingDuration3, 1000);
        Assert.assertEquals(task1PendingDuration2, task1PendingDuration3, 1000);
        Assert.assertEquals(0, task2PendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeError(createTime, job.getId());
    }

    @Test
    public void testJobErrorAndRestart() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new ErrorTestExecutable();
        task1.setProject("default");
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetModel(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 3s to make sure SucceedTestExecutable is running
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.ERROR);

        DefaultChainedExecutable stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.ERROR, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        long totalDuration = stopJob.getDuration();
        long task1Duration = stopJob.getTasks().get(0).getDuration();
        long task2Duration = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration = stopJob.getWaitTime();
        long task1PendingDuration = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(totalDuration > 0);
        Assert.assertTrue(task1Duration > 0);
        Assert.assertEquals(0, task2Duration);
        Assert.assertTrue(totalPendingDuration >= 0);
        Assert.assertTrue(task1PendingDuration > 0);
        Assert.assertEquals(0, task2PendingDuration);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration2 = stopJob.getDuration();
        long task1Duration2 = stopJob.getTasks().get(0).getDuration();
        long task2Duration2 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration2 = stopJob.getWaitTime();
        long task1PendingDuration2 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration2 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertEquals(totalDuration, totalDuration2);
        Assert.assertEquals(task1Duration, task1Duration2);
        Assert.assertEquals(0, task2Duration2);
        Assert.assertEquals(totalPendingDuration + 1000, totalPendingDuration2, 100);
        Assert.assertEquals(task1PendingDuration + 1000, task1PendingDuration2, 100);
        Assert.assertEquals(0, task2PendingDuration2);

        //restart
        executableManager.restartJob(job.getId());
        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.READY);
        stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();
        Assert.assertEquals(ExecutableState.READY, stopJob.getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(0).getStatus());
        Assert.assertEquals(ExecutableState.READY, stopJob.getTasks().get(1).getStatus());
        Assert.assertEquals(0, totalDuration3, 10);
        Assert.assertEquals(0, task1Duration3, 10);
        Assert.assertEquals(0, task2Duration3, 10);
        Assert.assertEquals(0, totalPendingDuration3, 1000);
        Assert.assertEquals(0, task1PendingDuration3);
        Assert.assertEquals(0, task2PendingDuration3);


        await().atMost(1000, TimeUnit.MILLISECONDS).until(() -> executableManager.getJob(job.getId())
                .getStatus() == ExecutableState.RUNNING);
        assertTimeRunning(newCreateTime, job.getId());
        waitForJobFinish(job.getId());

        assertTimeError(newCreateTime, job.getId());
    }

    @Test
    public void testRetryableException() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task = new ErrorTestExecutable();
        task.setTargetModel(df.getModel().getUuid());
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        System.setProperty("kylin.job.retry", "3");

        //don't retry on DefaultChainedExecutable, only retry on subtasks
        Assert.assertFalse(job.needRetry(1, new Exception("")));
        Assert.assertTrue(task.needRetry(1, new Exception("")));
        Assert.assertFalse(task.needRetry(1, null));
        Assert.assertFalse(task.needRetry(4, new Exception("")));

        System.setProperty("kylin.job.retry-exception-classes", "java.io.FileNotFoundException");

        Assert.assertTrue(task.needRetry(1, new FileNotFoundException()));
        Assert.assertFalse(task.needRetry(1, new Exception("")));
    }

    @Test
    public void testKillProcess() {
        val cmd = "nohup sleep 5 & sleep 5";
        getTestConfig().setProperty("kylin.env", "DEV");
        val jobId = UUID.randomUUID().toString();
        Thread executorThread = new Thread(new Runnable() {
            @Override
            public void run() {
                CliCommandExecutor exec = new CliCommandExecutor();
                try {
                    Pair<Integer, String> result = exec.execute(cmd, null, jobId);
                } catch (ShellException e) {
                    // do nothing
                    e.printStackTrace();
                }
            }
        });
        executorThread.start();
        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Process process = JobProcessContext.getProcess(jobId);

            Assert.assertNotNull(process);
            Assert.assertEquals(true, process.isAlive());
        });

        executableManager.destroyProcess(jobId);

        await().atMost(1000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(JobProcessContext.getProcess(jobId));
        });

        getTestConfig().setProperty("kylin.env", "UT");
    }

}
