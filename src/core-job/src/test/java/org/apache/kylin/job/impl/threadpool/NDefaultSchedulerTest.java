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

import static org.awaitility.Awaitility.await;

import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.JobProcessContext;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ErrorTestExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FailedTestExecutable;
import org.apache.kylin.job.execution.FiveSecondErrorTestExecutable;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.NoErrorStatusExecutableOnModel;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import lombok.var;

public class NDefaultSchedulerTest extends BaseSchedulerTest {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultSchedulerTest.class);

    public NDefaultSchedulerTest() {
        super("default");
    }

    private static double SPARK_DRIVER_BASE_MEMORY;

    @Override
    public void setup() throws Exception {
        super.setup();
        SPARK_DRIVER_BASE_MEMORY = KylinConfig.getInstanceFromEnv().getSparkEngineDriverMemoryBase();
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
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSucceed() {
        logger.info("testSucceed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem);
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
        assertMemoryRestore(currMem);
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
        assertTimeFinalState(createTime, id, ExecutableState.SUICIDAL);
    }

    private void assertTimeDiscard(long createTime, String id) {
        assertTimeFinalState(createTime, id, ExecutableState.DISCARDED);
    }

    private void assertTimeFinalState(long createTime, String id, ExecutableState state) {
        AbstractExecutable job = executableManager.getJob(id);
        Assert.assertNotNull(job);
        Assert.assertEquals(state, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(createTime, job.getCreateTime());
        Assert.assertTrue(job.getStartTime() > 0L);
        Assert.assertTrue(job.getEndTime() > 0L);
        Assert.assertTrue(job.getDuration() >= 0L);
        Assert.assertTrue(job.getWaitTime() >= 0L);
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

    private void assertMemoryRestore(double currMem) {
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(currMem, NDefaultScheduler.currentAvailableMem(), 0.1);
        });
    }

    @Test
    public void testSucceedAndFailed() {
        logger.info("testSucceedAndFailed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new FailedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
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
        assertMemoryRestore(currMem);
    }

    @Test
    public void testSucceedAndError() {
        logger.info("testSucceedAndError");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new ErrorTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem);
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
        testJobPending(job.getId());
        assertTimeError(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscard() {
        logger.info("testDiscard");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        SucceedTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        SucceedTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        // give time to launch job/task1
        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        discardJobWithLock(job.getId());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(job.getId()).getState());
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) getManager().getJob(job.getId())).getTasks().get(0).getStatus()
                        .isFinalState());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    public void testDiscardJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project);
        executableManager.addJob(job);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(ExecutableState.DISCARDED,
                executableManager.getJob(job.getId()).getStatus()));
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscardErrorJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new ErrorTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(
                () -> Assert.assertEquals(ExecutableState.ERROR, executableManager.getJob(job.getId()).getStatus()));
        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project);
        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(ExecutableState.DISCARDED,
                executableManager.getJob(job.getId()).getStatus()));
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testDiscardPausedJobBeforeSchedule() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        val task2 = new FiveSecondSucceedTestExecutable();
        task2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(
                () -> Assert.assertEquals(ExecutableState.RUNNING, executableManager.getJob(job.getId()).getStatus()));
        pauseJobWithLock(job.getId());
        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(ExecutableState.PAUSED,
                ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0).getStatus()));
        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val model = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            modelManager.dropModel(model);
            return null;
        }, project);
        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(ExecutableState.DISCARDED,
                executableManager.getJob(job.getId()).getStatus()));
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
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
        for (AbstractExecutable task : stopJob.getTasks()) {
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
            if (state.isNotProgressing()) {
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
        for (AbstractExecutable task : stopJob.getTasks()) {
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
        for (AbstractExecutable task : stopJob.getTasks()) {
            durations[i] = task.getWaitTime();
            i += 1;
        }
        return durations;
    }

    @Test
    public void testIllegalState() {
        logger.info("testIllegalState");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        job.addTask(task2);
        executableManager.addJob(job);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateJobOutput(task2.getId(),
                ExecutableState.RUNNING);
        waitForJobFinish(job.getId(), 10000);
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSuicide_RemoveSegment() {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        //in case hdfs write is not finished yet
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(ExecutableState.DISCARDED,
                executableManager.getJob(job.getId()).getStatus()));
        assertTimeDiscard(createTime, job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    private void changeSchedulerInterval() {
        changeSchedulerInterval(30);
    }

    private void changeSchedulerInterval(int second) {
        NDefaultScheduler.shutdownByProject("default");
        System.setProperty("kylin.job.scheduler.poll-interval-second", String.valueOf(second));
        startScheduler();
    }

    @Test
    public void testSuicide_RemoveSegmentAfterRunning() {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        val task2 = new SucceedTestExecutable();
        task2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                        .getStatus() == ExecutableState.RUNNING);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val task1 = ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0);
            Assert.assertEquals(ExecutableState.SUICIDAL, job.getStatus());
            Assert.assertEquals(ExecutableState.SUICIDAL, task1.getStatus());
        });
        //in case hdfs write is not finished yet
        assertTimeSuicide(createTime, job.getId());
        testJobStopped(job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testSuicide_RemoveLayout() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L, 10001L), LayoutEntity::equals, true, true);
        });

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, output.getState());
    }

    @Test
    public void testSuccess_RemoveSomeLayout() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val job = initNoErrorJob(modelId);
        val mgr = NIndexPlanManager.getInstance(getTestConfig(), project);
        mgr.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), LayoutEntity::equals, true, true);
        });

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, output.getState());
    }

    private AbstractExecutable initNoErrorJob(String modelId) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(modelId);
        job.setName("NO_ERROR_STATUS_EXECUTABLE");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,10001");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject(modelId);
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        task.setParam(NBatchConstants.P_LAYOUT_IDS, "1,10001");
        job.addTask(task);
        executableManager.addJob(job);
        return job;
    }

    @Test
    public void testSuicide_AfterSuccess() {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new SucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);
        executableManager.addJob(job);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
    }

    @Test
    public void testSuicide_JobCuttingIn() throws InterruptedException {
        changeSchedulerInterval();
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(JobTypeEnum.INDEX_BUILD.toString());
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);
        assertMemoryRestore(currMem);

        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        NoErrorStatusExecutableOnModel job2 = new NoErrorStatusExecutableOnModel();
        job2.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job2.setJobType(JobTypeEnum.INC_BUILD);
        job2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task2 = new SucceedTestExecutable();
        task2.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));

        job2.addTask(task2);
        executableManager.addJob(job2);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        //in case hdfs write is not finished yet
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(
                () -> Assert.assertEquals(ExecutableState.SUICIDAL, executableManager.getJob(job.getId()).getStatus()));

    }

    @Test
    public void testIncBuildJobError_ModelBasedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIncBuildJobError_TableOrientedDataFlowLagBehind() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INC_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_TableOrientedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.TABLE_ORIENTED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    @Test
    public void testIndexBuildJobError_ModelBasedDataFlowOnline() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val job = testDataflowStatusWhenJobError(ManagementType.MODEL_BASED, JobTypeEnum.INDEX_BUILD);

        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        val updateDf = dfMgr.getDataflow(job.getTargetSubject());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, updateDf.getStatus());
    }

    private DefaultChainedExecutable testDataflowStatusWhenJobError(ManagementType tableOriented,
            JobTypeEnum indexBuild) {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelMgr = NDataModelManager.getInstance(getTestConfig(), project);
        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(tableOriented);
        });
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        job.setName(indexBuild.toString());
        job.setJobType(indexBuild);
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new ErrorTestExecutable();
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        task.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task);

        executableManager.addJob(job);
        return job;
    }

    @Test
    public void testCheckJobStopped_TaskSucceed() throws JobStoppedException {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSegments(targetSegs);
        job.setTargetSubject(modelId);
        val task = new SucceedTestExecutable();
        task.setProject("default");
        task.setTargetSubject(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        await().atMost(1500, TimeUnit.MILLISECONDS).until(() -> {
            val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
            String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
            return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus)
                    && runningStatus.equals("inRunning");
        });
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        UnitOfWork.doInTransactionWithRetry(() -> {
            executableManager.pauseJob(job.getId());
            return null;
        }, "default");

        await().atMost(3000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.PAUSED, task.getStatus());
        });

        thrown.expect(JobStoppedNonVoluntarilyException.class);
        task.abortIfJobStopped(true);
        assertMemoryRestore(currMem);
    }

    @Test
    public void testCheckJobStopped_TaskError() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val df = dfMgr.getDataflow(modelId);
        val targetSegs = df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSegments(targetSegs);
        job.setTargetSubject(modelId);
        val task = new ErrorTestExecutable();
        task.setProject("default");
        task.setTargetSubject(modelId);
        task.setTargetSegments(targetSegs);
        job.addTask(task);

        executableManager.addJob(job);
        await().pollInterval(50, TimeUnit.MILLISECONDS).atMost(60000, TimeUnit.MILLISECONDS).until(() -> {
            val executeManager = NExecutableManager.getInstance(getTestConfig(), project);
            String runningStatus = executeManager.getOutput(task.getId()).getExtra().get("runningStatus");
            return job.getStatus() == ExecutableState.RUNNING && StringUtils.isNotEmpty(runningStatus)
                    && runningStatus.equals("inRunning");
        });
        pauseJobWithLock(job.getId());

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            Assert.assertEquals(ExecutableState.PAUSED, job.getStatus());
            Assert.assertEquals(ExecutableState.PAUSED, task.getStatus());
        });
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    @Test
    @Ignore("reopen it after #10272")
    public void testFinishJob_EventStoreDownAndUp() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), project);
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable(2);
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
        NoErrorStatusExecutableOnModel job = new NoErrorStatusExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val df = dfMgr.getDataflow(job.getTargetSubject());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        val task = new FiveSecondSucceedTestExecutable(2);
        task.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        // make sure the job is running
        await().atMost(2 * 1000, TimeUnit.MILLISECONDS).until(() -> job.getStatus() == ExecutableState.RUNNING);
        //scheduler failed due to some reason
        scheduler.shutdown();
        Assert.assertFalse(scheduler.hasStarted());

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
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        waitForJobFinish(job.getId(), 6000);
    }

    @Test
    public void testSchedulerRestart() {
        logger.info("testSchedulerRestart");

        var currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondSucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 2s to make sure SucceedTestExecutable is running
        await().atMost(2000, TimeUnit.MILLISECONDS).until(() -> {
            return ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                    .getStatus() == ExecutableState.RUNNING;
        });

        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);

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
        currMem = NDefaultScheduler.currentAvailableMem();
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        await().atMost(3000, TimeUnit.MILLISECONDS).until(() -> {
            return ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(1)
                    .getStatus() == ExecutableState.RUNNING;
        });
        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
        assertTimeSucceed(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testJobPauseAndResume() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                        .getStatus() == ExecutableState.RUNNING);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //pause job due to some reason
        pauseJobWithLock(job.getId());
        //sleep 7s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertPausedState(context1, 3000);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertPausedPending(context1, context2, 1000);

        assertMemoryRestore(currMem);

        //resume
        resumeJobWithLock(job.getId());
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(1)
                        .getStatus() == ExecutableState.RUNNING);
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(context2.getRecord().getDuration() < totalDuration3);
        val stepType = context1.getStepRecords().get(0).getState();
        if (stepType == ExecutableState.READY) {
            Assert.assertEquals(context2.getStepRecords().get(0).getDuration() + 5000, task1Duration3, 1000);
        } else if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(context2.getStepRecords().get(0).getDuration(), task1Duration3);
        }
        Assert.assertTrue(0 < task2Duration3);
        Assert.assertTrue(context2.getRecord().getWaitTime() <= totalPendingDuration3);
        Assert.assertTrue(context2.getStepRecords().get(0).getWaitTime() <= task1PendingDuration3);
        Assert.assertEquals(0, task2PendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        assertTimeSucceed(createTime, job.getId());
        Assert.assertEquals(1, killProcessCount.get());

    }

    @Test
    public void testJobRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                        .getStatus() == ExecutableState.RUNNING);

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
        restartJobWithLock(job.getId());
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

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING);
        assertTimeRunning(newCreateTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testJobPauseAndRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new FiveSecondSucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                        .getStatus() == ExecutableState.RUNNING);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //pause job due to some reason
        pauseJobWithLock(job.getId());
        //sleep 7s to make sure DefaultChainedExecutable is paused
        try {
            Thread.sleep(7000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context1 = new ExecutableDurationContext(project, job.getId());
        assertPausedState(context1, 3000);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertPausedPending(context1, context2, 1000);
        assertMemoryRestore(currMem);

        restartJobWithLock(job.getId());
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
        Assert.assertTrue(newCreateTime > createTime);
        assertTimeLegal(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        Assert.assertEquals(0, totalDuration3, 200);
        Assert.assertEquals(0, task1Duration3, 200);
        Assert.assertEquals(0, task2Duration3, 200);
        AtomicBoolean ended = new AtomicBoolean(false);
        await().atMost(60000, TimeUnit.MILLISECONDS).until(() -> {
            if (executableManager.getJob(job.getId()).getStatus() == ExecutableState.SUCCEED) {
                ended.set(true);
                return true;
            }
            return executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING;
        });
        if (!ended.get()) {
            assertTimeRunning(newCreateTime, job.getId());
        }
        waitForJobFinish(job.getId());
        assertTimeSucceed(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(1, killProcessCount.get());
    }

    private void assertPausedState(ExecutableDurationContext context, long interval) {
        Assert.assertEquals(ExecutableState.PAUSED, context.getRecord().getState());
        val stepType = context.getStepRecords().get(0).getState();
        Assert.assertTrue(stepType == ExecutableState.SUCCEED || stepType == ExecutableState.PAUSED);
        Assert.assertEquals(ExecutableState.READY, context.getStepRecords().get(1).getState());
        Assert.assertTrue(context.getRecord().getDuration() > 0);
        Assert.assertTrue(context.getStepRecords().get(0).getDuration() > 0);
        Assert.assertEquals(0, context.getStepRecords().get(1).getDuration());
        Assert.assertTrue(context.getRecord().getWaitTime() >= 0);
        if (stepType == ExecutableState.READY) {
            Assert.assertEquals(interval, context.getStepRecords().get(0).getWaitTime(), 1000);
        } else if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(0, context.getStepRecords().get(0).getWaitTime());
        }
        Assert.assertEquals(0, context.getStepRecords().get(1).getWaitTime());
    }

    private void assertPausedPending(ExecutableDurationContext context1, ExecutableDurationContext context2,
            long interval) {
        assertContextStateEquals(context1, context2);
        val stepType = context2.getStepRecords().get(0).getState();
        Assert.assertEquals(context1.getRecord().getDuration(), context2.getRecord().getDuration());
        Assert.assertEquals(context1.getStepRecords().get(0).getDuration(),
                context2.getStepRecords().get(0).getDuration());
        Assert.assertEquals(0, context2.getStepRecords().get(1).getDuration());
        Assert.assertEquals(context1.getRecord().getWaitTime() + interval, context2.getRecord().getWaitTime(), 100);
        if (stepType == ExecutableState.READY) {
            Assert.assertEquals(context1.getStepRecords().get(0).getWaitTime() + interval,
                    context2.getStepRecords().get(0).getWaitTime(), 100);
        } else if (stepType == ExecutableState.SUCCEED) {
            Assert.assertEquals(0, context2.getStepRecords().get(0).getWaitTime());
        }
        Assert.assertEquals(0, context2.getStepRecords().get(1).getWaitTime());
    }

    private void assertContextStateEquals(ExecutableDurationContext context1, ExecutableDurationContext context2) {
        Assert.assertEquals(context1.getRecord().getState(), context2.getRecord().getState());
        Assert.assertEquals(context1.getStepRecords().size(), context2.getStepRecords().size());
        for (int i = 0; i < context1.getStepRecords().size(); i++) {
            Assert.assertEquals(context1.getStepRecords().get(i).getState(),
                    context2.getStepRecords().get(i).getState());
        }
    }

    private void assertErrorState(ExecutableDurationContext context) {
        Assert.assertEquals(ExecutableState.ERROR, context.getRecord().getState());
        Assert.assertEquals(ExecutableState.ERROR, context.getStepRecords().get(0).getState());
        Assert.assertEquals(ExecutableState.READY, context.getStepRecords().get(1).getState());
        Assert.assertTrue(context.getRecord().getDuration() > 0);
        Assert.assertTrue(context.getStepRecords().get(0).getDuration() > 0);
        Assert.assertEquals(0, context.getStepRecords().get(1).getDuration());
        Assert.assertTrue(context.getRecord().getWaitTime() >= 0);
        Assert.assertTrue(context.getStepRecords().get(0).getWaitTime() > 0);
        Assert.assertEquals(0, context.getStepRecords().get(1).getWaitTime());
    }

    private void assertErrorPending(ExecutableDurationContext context1, ExecutableDurationContext context2,
            long interval) {
        assertContextStateEquals(context1, context2);
        Assert.assertEquals(context1.getRecord().getDuration(), context2.getRecord().getDuration());
        Assert.assertEquals(context1.getStepRecords().get(0).getDuration(),
                context2.getStepRecords().get(0).getDuration());
        Assert.assertEquals(0, context2.getStepRecords().get(1).getDuration());
        Assert.assertEquals(context1.getRecord().getWaitTime() + interval, context2.getRecord().getWaitTime(), 100);
        Assert.assertEquals(context1.getStepRecords().get(0).getWaitTime() + 1000,
                context2.getStepRecords().get(0).getWaitTime(), 100);
        Assert.assertEquals(0, context2.getStepRecords().get(1).getWaitTime());

    }

    private void restartJobWithLock(String id) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            getManager().restartJob(id);
            return null;
        }, project);
    }

    private void resumeJobWithLock(String id) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            getManager().resumeJob(id);
            return null;
        }, project);
    }

    private void pauseJobWithLock(String id) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            getManager().pauseJob(id);
            return null;
        }, project);
    }

    private void discardJobWithLock(String id) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            getManager().discardJob(id);
            return null;
        }, project);
    }

    private NExecutableManager getManager() {
        val originExecutableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val executableManager = Mockito.spy(originExecutableManager);
        Mockito.doAnswer(invocation -> {
            String jobId = invocation.getArgument(0);
            originExecutableManager.destroyProcess(jobId);
            killProcessCount.incrementAndGet();
            return null;
        }).when(executableManager).destroyProcess(Mockito.anyString());
        return executableManager;
    }

    @Test
    public void testJobErrorAndResume() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new ErrorTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);

        //sleep 3s to make sure SucceedTestExecutable is running
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR);

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertErrorState(context1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertErrorPending(context1, context2, 1000);
        assertMemoryRestore(currMem);

        //resume
        resumeJobWithLock(job.getId());
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> ((DefaultChainedExecutable) executableManager.getJob(job.getId())).getTasks().get(0)
                        .getStatus() == ExecutableState.RUNNING);
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long totalDuration3 = stopJob.getDuration();
        long task1Duration3 = stopJob.getTasks().get(0).getDuration();
        long task2Duration3 = stopJob.getTasks().get(1).getDuration();
        long totalPendingDuration3 = stopJob.getWaitTime();
        long task1PendingDuration3 = stopJob.getTasks().get(0).getWaitTime();
        long task2PendingDuration3 = stopJob.getTasks().get(1).getWaitTime();

        Assert.assertTrue(context2.getRecord().getDuration() < totalDuration3);
        Assert.assertTrue(context2.getStepRecords().get(0).getDuration() < task1Duration3);
        Assert.assertEquals(0, task2Duration3);
        Assert.assertTrue(context2.getRecord().getWaitTime() <= totalPendingDuration3);
        Assert.assertTrue(context2.getStepRecords().get(0).getWaitTime() <= task1PendingDuration3);
        Assert.assertEquals(0, task2PendingDuration3);

        assertTimeRunning(createTime, job.getId());
        waitForJobFinish(job.getId());
        assertTimeError(createTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Ignore
    @Test
    public void testJobErrorAndRestart() {
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondErrorTestExecutable();
        task1.setProject("default");
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        BaseTestExecutable task2 = new SucceedTestExecutable();
        task2.setProject("default");
        task2.setTargetSubject(df.getModel().getUuid());
        task2.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task2);
        executableManager.addJob(job);
        long createTime = executableManager.getJob(job.getId()).getCreateTime();
        Assert.assertTrue(createTime > 0L);

        //sleep 8s to make sure SucceedTestExecutable is running
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR);

        val context1 = new ExecutableDurationContext(project, job.getId());
        assertErrorState(context1);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        val context2 = new ExecutableDurationContext(project, job.getId());
        assertErrorPending(context1, context2, 1000);

        assertMemoryRestore(currMem);

        //restart
        restartJobWithLock(job.getId());
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> executableManager.getJob(job.getId()).getStatus() == ExecutableState.READY
                        && executableManager.getJob(job.getId()).getCreateTime() > createTime);
        val stopJob = (DefaultChainedExecutable) executableManager.getJob(job.getId());
        long newCreateTime = stopJob.getCreateTime();
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
        Assert.assertEquals(0, totalDuration3, 100);
        Assert.assertEquals(0, task1Duration3, 100);
        Assert.assertEquals(0, task2Duration3, 100);
        Assert.assertTrue(totalPendingDuration3 > 0);
        Assert.assertEquals(0, task1PendingDuration3);
        Assert.assertEquals(0, task2PendingDuration3);

        AtomicBoolean ended = new AtomicBoolean(false);
        await().pollInterval(50, TimeUnit.MILLISECONDS).atMost(60000, TimeUnit.MILLISECONDS).until(() -> {
            if (executableManager.getJob(job.getId()).getStatus() == ExecutableState.ERROR) {
                ended.set(true);
                return true;
            }
            return executableManager.getJob(job.getId()).getStatus() == ExecutableState.RUNNING;
        });
        if (!ended.get()) {
            assertTimeRunning(newCreateTime, job.getId());
        }
        waitForJobFinish(job.getId());
        assertTimeError(newCreateTime, job.getId());
        assertMemoryRestore(currMem);
    }

    @Test
    public void testRetryableException() {
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task = new ErrorTestExecutable();
        task.setTargetSubject(df.getModel().getUuid());
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
        final String kylinHome = System.getProperty("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            System.setProperty("KYLIN_HOME", Paths.get(this.getClass().getResource("/").getPath()).getParent()
                    .getParent().getParent().getParent() + "/build");
        }
        try {
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
        } finally {
            getTestConfig().setProperty("kylin.env", "UT");
            if (StringUtils.isEmpty(kylinHome)) {
                System.clearProperty("KYLIN_HOME");
            }
        }
    }

    @Test
    public void testSubmitParallelTasksSucceed() throws InterruptedException {
        logger.info("testSubmitParallelTasksSuccessed");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testSubmitParallelTasksError() throws InterruptedException {
        logger.info("testSubmitParallelTasksError");
        val currMem = NDefaultScheduler.currentAvailableMem();
        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutableOnModel();
        job.setProject("default");
        job.setJobType(JobTypeEnum.INDEX_BUILD);
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondErrorTestExecutable();
        task1.setTargetSubject(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);
        assertMemoryRestore(currMem - SPARK_DRIVER_BASE_MEMORY);
        waitForJobFinish(job.getId());
        assertMemoryRestore(currMem);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task1.getId()).getState());
    }

    @Ignore("#13813")
    @Test
    public void testSubmitParallelTasksReachMemoryQuota()
            throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        logger.info("testSubmitParallelTasksByMemoryQuota");
        val manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), project));
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(NExecutableManager.class).put(project, manager);
        Mockito.when(manager.countCuttingInJobByModel(Mockito.eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(AbstractExecutable.class))).thenReturn(0L);
        val currMem = NDefaultScheduler.currentAvailableMem();
        val dfs = Lists.newArrayList(NDataflowManager.getInstance(getTestConfig(), project).listAllDataflows());

        val baseMem = Math.max(Math.round(currMem / dfs.size()), 1024) * 2;
        getTestConfig().setProperty("kylin.engine.driver-memory-base", Long.valueOf(baseMem).toString());
        getTestConfig().setProperty("kylin.engine.driver-memory-maximum", "102400");
        addParallelTasksForJob(dfs, executableManager);

        await().pollInterval(50, TimeUnit.MILLISECONDS).atMost(60000, TimeUnit.MILLISECONDS)
                .until(() -> (NDefaultScheduler.currentAvailableMem() < baseMem));
        assertMemoryRestore(currMem);
    }

    private void addParallelTasksForJob(List<NDataflow> dfs, NExecutableManager executableManager) {
        for (int i = 0; i < dfs.size(); i++) {
            DefaultChainedExecutable job = new NoErrorStatusExecutableOnModel();
            job.setProject("default");
            job.setJobType(JobTypeEnum.INDEX_BUILD);
            job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
            job.setTargetSubject(dfs.get(i).getModel().getUuid());
            job.setTargetSegments(
                    dfs.get(i).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
            BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable(10);
            task1.setTargetSubject(dfs.get(i).getModel().getUuid());
            task1.setTargetSegments(
                    dfs.get(i).getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
            job.addTask(task1);
            executableManager.addJob(job);
        }
    }

}
