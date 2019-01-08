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

import java.io.FileNotFoundException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.mq.MessageQueue;
import io.kyligence.kap.cube.model.LayoutEntity;
import io.kyligence.kap.cube.model.NBatchConstants;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.cube.model.NIndexPlanManager;
import lombok.val;

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
    public void testSingleTaskJob() throws Exception {
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
    public void testSucceed() throws Exception {
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
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndFailed() throws Exception {
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
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSucceedAndError() throws Exception {
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
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.READY, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testDiscard() throws Exception {
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
        Thread.sleep(1100); // give time to launch job/task1 
        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 500);
        executableManager.discardJob(job.getId());
        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.DISCARDED, executableManager.getOutput(task1.getId()).getState());
        task1.waitForDoWork();
    }

    @Test
    public void testIllegalState() throws Exception {
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
                ExecutableState.RUNNING, null, null);
        waitForJobFinish(job.getId(), 10000);
        Assert.assertEquals(ExecutableState.ERROR, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
        Assert.assertEquals(ExecutableState.RUNNING, executableManager.getOutput(task2.getId()).getState());
    }

    @Test
    public void testSuicide_RemoveSegment() throws Exception {
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

        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfMgr.updateDataflow(update);

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
        Assert.assertTrue(output.getVerboseMsg().contains("suicide"));

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
    public void testSuicide_AfterSuccess() throws Exception {
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

        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 100);
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
        val task = new FiveSecondSucceedTestExecutable();
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
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUICIDAL, output.getState());
        Assert.assertTrue(output.getVerboseMsg().contains("suicide"));

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

        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 100);

        val mq = (MockMQ2) MessageQueue.getInstance(getTestConfig());
        val clazz = mq.getClass();
        val field = clazz.getDeclaredField("inmemQueue");
        field.setAccessible(true);
        field.set(mq, null);

        Thread.sleep(3000);
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());

        field.set(mq, new ArrayBlockingQueue<>(100));

        waitForJobFinish(job.getId());
        val output = executableManager.getOutput(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, output.getState());
    }

    @Test
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

        waitForJobStatus(job.getId(), ExecutableState.RUNNING, 100);

        val mq = (MockMQ2) MessageQueue.getInstance(getTestConfig());
        val clazz = mq.getClass();
        val field = clazz.getDeclaredField("inmemQueue");
        field.setAccessible(true);
        field.set(mq, null);

        Thread.sleep(10000);
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
    }

    @Test
    public void testSchedulerStop() throws Exception {
        logger.info("testSchedulerStop");

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        // make sure the job is running
        Thread.sleep(2 * 1000);
        //scheduler failed due to some reason
        scheduler.shutdown();

        AbstractExecutable job1 = executableManager.getJob(job.getId());
        ExecutableState status = job1.getStatus();
        Assert.assertEquals(status, ExecutableState.SUCCEED);
    }

    @Test
    public void testSchedulerStopCase2() throws Exception {
        logger.info("testSchedulerStop case 2");

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("too long wait time");

        // testSchedulerStopCase2 shutdown first, then the job added will not be scheduled
        scheduler.shutdown();

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        waitForJobFinish(job.getId(), 6000);
    }

    @Test
    public void testSchedulerRestart() throws Exception {
        logger.info("testSchedulerRestart");

        val df = NDataflowManager.getInstance(getTestConfig(), project)
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setProject("default");
        job.setTargetModel(df.getModel().getUuid());
        job.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        BaseTestExecutable task1 = new FiveSecondSucceedTestExecutable();
        task1.setTargetModel(df.getModel().getUuid());
        task1.setTargetSegments(df.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()));
        job.addTask(task1);
        executableManager.addJob(job);

        //sleep 3s to make sure SucceedTestExecutable is running 
        Thread.sleep(3000);
        //scheduler failed due to some reason
        scheduler.shutdown();
        //restart
        startScheduler();

        waitForJobFinish(job.getId());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(job.getId()).getState());
        Assert.assertEquals(ExecutableState.SUCCEED, executableManager.getOutput(task1.getId()).getState());
    }

    @Test
    public void testRetryableException() throws Exception {
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

}
