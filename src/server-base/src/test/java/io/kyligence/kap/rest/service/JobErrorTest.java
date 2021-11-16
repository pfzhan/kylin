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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.rest.service.JobService.EXCEPTION_RESOLVE_DEFAULT;
import static io.kyligence.kap.rest.service.JobService.EXCEPTION_RESOLVE_PATH;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_JOB_STATUS;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.application.NoRetryException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkExecutable;
import io.kyligence.kap.engine.spark.job.step.NStageForBuild;
import io.kyligence.kap.engine.spark.job.step.NStageForMerge;
import io.kyligence.kap.engine.spark.job.step.NStageForSnapshot;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.rest.execution.SucceedChainedTestExecutable;
import lombok.val;
import lombok.var;

public class JobErrorTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService = Mockito.spy(ProjectService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobService, "tableExtService", tableExtService);
        ReflectionTestUtils.setField(jobService, "projectService", projectService);
        ReflectionTestUtils.setField(jobService, "modelService", modelService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private String getProject() {
        return "default";
    }

    @Test
    public void testWrapWithExecuteException() throws ExecuteException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, RandomUtil.randomUUIDStr());
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);
        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = sparkExecutable.getId();

        sparkExecutable.wrapWithExecuteException(() -> null);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertNull(output.getFailedStepId());

        try {
            sparkExecutable.wrapWithExecuteException(() -> {
                throw new KylinException(FAILED_UPDATE_JOB_STATUS, "test");
            });
            Assert.fail();
        } catch (ExecuteException e) {
            output = manager.getJob(jobId).getOutput();
            Assert.assertEquals(failedStepId, output.getFailedStepId());
        }
    }

    @Test
    public void testGetExceptionResolve() throws IOException {
        val exceptionResolveStream = getClass().getClassLoader().getResource(EXCEPTION_RESOLVE_PATH).openStream();
        val map = JsonUtil.readValue(exceptionResolveStream, Map.class);

        var exceptionResolve = jobService.getExceptionResolve(null);

        var expectedResolve = EXCEPTION_RESOLVE_DEFAULT;
        Assert.assertEquals(expectedResolve, exceptionResolve);

        val dateFormatNotMatch = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        val dateFormatNotMatchException = dateFormatNotMatch.split("\n")[0];
        exceptionResolve = jobService.getExceptionResolve(dateFormatNotMatchException);
        expectedResolve = JsonUtil.writeValueAsString(map.get(dateFormatNotMatchException));
        Assert.assertEquals(expectedResolve, exceptionResolve);
    }

    @Test
    public void testUpdateJobError() {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val project = getProject();
        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));

        jobService.updateJobError(project, jobId, failedStepId, failedSegmentId, failedStack);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());

        jobService.updateJobError(project, jobId, "", failedSegmentId, failedStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
    }

    @Test
    public void testUpdateJobErrorManager() {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = RandomUtil.randomUUIDStr();
        var failedSegmentId = RandomUtil.randomUUIDStr();
        var failedStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));

        manager.updateJobError(jobId, jobId, failedSegmentId, failedStack);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertNull(output.getFailedStepId());
        Assert.assertNull(output.getFailedSegmentId());
        Assert.assertNull(output.getFailedStack());

        manager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());

        manager.updateJobError(jobId, "", failedSegmentId, failedStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals("", output.getFailedStepId());
        Assert.assertEquals(failedSegmentId, output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());

        manager.updateJobError(jobId, failedStepId, null, failedStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(failedStepId, output.getFailedStepId());
        Assert.assertNull(output.getFailedSegmentId());
        Assert.assertEquals(failedStack, output.getFailedStack());
    }

    @Test
    public void testGetJobDetail() throws IOException {
        val segmentId = RandomUtil.randomUUIDStr();
        val segmentId2 = RandomUtil.randomUUIDStr();
        val errMsg = "test output";

        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        val sparkExecutable = new NSparkExecutable();
        sparkExecutable.setProject(getProject());
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
        build1.setProject(getProject());
        val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
        build2.setProject(getProject());
        val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
        build3.setProject(getProject());
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        val jobId = executable.getId();
        var failedStepId = logicStep2.getId();
        var failedSegmentId = segmentId;
        var failedStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.ERROR, null, null, "test output");
        manager.updateJobError(jobId, failedStepId, failedSegmentId, failedStack);

        var jobDetail = jobService.getJobDetail(getProject(), executable.getId());
        Assert.assertEquals(1, jobDetail.size());
        var stepResponse = jobDetail.get(0);
        Assert.assertEquals(failedStepId, stepResponse.getFailedStepId());
        Assert.assertEquals(failedSegmentId, stepResponse.getFailedSegmentId());
        Assert.assertEquals(failedStack, stepResponse.getFailedStack());
        Assert.assertEquals(logicStep2.getName(), stepResponse.getFailedStepName());

        val exceptionResolveStream = getClass().getClassLoader().getResource(EXCEPTION_RESOLVE_PATH).openStream();
        val map = JsonUtil.readValue(exceptionResolveStream, Map.class);
        val dateFormatNotMatchException = failedStack.split("\n")[0];
        Assert.assertEquals(JsonUtil.writeValueAsString(map.get(dateFormatNotMatchException)),
                stepResponse.getFailedResolve());
    }

    @Test
    public void testGetPausedTimeFromLastModify() {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        var output = manager.getOutput(executable.getId());
        var pausedTimeFromLastModify = AbstractExecutable.getPausedTimeFromLastModify(output);
        Assert.assertEquals(0, pausedTimeFromLastModify);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        output = manager.getOutput(executable.getId());
        pausedTimeFromLastModify = AbstractExecutable.getPausedTimeFromLastModify(output);
        val pausedTimeFromLastModifyFromExecutable = executable.getPausedTimeFromLastModify();
        Assert.assertTrue(pausedTimeFromLastModify <= pausedTimeFromLastModifyFromExecutable);

        ((DefaultOutput) output).setState(ExecutableState.SUCCEED);
        pausedTimeFromLastModify = AbstractExecutable.getPausedTimeFromLastModify(output);
        Assert.assertEquals(0, pausedTimeFromLastModify);

        ((DefaultOutput) output).setState(ExecutableState.SKIP);
        pausedTimeFromLastModify = AbstractExecutable.getPausedTimeFromLastModify(output);
        Assert.assertEquals(0, pausedTimeFromLastModify);
    }

    @Test
    public void testGetDuration() throws InterruptedException {
        val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
        val executable = new SucceedTestExecutable();
        executable.setProject(getProject());
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        manager.addJob(executable);

        var output = manager.getOutput(executable.getId());
        var duration = AbstractExecutable.getDuration(output);
        Assert.assertEquals(0, duration);

        ((DefaultOutput) output).setStartTime(System.currentTimeMillis());
        Thread.sleep(10);
        duration = AbstractExecutable.getDuration(output);
        Assert.assertTrue(duration >= 10);

        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING);
        output = manager.getOutput(executable.getId());
        duration = AbstractExecutable.getDuration(output);
        Assert.assertTrue((System.currentTimeMillis() - output.getStartTime()) >= duration);

        val durationFromExecutable = executable.getDuration();
        Assert.assertTrue(durationFromExecutable >= duration);
    }

    @Test
    public void testGetDurationWithoutWaiteTimeFromTwoSegment() throws JsonProcessingException {
        for (int i = 1; i < 3; i++) {
            val segmentId = RandomUtil.randomUUIDStr();
            val segmentId2 = RandomUtil.randomUUIDStr();

            val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
            val executable = new SucceedChainedTestExecutable();
            executable.setProject(getProject());
            executable.setId(RandomUtil.randomUUIDStr());
            executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

            val sparkExecutable = new NSparkExecutable();
            sparkExecutable.setProject(getProject());
            sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId + "," + segmentId2);
            sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
            sparkExecutable.setId(RandomUtil.randomUUIDStr());
            executable.addTask(sparkExecutable);

            val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
            build1.setProject(getProject());
            val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
            build2.setProject(getProject());
            val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
            build3.setProject(getProject());
            final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
            final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
            final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
            sparkExecutable.setStageMap();

            manager.addJob(executable);

            Map<String, String> info = Maps.newHashMap();
            if (i == 2) {
                Map<String, String> pauseTimeMap = Maps.newHashMap();
                pauseTimeMap.put(sparkExecutable.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep1.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep2.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep3.getId(), "1");
                info.put(NBatchConstants.P_PAUSED_TIME, JsonUtil.writeValueAsString(pauseTimeMap));
            }
            manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, info);
            manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.RUNNING);
            manager.updateStageStatus(logicStep1.getId(), null, ExecutableState.RUNNING, null, null);
            manager.updateStageStatus(logicStep2.getId(), null, ExecutableState.RUNNING, null, null);
            manager.updateStageStatus(logicStep3.getId(), null, ExecutableState.RUNNING, null, null);

            val durationWithoutWaiteTime = executable.getDurationWithoutPausedTime();

            int finalI = i;
            val sumDuration = ((ChainedExecutable) executable).getTasks().stream()
                    .map(exe -> finalI == 2 ? exe.getDuration() - 1 : exe.getDuration()).mapToLong(Long::valueOf).sum();
            Assert.assertTrue(sumDuration >= durationWithoutWaiteTime);
        }
    }

    @Test
    public void testGetDurationWithoutWaiteTimeFromSingleSegment() throws JsonProcessingException {
        for (int i = 1; i < 3; i++) {
            val segmentId = RandomUtil.randomUUIDStr();

            val manager = NExecutableManager.getInstance(jobService.getConfig(), getProject());
            val executable = new SucceedChainedTestExecutable();
            executable.setProject(getProject());
            executable.setId(RandomUtil.randomUUIDStr());
            executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

            val sparkExecutable = new NSparkExecutable();
            sparkExecutable.setProject(getProject());
            sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
            sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
            sparkExecutable.setId(RandomUtil.randomUUIDStr());
            executable.addTask(sparkExecutable);

            val build1 = new NStageForBuild(RandomUtil.randomUUIDStr());
            build1.setProject(getProject());
            val build2 = new NStageForMerge(RandomUtil.randomUUIDStr());
            build2.setProject(getProject());
            val build3 = new NStageForSnapshot(RandomUtil.randomUUIDStr());
            build3.setProject(getProject());
            final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
            final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
            final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
            sparkExecutable.setStageMap();

            manager.addJob(executable);

            Map<String, String> info = Maps.newHashMap();
            if (i == 2) {
                Map<String, String> pauseTimeMap = Maps.newHashMap();
                pauseTimeMap.put(sparkExecutable.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep1.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep2.getId(), "1");
                pauseTimeMap.put(segmentId + logicStep3.getId(), "1");
                info.put(NBatchConstants.P_PAUSED_TIME, JsonUtil.writeValueAsString(pauseTimeMap));
            }
            manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, info);
            manager.updateJobOutput(sparkExecutable.getId(), ExecutableState.RUNNING);
            manager.updateStageStatus(logicStep1.getId(), null, ExecutableState.RUNNING, null, null);
            manager.updateStageStatus(logicStep2.getId(), null, ExecutableState.RUNNING, null, null);
            manager.updateStageStatus(logicStep3.getId(), null, ExecutableState.RUNNING, null, null);

            val durationWithoutWaiteTime = executable.getDurationWithoutPausedTime();

            val stagesMap = ((ChainedStageExecutable) ((ChainedExecutable) executable).getTasks().get(0))
                    .getStagesMap();

            var sumDuration = 0L;
            for (Map.Entry<String, List<StageBase>> entry : stagesMap.entrySet()) {
                int finalI = i;
                sumDuration = entry.getValue().stream().map(stage -> stage.getOutput(entry.getKey()))
                        .map(output -> finalI == 2 ? AbstractExecutable.getDuration(output) - 1
                                : AbstractExecutable.getDuration(output))
                        .mapToLong(Long::valueOf).sum();
            }
            Assert.assertTrue(sumDuration != 0);
            Assert.assertTrue(sumDuration >= durationWithoutWaiteTime);
        }
    }
}
