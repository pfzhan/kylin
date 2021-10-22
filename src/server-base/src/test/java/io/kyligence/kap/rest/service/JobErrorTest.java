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
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
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
    public void testGetExceptionResolve() throws IOException {
        val is = getClass().getClassLoader().getResource(EXCEPTION_RESOLVE_PATH).openStream();
        val map = JsonUtil.readValue(is, Map.class);

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
        var errStepId = RandomUtil.randomUUIDStr();
        var errSegmentId = RandomUtil.randomUUIDStr();
        var errStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));

        jobService.updateJobError(project, jobId, errStepId, errSegmentId, errStack);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(errStepId, output.getErrStepId());
        Assert.assertEquals(errSegmentId, output.getErrSegmentId());
        Assert.assertEquals(errStack, output.getErrStack());

        jobService.updateJobError(project, jobId, "", errSegmentId, errStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(errStepId, output.getErrStepId());
        Assert.assertEquals(errSegmentId, output.getErrSegmentId());
        Assert.assertEquals(errStack, output.getErrStack());
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
        var errStepId = RandomUtil.randomUUIDStr();
        var errSegmentId = RandomUtil.randomUUIDStr();
        var errStack = ExceptionUtils.getStackTrace(new KylinException(FAILED_UPDATE_JOB_STATUS, "test"));

        manager.updateJobError(jobId, errStepId, errSegmentId, errStack);
        var output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(errStepId, output.getErrStepId());
        Assert.assertEquals(errSegmentId, output.getErrSegmentId());
        Assert.assertEquals(errStack, output.getErrStack());

        manager.updateJobError(jobId, "", errSegmentId, errStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals("", output.getErrStepId());
        Assert.assertEquals(errSegmentId, output.getErrSegmentId());
        Assert.assertEquals(errStack, output.getErrStack());

        manager.updateJobError(jobId, errStepId, null, errStack);
        output = manager.getJob(jobId).getOutput();
        Assert.assertEquals(errStepId, output.getErrStepId());
        Assert.assertNull(output.getErrSegmentId());
        Assert.assertEquals(errStack, output.getErrStack());
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
        var errStepId = logicStep2.getId();
        var errSegmentId = segmentId;
        var errStack = ExceptionUtils.getStackTrace(new NoRetryException("date format not match"));
        manager.updateJobError(jobId, errStepId, errSegmentId, errStack);

        var jobDetail = jobService.getJobDetail(getProject(), executable.getId());
        Assert.assertEquals(1, jobDetail.size());
        var stepResponse = jobDetail.get(0);
        Assert.assertEquals(errStepId, stepResponse.getErrStepId());
        Assert.assertEquals(errSegmentId, stepResponse.getErrSegmentId());
        Assert.assertEquals(errStack, stepResponse.getErrStack());
        Assert.assertEquals(logicStep2.getName(), stepResponse.getErrStepName());

        val is = getClass().getClassLoader().getResource(EXCEPTION_RESOLVE_PATH).openStream();
        val map = JsonUtil.readValue(is, Map.class);
        val dateFormatNotMatchException = errStack.split("\n")[0];
        Assert.assertEquals(JsonUtil.writeValueAsString(map.get(dateFormatNotMatchException)),
                stepResponse.getErrResolve());
    }
}
