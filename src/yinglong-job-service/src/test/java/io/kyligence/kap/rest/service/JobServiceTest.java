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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STATUS_ILLEGAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;

import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.job.execution.BaseTestExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.execution.SucceedTestExecutable;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.ChainedExecutable;
import io.kyligence.kap.job.execution.ChainedStageExecutable;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.NSparkSnapshotJob;
import io.kyligence.kap.job.execution.stage.NStageForBuild;
import io.kyligence.kap.job.execution.stage.StageBase;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import lombok.val;
import lombok.var;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final JobService jobService = Mockito.spy(new JobService());

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final NExecutableDao executableDao = Mockito.mock(NExecutableDao.class);

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
    public void testCreateInstanceFromJobByReflection() throws Exception {
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(AbstractExecutable.class));

        Set<BeanDefinition> components_kylin = provider.findCandidateComponents("org.apache.kylin");
        Set<BeanDefinition> components_kap = provider.findCandidateComponents("io.kyligence.kap");
        Set<BeanDefinition> components = Sets.newHashSet(components_kylin);
        components.addAll(components_kap);
        for (BeanDefinition component : components) {
            final String beanClassName = component.getBeanClassName();
            Class<? extends AbstractExecutable> clazz = ClassUtil.forName(beanClassName, AbstractExecutable.class);
            // no construction method to create a random number ID
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(Object.class);
            AbstractExecutable result = constructor.newInstance(new Object());
            if (org.apache.commons.lang3.StringUtils.equals(result.getId(), null)) {
                Assert.assertNull(result.getId());
            } else {
                Assert.assertTrue(org.apache.commons.lang3.StringUtils.endsWith(result.getId(), "null"));
            }
        }
    }

    private List<ProjectInstance> mockProjects() {
        ProjectInstance defaultProject = new ProjectInstance();
        defaultProject.setName("default");
        defaultProject.setMvcc(0);

        ProjectInstance defaultProject1 = new ProjectInstance();
        defaultProject1.setName("default1");
        defaultProject1.setMvcc(0);

        return Lists.newArrayList(defaultProject, defaultProject1);
    }

    private List<AbstractExecutable> mockJobs1(ExecutableManager executableManager) throws Exception {
        ExecutableManager manager = Mockito.spy(ExecutableManager.getInstance(getTestConfig(), "default1"));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject("default1");
        job1.setName("sparkjob22");
        job1.setTargetSubject("model22");
        jobs.add(job1);
        mockExecutablePOJobs(jobs, executableManager);
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324102100L);

        return jobs;
    }

    private void addSegment(AbstractExecutable job) {
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testJobStepRatio() {
        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        ExecutableResponse response = ExecutableResponse.create(executable, ExecutableManager.toPO(executable, project));
        Assert.assertEquals(0.99F, response.getStepRatio(), 0.001);
    }

    @Test
    public void testSnapshotDataRange() {
        val project = "default";
        NSparkSnapshotJob snapshotJob = new NSparkSnapshotJob();
        snapshotJob.setProject(project);
        Map params = new HashMap<String, String>();
        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        snapshotJob.setParams(params);
        ExecutableResponse response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"3\",\"2\",\"1\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("[\"1\",\"2\",\"3\"]", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "false");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("FULL", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, "testCol");
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, null);
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("INC", response.getSnapshotDataRange());

        params.put(NBatchConstants.P_INCREMENTAL_BUILD, "true");
        params.put(NBatchConstants.P_SELECTED_PARTITION_COL, null);
        params.put(NBatchConstants.P_SELECTED_PARTITION_VALUE, "[\"1\",\"2\",\"3\"]");
        snapshotJob.setParams(params);
        response = ExecutableResponse.create(snapshotJob, ExecutableManager.toPO(snapshotJob, project));
        assertEquals("FULL", response.getSnapshotDataRange());
    }

    @Test
    public void testCalculateStepRatio() {
        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(project);
        addSegment(executable);
        FiveSecondSucceedTestExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject(project);
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.PAUSED, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(task.getId(), ExecutableState.SUCCEED, null, null, null);

        var ratio = ExecutableResponse.calculateStepRatio(executable);
        assertTrue(0.99F == ratio);
    }

    @Test
    public void testcalculateSuccessStageInTaskMapSingle() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.8 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(2 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStageInTaskMap() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + UUID.randomUUID();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        var successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1.5 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(0.5 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap();
        successLogicStep = ExecutableResponse.calculateSuccessStageInTaskMap(sparkExecutable, buildSteps);
        assertTrue(1 == successLogicStep);
    }

    @Test
    public void testcalculateSuccessStage() {
        String segmentId = RandomUtil.randomUUIDStr();

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentId);
        sparkExecutable.setParam(NBatchConstants.P_INDEX_COUNT, "10");
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        NStageForBuild build1 = new NStageForBuild();
        NStageForBuild build2 = new NStageForBuild();
        NStageForBuild build3 = new NStageForBuild();
        final StageBase logicStep1 = (StageBase) sparkExecutable.addStage(build1);
        final StageBase logicStep2 = (StageBase) sparkExecutable.addStage(build2);
        final StageBase logicStep3 = (StageBase) sparkExecutable.addStage(build3);
        sparkExecutable.setStageMap();

        manager.addJob(executable);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SKIP, null, "test output");
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");

        var buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        var successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(3 == successLogicStep);

        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);
        manager.updateStageStatus(logicStep3.getId(), segmentId, ExecutableState.ERROR, null, "test output", true);

        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0 == successLogicStep);

        Map<String, String> info = Maps.newHashMap();
        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "1");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0.1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "8");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(0.8 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "10");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.RUNNING, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(0 == successLogicStep);

        info.put(NBatchConstants.P_INDEX_SUCCESS_COUNT, "12");
        manager.updateStageStatus(logicStep1.getId(), segmentId, ExecutableState.SUCCEED, info, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(1 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(1 == successLogicStep);

        manager.updateStageStatus(logicStep2.getId(), segmentId, ExecutableState.SUCCEED, null, "test output");
        buildSteps = ((ChainedStageExecutable) ((ChainedExecutable) manager.getJob(executable.getId())).getTasks()
                .get(0)).getStagesMap().get(segmentId);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, true);
        assertTrue(2 == successLogicStep);
        successLogicStep = ExecutableResponse.calculateSuccessStage(sparkExecutable, segmentId, buildSteps, false);
        assertTrue(2 == successLogicStep);
    }

    @Test
    public void updateStageOutputTaskMapEmpty() {
        String segmentId = RandomUtil.randomUUIDStr();
        String segmentId2 = RandomUtil.randomUUIDStr();
        String segmentIds = segmentId + "," + segmentId2;

        val project = "default";
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setId(RandomUtil.randomUUIDStr());
        executable.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NSparkExecutable sparkExecutable = new NSparkExecutable();
        sparkExecutable.setParam(NBatchConstants.P_SEGMENT_IDS, segmentIds);
        sparkExecutable.setId(RandomUtil.randomUUIDStr());
        executable.addTask(sparkExecutable);

        manager.addJob(executable);

        val outputOld = manager.getOutput(sparkExecutable.getId());
        manager.updateStageStatus(sparkExecutable.getId(), segmentId, ExecutableState.RUNNING, null, "test output");
        val outputNew = manager.getOutput(sparkExecutable.getId());
        assertEquals(outputOld.getState(), outputNew.getState());
        assertEquals(outputOld.getCreateTime(), outputNew.getCreateTime());
        assertEquals(outputOld.getEndTime(), outputNew.getEndTime());
        assertEquals(outputOld.getStartTime(), outputNew.getStartTime());
    }

    private List<AbstractExecutable> mockJobs(ExecutableManager executableManager) throws Exception {
        ExecutableManager manager = Mockito.spy(ExecutableManager.getInstance(getTestConfig(), getProject()));
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = NLocalFileMetadataTestCase
                .getInstanceByProject();
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("sparkjob1");
        job1.setTargetSubject("model1");

        SucceedChainedTestExecutable job2 = new SucceedChainedTestExecutable();
        job2.setProject(getProject());
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");

        SucceedChainedTestExecutable job3 = new SucceedChainedTestExecutable();
        job3.setProject(getProject());
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");

        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);

        val job1Output = new DefaultOutput();
        job1Output.setState(ExecutableState.SUCCEED);
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324101000L);
        Mockito.when(manager.getCreateTime(job2.getId())).thenReturn(1560324102000L);
        Mockito.when(manager.getCreateTime(job3.getId())).thenReturn(1560324103000L);

        Mockito.when(manager.getOutput(job1.getId())).thenReturn(job1Output);

        mockExecutablePOJobs(jobs, executableManager);//id update
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324101000L);
        Mockito.when(manager.getCreateTime(job2.getId())).thenReturn(1560324102000L);
        Mockito.when(manager.getCreateTime(job3.getId())).thenReturn(1560324103000L);
        Mockito.when(manager.getOutput(job1.getId())).thenReturn(job1Output);

        return jobs;
    }

    private void mockExecutablePOJobs(List<AbstractExecutable> mockJobs, ExecutableManager executableManager) {
        List<ExecutablePO> jobs = new ArrayList<>();
        for (int i = 0; i < mockJobs.size(); i++) {
            AbstractExecutable executable = mockJobs.get(i);
            ExecutablePO job1 = new ExecutablePO();
            if (executable.getOutput() != null) {
                job1.getOutput().setStatus(executable.getOutput().getState().name());
            }
            job1.setCreateTime(executable.getCreateTime());
            job1.getOutput().setCreateTime(executable.getCreateTime());
            job1.getOutput().getInfo().put("applicationid", "app000");

            job1.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
            job1.setProject(executable.getProject());
            job1.setName(executable.getName());
            job1.setTargetModel(executable.getTargetSubject());

            jobs.add(job1);
            executable.setId(jobs.get(i).getId());
            Mockito.doReturn(executable).when(executableManager).fromPO(job1);

        }

        Mockito.when(executableManager.getAllJobs(Mockito.anyLong(), Mockito.anyLong())).thenReturn(jobs);
    }

    private List<ExecutablePO> mockDetailJobs(boolean random) throws Exception {
        List<ExecutablePO> jobs = new ArrayList<>();
        for (int i = 1; i < 4; i++) {
            jobs.add(mockExecutablePO(random, i + ""));
        }
        return jobs;
    }

    private ExecutablePO mockExecutablePO(boolean random, String name) {
        ExecutablePO mockJob = new ExecutablePO();
        mockJob.setType("org.apache.kylin.job.execution.SucceedChainedTestExecutable");
        mockJob.setProject(getProject());
        mockJob.setName("sparkjob" + name);
        mockJob.setTargetModel("model" + name);
        val jobOutput = mockJob.getOutput();
        if ("1".equals(name))
            jobOutput.setStatus(ExecutableState.SUCCEED.name());

        val startTime = getCreateTime(name);
        mockJob.setCreateTime(startTime);
        jobOutput.setCreateTime(startTime);
        jobOutput.setStartTime(startTime);
        var lastEndTime = startTime;
        List<ExecutablePO> tasks = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            val childExecutable = new ExecutablePO();
            childExecutable.setUuid(mockJob.getId() + "_0" + i);
            childExecutable.setType("org.apache.kylin.job.execution.SucceedSubTaskTestExecutable");
            childExecutable.setProject(getProject());
            val jobChildOutput = childExecutable.getOutput();
            mockOutputTime(random, lastEndTime, jobChildOutput, i);
            lastEndTime = jobChildOutput.getEndTime();
            tasks.add(childExecutable);
        }
        mockJob.setTasks(tasks);

        jobOutput.setEndTime(lastEndTime);
        Mockito.when(executableDao.getJobByUuid(eq(mockJob.getId()))).thenReturn(mockJob);
        return mockJob;
    }

    private long getCreateTime(String name) {
        switch (name) {
        case "1":
            return 1560324101000L;
        case "2":
            return 1560324102000L;
        case "3":
            return 1560324103000L;
        default:
            return 0L;
        }
    }

    private void mockOutputTime(boolean random, long baseTime, ExecutableOutputPO output, int index) {
        long createTime = baseTime + (index + 1) * 2000L;
        long startTime = createTime + (index + 1) * 2000L;
        long endTime = startTime + (index + 1) * 2000L;
        if (random) {
            val randomObj = new Random();
            Supplier<Long> randomSupplier = () -> (long) randomObj.nextInt(100);
            endTime += randomSupplier.get();
        }

        output.setStartTime(startTime);
        output.setCreateTime(createTime);
        output.setEndTime(endTime);

    }

    @Test
    public void testGetJobStats() throws ParseException {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getTotalByteSize(), 0);
        Assert.assertEquals(0, jobStats.getTotalDuration(), 0);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
        String date = "2018-01-01";
        long startTime = format.parse(date).getTime();
        date = "2018-02-01";
        long endTime = format.parse(date).getTime();
        Map<String, Integer> jobCount = jobService.getJobCount("default", startTime, endTime, "day");
        Assert.assertEquals(32, jobCount.size());
        Assert.assertEquals(0, (int) jobCount.get("2018-01-01"));
        Assert.assertEquals(0, (int) jobCount.get("2018-02-01"));

        jobCount = jobService.getJobCount("default", startTime, endTime, "model");
        Assert.assertEquals(0, jobCount.size());

        Map<String, Double> jobDurationPerMb = jobService.getJobDurationPerByte("default", startTime, endTime, "day");
        Assert.assertEquals(32, jobDurationPerMb.size());
        Assert.assertEquals(0, jobDurationPerMb.get("2018-01-01"), 0.1);

        jobDurationPerMb = jobService.getJobDurationPerByte("default", startTime, endTime, "model");
        Assert.assertEquals(0, jobDurationPerMb.size());
    }

    @Test
    public void testCheckJobStatus() {
        jobService.checkJobStatus(Lists.newArrayList("RUNNING"));
        thrown.expect(KylinException.class);
        thrown.expectMessage(JOB_STATUS_ILLEGAL.getMsg());
        jobService.checkJobStatus("UNKNOWN");
    }

    @Test
    public void testFusionModelStopBatchJob() {

        String project = "streaming_test";
        FusionModelManager mgr = FusionModelManager.getInstance(getTestConfig(), project);
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), project);

        FusionModel fusionModel = mgr.getFusionModel("b05034a8-c037-416b-aa26-9e6b4a41ee40");

        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setProject(project);
        executable.setTargetSubject(fusionModel.getBatchModel().getUuid());
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);

        // test fusion model stop batch job
        String table = "SSB.P_LINEORDER_STREAMING";
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(), project);
        val tableDesc = tableMetadataManager.getTableDesc(table);
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.stopBatchJob(project, tableDesc);
            return null;
        }, project);
        AbstractExecutable job = manager.getJob(executable.getId());
        Assert.assertEquals(ExecutableState.DISCARDED, job.getStatus());

        // test no fusion model
        String table2 = "SSB.DATES";
        val tableDesc2 = tableMetadataManager.getTableDesc(table2);
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.stopBatchJob(project, tableDesc2);
            return null;
        }, project);
    }

    @Test
    public void testKillExistApplication() {
        ExecutableManager manager = ExecutableManager.getInstance(jobService.getConfig(), getProject());
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject(getProject());
        addSegment(executable);
        val task = new NSparkExecutable();
        task.setProject(getProject());
        addSegment(task);
        executable.addTask(task);
        manager.addJob(executable);
        jobService.killExistApplication(executable);

        jobService.killExistApplication(getProject(), executable.getId());
    }

    @Test
    public void testHistoryTrackerUrl() {
        getTestConfig().setProperty("kylin.history-server.enable", "true");
        AbstractExecutable task = new FiveSecondSucceedTestExecutable();
        task.setProject("default");
        DefaultOutput stepOutput = new DefaultOutput();
        stepOutput.setState(ExecutableState.RUNNING);
        stepOutput.setExtra(new HashMap<>());
        Map<String, String> waiteTimeMap = new HashMap<>();
        ExecutableState jobState = ExecutableState.RUNNING;
        ExecutableStepResponse result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        stepOutput.getExtra().put(ExecutableConstants.YARN_APP_ID, "app-id");
        result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
        getTestConfig().setProperty("kylin.history-server.enable", "false");
        result = jobService.parseToExecutableStep(task, stepOutput, waiteTimeMap, jobState);
        assert !result.getInfo().containsKey(ExecutableConstants.SPARK_HISTORY_APP_URL);
    }
}
