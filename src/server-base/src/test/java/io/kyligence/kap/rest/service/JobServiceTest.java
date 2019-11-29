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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletResponse;

import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.FiveSecondSucceedTestExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.execution.SucceedChainedTestExecutable;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.EventModelResponse;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import lombok.val;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private JobService jobService = Mockito.spy(new JobService());

    @Mock
    private TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(jobService, "tableExtService", tableExtService);
    }

    @After
    public void tearDown() {
        System.clearProperty("HADOOP_USER_NAME");
        staticCleanupTestMetadata();
    }

    private String getProject() {
        return "default";
    }

    @Test
    public void testListJobs() throws Exception {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default")).thenReturn(executableManager);
        val mockJobs = mockJobs();
        Mockito.when(executableManager.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);

        // test size
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter("", jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertEquals(3, jobs.size());

        jobFilter.setTimeFilter(0);
        jobNames.add("sparkjob1");
        jobFilter.setJobNames(jobNames);
        List<ExecutableResponse> jobs2 = jobService.listJobs(jobFilter);
        Assert.assertEquals(1, jobs2.size());

        jobFilter.setSubject("model1");
        jobNames.remove(0);
        jobFilter.setJobNames(jobNames);
        jobFilter.setTimeFilter(2);
        List<ExecutableResponse> jobs3 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs3.size() == 1);

        jobFilter.setSubject("");
        jobFilter.setStatus("NEW");
        jobFilter.setTimeFilter(1);
        List<ExecutableResponse> jobs4 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs4.size() == 3);

        jobFilter.setStatus("");
        jobFilter.setTimeFilter(3);
        jobFilter.setSortBy("job_name");
        List<ExecutableResponse> jobs5 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs5.size() == 3 && jobs5.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setTimeFilter(4);
        jobFilter.setReverse(false);
        List<ExecutableResponse> jobs6 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs6.size() == 3 && jobs6.get(0).getJobName().equals("sparkjob1"));

        //        jobFilter.setSortBy("duration");
        //        jobFilter.setReverse(true);
        //        List<ExecutableResponse> jobs7 = jobService.listJobs(jobFilter);
        //        Assert.assertTrue(jobs7.size() == 3 && jobs7.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setSortBy("create_time");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs8 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs8.size() == 3 && jobs8.get(0).getJobName().equals("sparkjob3"));

        jobFilter.setReverse(false);
        jobFilter.setStatus("");
        jobFilter.setSortBy("");
        List<ExecutableResponse> jobs10 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs10.size() == 3);

        jobFilter.setSortBy("job_status");
        List<ExecutableResponse> jobs11 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs11.size() == 3 && jobs11.get(0).getJobName().equals("sparkjob1"));

        jobFilter.setSortBy("create_time");
        List<ExecutableResponse> jobs12 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs12.size() == 3 && jobs12.get(0).getJobName().equals("sparkjob1"));

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

    private List<AbstractExecutable> mockJobs1() throws NoSuchFieldException, IllegalAccessException {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), "default1"));
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject("default1");
        job1.setName("sparkjob22");
        job1.setTargetSubject("model22");
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324102100L);

        jobs.add(job1);
        return jobs;
    }

    @Test
    public void testListAllJobs() throws Exception {
        Mockito.doReturn(mockProjects()).when(jobService).getReadableProjects();

        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default")).thenReturn(executableManager);
        val mockJobs = mockJobs();
        Mockito.when(executableManager.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs);

        NExecutableManager executableManager1 = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default1")).thenReturn(executableManager1);
        val mockJobs1 = mockJobs1();
        Mockito.when(executableManager1.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs1);

        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter("", jobNames, 4, "", "", "", "", true);
        List<ExecutableResponse> jobs = jobService.listGlobalJobs(jobFilter, 0, 10).getValue();
        Assert.assertEquals(4, jobs.size());
        Assert.assertEquals("default1", jobs.get(3).getProject());
    }

    private void addSegment(AbstractExecutable job) {
        job.setProject("default");
        job.setParam(NBatchConstants.P_LAYOUT_IDS, "1,2,3,4,5");
        job.setTargetSubject("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testJobStepRatio() {
        val project = "default";
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), project);
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

        ExecutableResponse response = ExecutableResponse.create(executable);
        Assert.assertEquals(0.99F, response.getStepRatio(), 0.001);
    }

    @Test
    public void testBasic() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        manager.addJob(executable);
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE", "");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.PAUSED));
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME", "");
            return null;
        }, "default");
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE", "OTHER_STATUS");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.PAUSED));
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME", "STOPPED");
            return null;
        }, "default");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.READY));
        UnitOfWork.doInTransactionWithRetry(() -> {
            jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD", "");
            return null;
        }, "default");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.DISCARDED));
        Assert.assertTrue(
                dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment() == null);
        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.batchDropJob("default", Lists.newArrayList(executable.getId()), "");
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testGlobalBasic() throws IOException {
        Mockito.doReturn(mockProjects()).when(jobService).getReadableProjects();

        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        manager.addJob(executable);

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "PAUSE", "");
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "RESUME", "");
        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "PAUSE", "OTHER_STATUS");
        Assert.assertEquals(ExecutableState.PAUSED, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "RESUME", "STOPPED");
        Assert.assertEquals(ExecutableState.READY, manager.getJob(executable.getId()).getStatus());

        jobService.batchUpdateGlobalJobStatus(Lists.newArrayList(executable.getId()), "DISCARD", "");
        Assert.assertEquals(ExecutableState.DISCARDED, manager.getJob(executable.getId()).getStatus());

        Assert.assertNull(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment());

        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.batchDropGlobalJob(Lists.newArrayList(executable.getId()), "");
        Assert.assertFalse(manager.getAllExecutables().contains(executable));
    }

    @Test
    public void testDiscardJobException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null, null);
        Assert.assertEquals(ExecutableState.SUCCEED, executable.getStatus());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("The job " + executable.getId() + " has already been succeed and cannot be discarded.");
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD", "");
    }

    @Test
    public void testUpdateException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        manager.addJob(executable);
        thrown.expect(IllegalStateException.class);
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "ROLLBACK", "");
    }

    @Test
    public void testGetJobDetail() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        executable.addTask(new FiveSecondSucceedTestExecutable());
        manager.addJob(executable);
        List<ExecutableStepResponse> result = jobService.getJobDetail("default", executable.getId());
        Assert.assertTrue(result.size() == 1);
    }

    @Test
    public void testGetJobCreateTime() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable executable = new SucceedChainedTestExecutable();
        addSegment(executable);
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test_create_time");
        manager.addJob(executable);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter("", jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs.get(0).getCreateTime() > 0);
    }

    @Test
    public void testGetTargetSubjectAndJobType() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("mocked job");
        job1.setTargetSubject("12345678");
        final TableDesc tableDesc = NTableMetadataManager.getInstance(getTestConfig(), getProject())
                .getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        NTableSamplingJob samplingJob = NTableSamplingJob.create(tableDesc, getProject(), "ADMIN", 20000);
        manager.addJob(job1);
        manager.addJob(samplingJob);
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter("", jobNames, 4, "", "", "default", "", true);
        jobFilter.setSortBy("job_name");
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);

        Assert.assertEquals("The model is deleted", jobs.get(0).getTargetSubject()); // no target model so it's null
        Assert.assertEquals("mocked job", jobs.get(0).getJobName());
        Assert.assertEquals(tableDesc.getIdentity(), jobs.get(1).getTargetSubject());
        Assert.assertEquals("TABLE_SAMPLING", jobs.get(1).getJobName());

    }

    private List<AbstractExecutable> mockJobs() throws NoSuchFieldException, IllegalAccessException {
        NExecutableManager manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), getProject()));
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(NExecutableManager.class).put(getProject(), manager);
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedChainedTestExecutable job1 = new SucceedChainedTestExecutable();
        job1.setProject(getProject());
        job1.setName("sparkjob1");
        job1.setTargetSubject("model1");
        Mockito.when(manager.getCreateTime(job1.getId())).thenReturn(1560324101000L);
        SucceedChainedTestExecutable job2 = new SucceedChainedTestExecutable();
        job2.setProject(getProject());
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");
        Mockito.when(manager.getCreateTime(job2.getId())).thenReturn(1560324102000L);
        SucceedChainedTestExecutable job3 = new SucceedChainedTestExecutable();
        job3.setProject(getProject());
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");
        Mockito.when(manager.getCreateTime(job3.getId())).thenReturn(1560324103000L);
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }

    @Ignore
    @Test
    public void testGetJobStats() throws ParseException {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getTotalByteSize(), 0);
        Assert.assertEquals(0, jobStats.getTotalDuration(), 0);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
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
    public void testGetWaitingJobs() {
        prepareEventData();

        Map<String, Object> result = jobService.getEventsInfoGroupByModel("default");
        Assert.assertEquals(6, result.get("size"));
        Map<String, EventModelResponse> models = (Map<String, EventModelResponse>) result.get("data");
        Assert.assertEquals(2, models.size());
        Assert.assertTrue(models.containsKey("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        Assert.assertTrue(models.containsKey("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96"));
        EventModelResponse model1 = models.get("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(4, model1.getSize());
        Assert.assertEquals("nmodel_basic", model1.getModelAlias());
        EventModelResponse model2 = models.get("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertEquals(2, model2.getSize());
        Assert.assertEquals("all_fixed_length", model2.getModelAlias());

        List<EventResponse> response = jobService.getWaitingJobsByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(4, response.size());
        Assert.assertEquals(JobTypeEnum.INDEX_BUILD.toString(), response.get(3).getJobType());
        Assert.assertEquals(JobTypeEnum.INDEX_REFRESH.toString(), response.get(0).getJobType());
        response = jobService.getWaitingJobsByModel("default", "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertEquals(2, response.size());
        Assert.assertEquals(JobTypeEnum.INDEX_BUILD.toString(), response.get(1).getJobType());
        Assert.assertEquals(JobTypeEnum.INC_BUILD.toString(), response.get(0).getJobType());
    }

    @Test
    public void testGetJobOutput() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        ExecutableOutputPO executableOutputPO = new ExecutableOutputPO();
        executableOutputPO.setStatus("SUCCEED");
        executableOutputPO.setContent("succeed");
        manager.updateJobOutputToHDFS(KylinConfig.getInstanceFromEnv().getJobTmpOutputStorePath("default",
                "e1ad7bb0-522e-456a-859d-2eab1df448de"), executableOutputPO);

        Assertions.assertThat(jobService.getJobOutput("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
                .isEqualTo("succeed");
    }

    private void prepareEventData() {
        EventDao eventDao = EventDao.getInstance(getTestConfig(), "default");
        Event event1 = new AddCuboidEvent();
        event1.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        eventDao.addEvent(event1);

        Event event2 = new AddSegmentEvent();
        event2.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        eventDao.addEvent(event2);

        Event event3 = new MergeSegmentEvent();
        event3.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        eventDao.addEvent(event3);

        Event event4 = new RefreshSegmentEvent();
        event4.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        eventDao.addEvent(event4);

        Event event5 = new AddCuboidEvent();
        event5.setModelId("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        eventDao.addEvent(event5);

        Event event6 = new AddSegmentEvent();
        event6.setModelId("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        eventDao.addEvent(event6);

        Event event7 = new PostAddCuboidEvent();
        event7.setModelId("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        eventDao.addEvent(event7);

        Event event8 = new AddCuboidEvent();
        event8.setModelId("not_existing_model");
        eventDao.addEvent(event8);
    }

    @Test
    public void testHdfsFileWrite2OutputStream() throws IOException {
        final String junitFolder = temporaryFolder.getRoot().getAbsolutePath();
        final String mainFolder = junitFolder + "/testHdfsFileWrite2OutputStream";
        File file = new File(mainFolder);
        if (!file.exists()) {
            Assert.assertTrue(file.mkdir());
        } else {
            Assert.fail("exist the test case folder: " + mainFolder);
        }

        String hdfsPath = mainFolder + "/hdfs.log";
        List<String> text = Arrays.asList("INFO: this is the line 1", "WARN: this is the line 2",
                "ERROR: this is the last line");
        FileUtils.writeLines(new File(hdfsPath), text);

        try (FileOutputStream outputStream = new FileOutputStream(new File(mainFolder + "/output.log"))) {
            Assert.assertTrue(jobService.hdfsFileWrite2OutputStream(outputStream, hdfsPath));
        }

        List<String> text1 = FileUtils.readLines(new File(hdfsPath));

        Assert.assertEquals(text.size(), text1.size());
        for (int i = 0; i < text.size(); i++) {
            Assert.assertEquals(text.get(i), text1.get(i));
        }
    }

    @Test
    public void testDownloadHdfsLogFile() throws IOException {
        final String junitFolder = temporaryFolder.getRoot().getAbsolutePath();
        final String mainFolder = junitFolder + "/testDownloadHdfsLogFile";

        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default")).thenReturn(executableManager);

        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        Mockito.when(response.getOutputStream()).thenReturn(null);

        String hdfsLogPath = mainFolder + "/hdfs.log";
        Mockito.when(jobService.hdfsFileWrite2OutputStream(null, hdfsLogPath)).thenReturn(true);

        ExecutableOutputPO jobOutput = new ExecutableOutputPO();
        jobOutput.setLogPath(hdfsLogPath);
        FileUtils.writeLines(new File(jobOutput.getLogPath()), Arrays.asList("line1", "line2"));

        Mockito.when(executableManager.getJobOutputFromHDFS(Mockito.anyString())).thenReturn(jobOutput);
        Mockito.when(executableManager.isHdfsPathExists(Mockito.anyString())).thenReturn(true);
        String newHdfsLogPath = jobService.getHdfsLogPath("default", "00000_01");
        if (Objects.isNull(newHdfsLogPath)) {
            Assert.fail("hdfs log path is null!");
        }

        if (!jobService.downloadHdfsLogFile(response, newHdfsLogPath)) {
            Assert.fail("download file failed!");
        }
    }

    @Test
    public void testGetJobInstance_ManageJob() throws IOException {
        String jobId = "job1";
        ExecutableResponse executableResponse = new ExecutableResponse();
        executableResponse.setId(jobId);

        AbstractExecutable job = new NSparkCubingJob();

        Mockito.doReturn(mockProjects()).when(jobService).getReadableProjects();
        NExecutableManager manager = Mockito.mock(NExecutableManager.class);
        Mockito.when(manager.getJob(jobId)).thenReturn(job);
        Mockito.doReturn(manager).when(jobService).getExecutableManager("default");
        Assert.assertEquals("default", jobService.getProjectByJobId(jobId));

        Mockito.doReturn("default").when(jobService).getProjectByJobId(jobId);
        Mockito.doReturn(executableResponse).when(jobService).convert(job);
        Assert.assertEquals(jobId, jobService.getJobInstance(jobId).getId());

        Mockito.doNothing().when(jobService).updateJobStatus(jobId, "default", "RESUME");

        Assert.assertEquals(executableResponse, jobService.manageJob("default", executableResponse, "RESUME"));
    }
}
