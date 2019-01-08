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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import io.kyligence.kap.rest.response.EventModelResponse;
import io.kyligence.kap.rest.response.EventResponse;
import io.kyligence.kap.rest.response.JobStatisticsResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.constant.Constant;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.rest.execution.SucceedTestExecutable;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private JobService jobService = Mockito.spy(new JobService());

    @Mock
    private TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() throws IOException {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(jobService, "tableExtService", tableExtService);

    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testListJobs() throws Exception {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs());
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter("", jobNames, 4, "", "", "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs.size() == 3);
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
        jobFilter.setSortBy("duration");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs7 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs7.size() == 3 && jobs7.get(0).getJobName().equals("sparkjob3"));
        //        jobFilter.setSortBy("exec_start_time");
        jobFilter.setReverse(false);
        //        List<ExecutableResponse> jobs8 = jobService.listJobs(jobFilter);
        //        Assert.assertTrue(jobs8.size() == 3 && jobs8.get(0).getJobName().equals("sparkjob2"));
        //        jobFilter.setSortBy("target_subject_alias");
        //        List<ExecutableResponse> jobs9 = jobService.listJobs(jobFilter);
        //        Assert.assertTrue(jobs9.size() == 3 && jobs9.get(0).getJobName().equals("sparkjob1"));
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

    @Test
    public void testBasic() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        manager.addJob(executable);
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "PAUSE", "");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.STOPPED));
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "RESUME", "");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.READY));
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD", "");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.DISCARDED));
        Assert.assertTrue(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().getFirstSegment() == null);
        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.batchDropJob("default", Lists.newArrayList(executable.getId()), "");
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testDiscardJobException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null);
        Assert.assertEquals(ExecutableState.SUCCEED, executable.getStatus());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("The job " + executable.getId() + " has already been succeed and cannot be discarded.");
        jobService.batchUpdateJobStatus(Lists.newArrayList(executable.getId()), "default", "DISCARD", "");
    }

    @Test
    public void testUpdateException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
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
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        executable.addTask(new SucceedTestExecutable());
        manager.addJob(executable);
        List<ExecutableStepResponse> result = jobService.getJobDetail("default", executable.getId());
        Assert.assertTrue(result.size() == 1);
    }

    @Test
    public void testGetJobCreateTime() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
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

    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedTestExecutable job1 = new SucceedTestExecutable();
        job1.setProject("default");
        job1.initConfig(KylinConfig.getInstanceFromEnv());
        job1.setName("sparkjob1");
        job1.setTargetModel("model1");
        SucceedTestExecutable job2 = new SucceedTestExecutable();
        job2.setProject("default");
        job2.initConfig(KylinConfig.getInstanceFromEnv());
        job2.setName("sparkjob2");
        job2.setTargetModel("model2");
        SucceedTestExecutable job3 = new SucceedTestExecutable();
        job3.setProject("default");
        job3.initConfig(KylinConfig.getInstanceFromEnv());
        job3.setName("sparkjob3");
        job3.setTargetModel("model3");
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }

    @Test
    public void testGetJobStats() throws ParseException {
        JobStatisticsResponse jobStats = jobService.getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(0, jobStats.getCount());
        Assert.assertEquals(0, jobStats.getDurationPerByte(), 0);

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

        List<EventResponse> response = jobService.getWaitingJobsByModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(4, response.size());
        Assert.assertEquals(JobTypeEnum.INDEX_BUILD.toString(), response.get(3).getJobType());
        Assert.assertEquals(JobTypeEnum.INDEX_REFRESH.toString(), response.get(0).getJobType());
        response = jobService.getWaitingJobsByModel("default", "abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        Assert.assertEquals(2, response.size());
        Assert.assertEquals(JobTypeEnum.INDEX_BUILD.toString(), response.get(1).getJobType());
        Assert.assertEquals(JobTypeEnum.INC_BUILD.toString(), response.get(0).getJobType());
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
    }

}
