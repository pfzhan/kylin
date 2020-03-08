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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.request.SparkJobTimeRequest;
import io.kyligence.kap.rest.request.SparkJobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.service.JobService;
import lombok.val;

public class NJobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @InjectMocks
    private NJobController nJobController = Mockito.spy(new NJobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nJobController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetJobs() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        List<String> statuses = Lists.newArrayList("NEW", "RUNNING");
        JobFilter jobFilter = new JobFilter(statuses, jobNames, 4, "", "", "default", "job_name", false);
        Mockito.when(jobService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("page_offset", "0").param("page_size", "10")
                .param("time_filter", "1").param("subject", "").param("key", "").param("job_names", "")
                .param("statuses", "NEW,RUNNING").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobList(statuses, jobNames, 1, "", "", "default", 0, 10, "last_modified",
                true);
    }

    @Test
    public void testGetWaitingJobs() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/waiting_jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("model", "test_model")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getWaitingJobs("default", "test_model", 0, 10);
    }

    @Test
    public void testGetWaitingJobsModels() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/jobs/waiting_jobs/models").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getWaitingJobsInfoGroupByModel("default");
    }

    @Test
    public void testDropJob() throws Exception {
        Mockito.doNothing().when(jobService).batchDropJob("default",
                Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"), "");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs").param("project", "default")
                .param("job_ids", "e1ad7bb0-522e-456a-859d-2eab1df448de").param("status", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).dropJob("default", Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"),
                "");
    }

    @Test
    public void testDropGlobalJob() throws Exception {
        Mockito.doNothing().when(jobService)
                .batchDropGlobalJob(Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"), "");
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/jobs").param("job_ids", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                        .param("status", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).dropJob(null, Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"), "");
    }

    @Test
    public void testDropJob_selectNoneJob_exception() throws Exception {
        Mockito.doNothing().when(jobService).batchDropJob("default", Lists.newArrayList(), "");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs").param("project", "default").param("job_ids", "")
                .param("status", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nJobController).dropJob("default", Lists.newArrayList(), "");
    }

    @Test
    public void testUpdateJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        Mockito.doNothing().when(jobService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatus());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testUpdateGlobalJobStatus_PASS() throws Exception {
        val request = mockJobUpdateRequest();
        request.setProject(null);
        Mockito.doNothing().when(jobService).batchUpdateGlobalJobStatus(mockJobUpdateRequest().getJobIds(), "RESUME",
                mockJobUpdateRequest().getStatus());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testUpdateJobStatus_selectNoneJob_Exception() throws Exception {
        val request = mockJobUpdateRequest();
        request.setJobIds(Lists.newArrayList());
        Mockito.doNothing().when(jobService).batchUpdateJobStatus(mockJobUpdateRequest().getJobIds(), "default",
                "RESUME", mockJobUpdateRequest().getStatus());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nJobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testGetJobDetail() throws Exception {
        Mockito.when(jobService.getJobDetail("default", "e1ad7bb0-522e-456a-859d-2eab1df448de"))
                .thenReturn(mockStepsResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/{job}/detail", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("job_id", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobDetail("e1ad7bb0-522e-456a-859d-2eab1df448de", "default");
    }

    private List<ExecutableStepResponse> mockStepsResponse() {
        List<ExecutableStepResponse> result = new ArrayList<>();
        result.add(new ExecutableStepResponse());
        result.add(new ExecutableStepResponse());
        return result;
    }

    private JobUpdateRequest mockJobUpdateRequest() {
        JobUpdateRequest jobUpdateRequest = new JobUpdateRequest();
        jobUpdateRequest.setProject("default");
        jobUpdateRequest.setAction("RESUME");
        jobUpdateRequest.setJobIds(Lists.newArrayList("e1ad7bb0-522e-456a-859d-2eab1df448de"));
        return jobUpdateRequest;
    }

    @Test
    public void testGetJobOverallStats() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", String.valueOf(Long.MIN_VALUE))
                .param("end_time", String.valueOf(Long.MAX_VALUE))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobStats("default", Long.MIN_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void testGetJobCount() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics/count").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", String.valueOf(Long.MIN_VALUE))
                .param("end_time", String.valueOf(Long.MAX_VALUE)).param("dimension", "model")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobCount("default", Long.MIN_VALUE, Long.MAX_VALUE, "model");
    }

    @Test
    public void testGetJobDurationPerMb() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/statistics/duration_per_byte")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("start_time", String.valueOf(Long.MIN_VALUE)).param("end_time", String.valueOf(Long.MAX_VALUE))
                .param("dimension", "model").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobDurationPerByte("default", Long.MIN_VALUE, Long.MAX_VALUE, "model");
    }

    @Test
    public void testGetJobOutput() throws Exception {
        mockJobUpdateRequest();
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/jobs/{jobId}/steps/{stepId}/output", "e1ad7bb0-522e-456a-859d-2eab1df448de",
                        "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobOutput("e1ad7bb0-522e-456a-859d-2eab1df448de",
                "e1ad7bb0-522e-456a-859d-2eab1df448de", "default");
    }

    @Test
    public void testUpdateSparkJobInfo() throws Exception {
        SparkJobUpdateRequest request = new SparkJobUpdateRequest();
        request.setProject("default");
        request.setJobId("b");
        request.setTaskId("c");
        request.setYarnAppUrl("url");
        request.setYarnAppId("app_id");
        Mockito.doNothing().when(jobService).updateSparkJobInfo(request.getProject(), request.getJobId(),
                request.getTaskId(), request.getYarnAppId(), request.getYarnAppUrl());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/spark").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).updateSparkJobInfo(request);
    }

    @Test
    public void testUpdateSparkJobTime() throws Exception {
        SparkJobTimeRequest request = new SparkJobTimeRequest();
        request.setProject("default");
        request.setJobId("b");
        request.setTaskId("c");
        request.setYarnJobWaitTime("2");
        request.setYarnJobRunTime("1");
        Mockito.doNothing().when(jobService).updateSparkTimeInfo(request.getProject(), request.getJobId(),
                request.getTaskId(), request.getYarnJobWaitTime(), request.getYarnJobRunTime());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/wait_and_run_time")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).updateSparkJobTime(request);
    }
}
