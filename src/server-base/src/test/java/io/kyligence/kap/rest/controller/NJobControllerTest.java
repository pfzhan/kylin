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

import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.request.JobUpdateRequest;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import io.kyligence.kap.rest.service.JobService;
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

public class NJobControllerTest {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @InjectMocks
    private NJobController nJobController = Mockito.spy(new NJobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nJobController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetJobs() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        List<ExecutableResponse> jobs = new ArrayList<>();
        Integer[] statusInt = {4};
        String[] subjects = {};
        JobFilter jobFilter = new JobFilter(statusInt, "", 4, subjects, "default", "job_name", false);
        Mockito.when(jobService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("offset", "0").param("limit", "10").param("timeFilter", "1")
                .param("subjects", "").param("jobName", "").param("status", "4")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobList(statusInt, "", 1, subjects, "default", 0, 10, "last_modified", true);
    }

    @Test
    public void testDropJob() throws Exception {
        Mockito.doNothing().when(jobService).dropJob("default", "e1ad7bb0-522e-456a-859d-2eab1df448de");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/jobs/{project}/{jobId}", "default", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).dropJob("default", "e1ad7bb0-522e-456a-859d-2eab1df448de");

    }

    @Test
    public void testUpdateJobStatus() throws Exception {
        Mockito.doNothing().when(jobService).updateJobStatus("e1ad7bb0-522e-456a-859d-2eab1df448de", "default", "RESUME");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/jobs/status")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockJobUpdateRequest()))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nJobController).updateJobStatus(Mockito.any(JobUpdateRequest.class));
    }

    @Test
    public void testGetJobDetail() throws Exception {
        Mockito.when(jobService.getJobDetail("default", "e1ad7bb0-522e-456a-859d-2eab1df448de")).thenReturn(mockStepsResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs/detail").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("jobId", "e1ad7bb0-522e-456a-859d-2eab1df448de")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobController).getJobDetail("default", "e1ad7bb0-522e-456a-859d-2eab1df448de");
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
        jobUpdateRequest.setJobId("e1ad7bb0-522e-456a-859d-2eab1df448de");
        return jobUpdateRequest;
    }
}
