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

package io.kyligence.kap.rest.controller.v2;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.job.constant.JobActionEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.job.rest.ExecutableResponse;
import io.kyligence.kap.job.rest.JobFilter;
import io.kyligence.kap.job.service.JobInfoService;
import io.kyligence.kap.rest.service.JobService;

public class JobControllerV2Test {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @Mock
    private JobInfoService jobInfoService;

    @InjectMocks
    private final JobControllerV2 jobControllerV2 = Mockito.spy(new JobControllerV2());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(jobControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(jobService, "aclEvaluate", aclEvaluate);
    }

    @Test
    public void tesResume() throws Exception {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        ExecutableResponse response = new ExecutableResponse();
        Mockito.when(jobInfoService.getJobInstance(jobId)).thenReturn(response);
        Mockito.when(jobInfoService.manageJob(jobId, response, JobActionEnum.RESUME.toString()))
                .thenReturn(new ExecutableResponse());

        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/jobs/{jobId}/resume", jobId).contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).resume(jobId);
    }

    @Test
    public void testGetJobs() throws Exception {
        List<ExecutableResponse> jobs = new ArrayList<>();
        List<String> jobNames = Lists.newArrayList();
        JobFilter jobFilter = new JobFilter(Lists.newArrayList("NEW"), jobNames, 4, "", "", "default", "job_name",
                false);
        Mockito.when(jobInfoService.listJobs(jobFilter)).thenReturn(jobs);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                .param("projectName", "default").param("pageOffset", "0").param("pageSize", "10")
                .param("timeFilter", "1").param("jobName", "").param("status", "0")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobControllerV2).getJobList(new Integer[] { 0 }, 1, "", "default", null, 0, 10, "last_modified",
                true);
    }

}