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
package io.kyligence.kap.rest.controller;

import io.kyligence.kap.rest.controller.v2.NJobControllerV2;
import io.kyligence.kap.rest.request.JobActionEnum;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.service.JobService;
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

public class NJobControllerV2Test {

    private MockMvc mockMvc;

    @Mock
    private JobService jobService;

    @InjectMocks
    private NJobControllerV2 nJobControllerV2 = Mockito.spy(new NJobControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nJobControllerV2)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void tesResume() throws Exception {
        String jobId = "e1ad7bb0-522e-456a-859d-2eab1df448de";
        ExecutableResponse response = new ExecutableResponse();
        Mockito.when(jobService.getJobInstance(jobId)).thenReturn(response);
        Mockito.when(jobService.manageJob(jobId, response, JobActionEnum.RESUME.toString()))
                .thenReturn(new ExecutableResponse());

        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/jobs/{jobId}/resume", jobId).contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nJobControllerV2).resume(jobId);
    }

}
