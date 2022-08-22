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

package org.apache.kylin.rest.controller;

import static org.mockito.ArgumentMatchers.eq;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.util.JobDriverUIUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;


public class JobDriverUIControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Mock
    private JobDriverUIUtil jobDriverUIUtil;

    @InjectMocks
    private JobDriverUIController jobDriverUIController = Mockito.spy(new JobDriverUIController());

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(jobDriverUIController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
    }

    @Test
    public void testProxy() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get(JobDriverUIUtil.DRIVER_UI_BASE + "/project/step"))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(jobDriverUIUtil).proxy(eq("project"), eq("step"), Mockito.any(HttpServletRequest.class),
                Mockito.any(HttpServletResponse.class));
    }
}
