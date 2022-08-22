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
package org.apache.kylin.rest.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.lang.reflect.Field;
import java.net.UnknownHostException;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.service.JobInfoService;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

public class JobDriverUIUtilTest extends NLocalFileMetadataTestCase {
    private MockHttpServletRequest request = new MockHttpServletRequest() {
    };
    private MockHttpServletResponse response = new MockHttpServletResponse();
    @Mock
    private final JobInfoService jobInfoService = Mockito.spy(JobInfoService.class);

    JobDriverUIUtil jobDriverUIUtil;

    @Test
    public void testProxy() throws Exception {
        createTestMetadata();
        MockedStatic<SparkUIUtil> mockedSparkUIUtil = Mockito.mockStatic(SparkUIUtil.class);
        jobDriverUIUtil = Mockito.spy(new JobDriverUIUtil());
        Field field = jobDriverUIUtil.getClass().getDeclaredField("jobInfoService");
        field.setAccessible(true);
        field.set(jobDriverUIUtil, jobInfoService);
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any()))
                .thenReturn("http://testServer:4040");
        request.setRequestURI("/kylin/driver_ui/project/step/job");
        request.setMethod("GET");
        jobDriverUIUtil.proxy("project", "step", request, response);
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any())).thenReturn(null);
        jobDriverUIUtil.proxy("project", "step", request, response);
        assert response.getContentAsString().contains("track url not generated yet");

        response = new MockHttpServletResponse();
        Mockito.when(jobInfoService.getOriginTrackUrlByProjectAndStepId(any(), any()))
                .thenReturn("http://testServer:4040");
        mockedSparkUIUtil.when(() -> SparkUIUtil.resendSparkUIRequest(any(), any(), eq("http://testServer:4040"),
                eq("/job"), eq("/kylin/driver_ui/project/step"))).thenThrow(UnknownHostException.class);
        jobDriverUIUtil.proxy("project", "step", request, response);
        assert response.getContentAsString().contains("track url invalid already");

        assert JobDriverUIUtil.getProxyUrl("project", "step").equals("/kylin/driver_ui/project/step");

    }
}
