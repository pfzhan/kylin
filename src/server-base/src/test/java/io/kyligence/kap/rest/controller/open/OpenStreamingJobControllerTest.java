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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;

import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.service.StreamingJobService;
import lombok.val;

public class OpenStreamingJobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private StreamingJobService streamingJobService = Mockito.spy(StreamingJobService.class);

    @InjectMocks
    private OpenStreamingJobController streamingJobController = Mockito.spy(new OpenStreamingJobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(streamingJobController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(streamingJobController, "streamingJobService", streamingJobService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetStreamingJobList() throws Exception {
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs")
                .contentType(MediaType.APPLICATION_JSON).param("model_name", StringUtils.EMPTY)
                .param("model_names", StringUtils.EMPTY).param("job_types", StringUtils.EMPTY)
                .param("statuses", StringUtils.EMPTY).param("project", PROJECT).param("page_offset", "0")
                .param("page_size", "10").param("sort_by", "last_modified").param("reverse", "true")
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        MvcResult mvcResult = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        Mockito.verify(streamingJobController).getStreamingJobList(StringUtils.EMPTY, Collections.EMPTY_LIST,
                Collections.EMPTY_LIST, Collections.EMPTY_LIST, PROJECT, 0, 10, "last_modified", true,
                Collections.EMPTY_LIST);
    }

    @Test
    public void testUpdateStreamingJobStatus() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setJobIds(
                Collections.singletonList(StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name())));
        val mockRequestBuilder = MockMvcRequestBuilders.put("/api/streaming_jobs/status")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(streamingJobController).updateStreamingJobStatus(Mockito.any(StreamingJobExecuteRequest.class));
    }

    /**
     * test for empty job_ids parameters
     * @throws Exception
     */
    @Test
    public void testUpdateStreamingJobStatus_EmptyJobIds() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setJobIds(Collections.emptyList());
        val mockRequestBuilder = MockMvcRequestBuilders.put("/api/streaming_jobs/status")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        val errMsg = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andReturn().getResolvedException().getMessage();
        Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("job_ids"), errMsg);
    }

    @Test
    public void testGetStreamingJobDataStats() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/stats/" + jobId)
                .contentType(MediaType.APPLICATION_JSON).accept(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobDataStats(jobId, 30);
    }

    @Test
    public void testGetStreamingJobRecordList() throws Exception {
        val jobId = StreamingUtils.getJobId(MODEL_ID, JobTypeEnum.STREAMING_BUILD.name());
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/records")
                .contentType(MediaType.APPLICATION_JSON).param("job_id", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON));
        mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(streamingJobController).getStreamingJobRecordList(jobId);
    }

    @Test
    public void testGetStreamingJobRecordList_EmptyJobId() throws Exception {
        val jobId = "";
        val mockRequestBuilder = MockMvcRequestBuilders.get("/api/streaming_jobs/records")
                .contentType(MediaType.APPLICATION_JSON).param("job_id", jobId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON));
        val errMsg = mockMvc.perform(mockRequestBuilder).andExpect(MockMvcResultMatchers.status().is5xxServerError())
                .andReturn().getResolvedException().getMessage();
        Assert.assertEquals(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("job_id"), errMsg);
    }
}
