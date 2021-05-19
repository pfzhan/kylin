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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.rest.request.StreamingJobExecuteRequest;
import io.kyligence.kap.rest.request.StreamingJobParamsRequest;
import io.kyligence.kap.rest.service.StreamingJobService;
import io.kyligence.kap.streaming.request.LayoutUpdateRequest;
import io.kyligence.kap.streaming.request.SegmentMergeRequest;
import io.kyligence.kap.streaming.request.StreamingJobStatsRequest;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclEvaluate;
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

import java.util.Arrays;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

public class StreamingJobControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private StreamingJobService streamingJobService;


    @InjectMocks
    private StreamingJobController streamingJobController = Mockito.spy(new StreamingJobController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static String PROJECT = "streaming_test";
    private static String MODEL_ID = "e78a89dd-847f-4574-8afa-8768b4228b72";
    private static String DATAFLOW_ID = MODEL_ID;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(streamingJobController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

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
    public void testUpdateStreamingJobStatus() throws Exception {
        val request = new StreamingJobExecuteRequest();
        request.setProject(PROJECT);
        request.setAction("START");
        request.setModelId(MODEL_ID);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobStatus(Mockito.any(StreamingJobExecuteRequest.class));
    }

    @Test
    public void testUpdateStreamingJobParams() throws Exception {
        val request = new StreamingJobParamsRequest();
        request.setProject(PROJECT);
        request.setModelId(MODEL_ID);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/params").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobParams(Mockito.any(StreamingJobParamsRequest.class));
    }

    @Test
    public void testCollectStreamingJobStats() throws Exception {
        val request = new StreamingJobStatsRequest();
        request.setProject(PROJECT);
        String job_id = "abcef";
        request.setJobId(job_id);
        Long batch_row_num = 1234532L;
        request.setBatchRowNum(batch_row_num);
        Double rows_per_second = 123.32;
        request.setRowsPerSecond(rows_per_second);
        Long duration_ms = 12222L;
        request.setDurationMs(duration_ms);
        Long trigger_start_time = 999L;
        request.setTriggerStartTime(trigger_start_time);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/stats").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).collectStreamingJobStats(Mockito.any(StreamingJobStatsRequest.class));
    }

    @Test
    public void testUpdateStreamingJobInfo() throws Exception {
        val request = new StreamingJobUpdateRequest();
        request.setProject(PROJECT);
        request.setModelId(MODEL_ID);
        request.setProcessId("9921");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/spark").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateStreamingJobInfo(Mockito.any(StreamingJobUpdateRequest.class));
    }

    @Test
    public void testGetStreamingJobOfBuild() throws Exception {
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("model_id", MODEL_ID).param("job_type", "BUILD")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(streamingJobController).getStreamingJob(PROJECT, MODEL_ID);
    }

    @Test
    public void testGetStreamingJobOfMerge() throws Exception {
        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/streaming_jobs").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("model_id", MODEL_ID).param("job_type", "MERGE")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(streamingJobController).getStreamingJob(PROJECT, MODEL_ID);
    }

    @Test
    public void testAddSegment() throws Exception {
        val request = new SegmentMergeRequest();
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        request.setSegmentRange(new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 1L,
                createKafkaPartitionOffset(0, 100L), createKafkaPartitionOffset(0, 200L)));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_jobs/dataflow/segment").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).addSegment(Mockito.any(SegmentMergeRequest.class));
    }

    @Test
    public void testUpdateSegment() throws Exception {
        val request = new SegmentMergeRequest();
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        request.setNewSegId("c380dd2a-43b8-4268-b73d-2a5f76236638");
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        request.setRemoveSegment(df.getSegments());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/dataflow/segment").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateSegment(Mockito.any(SegmentMergeRequest.class));
    }

    @Test
    public void testDeleteSegment() throws Exception {
        val request = new SegmentMergeRequest();
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);
        request.setRemoveSegment(df.getSegments());

        Mockito.doNothing().when(streamingJobService).deleteSegment(Mockito.anyString(), Mockito.anyString(), Mockito.anyList());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/streaming_jobs/dataflow/segment/deletion").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).deleteSegment(Mockito.any(SegmentMergeRequest.class));
    }

    @Test
    public void testUpdateLayout() throws Exception {
        val request = new LayoutUpdateRequest();
        request.setProject(PROJECT);
        request.setDataflowId(DATAFLOW_ID);

        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, PROJECT);
        NDataflow df = mgr.getDataflow(DATAFLOW_ID);

        Assert.assertTrue(df.getSegments().size() > 0);
        val segDetails = df.getSegments().getFirstSegment().getSegDetails();
        request.setSegDetails(Arrays.asList(segDetails));
        val layouts = segDetails.getLayouts();
        request.setLayouts(layouts);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/streaming_jobs/dataflow/layout").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(streamingJobController).updateLayout(Mockito.any(LayoutUpdateRequest.class));

    }
}
