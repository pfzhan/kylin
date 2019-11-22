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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.v2.NCubesControllerV2;
import io.kyligence.kap.rest.request.CubeRebuildRequest;
import io.kyligence.kap.rest.request.SegmentMgmtRequest;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.ModelService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.Segments;
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static org.mockito.ArgumentMatchers.eq;

public class NCubesControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private NCubesControllerV2 nCubesControllerV2 = Mockito.spy(new NCubesControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nCubesControllerV2)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(UUID.randomUUID().toString());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private List<NDataModelResponse> mockModels() {
        final List<NDataModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        model.setProject("default");
        NDataModelResponse modelResponse = new NDataModelResponse(model);
        NDataSegmentResponse segmentResponse1 = new NDataSegmentResponse();
        segmentResponse1.setId("seg1");
        segmentResponse1.setName("test_seg1");
        NDataSegmentResponse segmentResponse2 = new NDataSegmentResponse();
        segmentResponse2.setId("seg2");
        segmentResponse2.setName("test_seg2");
        modelResponse.setSegments(Lists.newArrayList(segmentResponse1, segmentResponse2));
        models.add(modelResponse);
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        NDataModelResponse model2Response = new NDataModelResponse(model1);
        model2Response.setSegments(Lists.newArrayList());
        models.add(model2Response);
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

    @Test
    public void testGetCubes() throws Exception {
        Mockito.when(modelService.getCubes("model1", "default")).thenReturn(mockModels());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/cubes").contentType(MediaType.APPLICATION_JSON)
                .param("pageOffset", "0").param("projectName", "default").param("modelName", "model1")
                .param("pageSize", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nCubesControllerV2).getCubes("default", "model1", 0, 10);
    }

    @Test
    public void testGetCube() throws Exception {
        Mockito.when(modelService.getCube("model1", "default")).thenReturn(mockModels().get(0));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/cubes/{cubeName}", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nCubesControllerV2).getCube("model1", "default");
    }

    @Test
    public void testRebuild() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        String startTime = String.valueOf(0L);
        String endTime = String.valueOf(Long.MAX_VALUE - 1);
        Mockito.doNothing().when(modelService).buildSegmentsManually("default", "model1", startTime, endTime);

        CubeRebuildRequest rebuildRequest = new CubeRebuildRequest();
        rebuildRequest.setBuildType("BUILD");
        rebuildRequest.setStartTime(0L);
        rebuildRequest.setEndTime(Long.MAX_VALUE - 1);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/rebuild", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(rebuildRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nCubesControllerV2).rebuild(eq("model1"), eq(null), Mockito.any(CubeRebuildRequest.class));
    }

    @Test
    public void testManageSegments() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(0));
        Mockito.doNothing().when(modelService).mergeSegmentsManually("model1", "default",
                new String[] { "seg1", "seg2" });

        SegmentMgmtRequest request = new SegmentMgmtRequest();
        request.setBuildType("MERGE");
        request.setSegments(Lists.newArrayList("test_seg1", "test_seg2"));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/cubes/{cubeName}/segments", "model1")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nCubesControllerV2).manageSegments(eq("model1"), eq(null),
                Mockito.any(SegmentMgmtRequest.class));
    }

    @Test
    public void testGetHoles() throws Exception {
        Mockito.when(modelService.getCube("model1", null)).thenReturn(mockModels().get(1));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/cubes/{cubeName}/holes", "model1")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nCubesControllerV2).getHoles("model1", null);
    }
}
