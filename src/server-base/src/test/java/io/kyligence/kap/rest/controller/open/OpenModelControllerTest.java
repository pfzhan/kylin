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

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.mockito.ArgumentMatchers.eq;

public class OpenModelControllerTest {

    private MockMvc mockMvc;

    @Mock
    private NModelController nModelController;

    @InjectMocks
    private OpenModelController openModelController = Mockito.spy(new OpenModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

    }

    @After
    public void tearDown() {
    }

    private List<NDataModel> mockModels() {
        final List<NDataModel> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new NDataModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        models.add(new NDataModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(UUID.randomUUID().toString());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private void mockGetModelName(String modelName, String project, String modelId) {
        NDataModelResponse model = new NDataModelResponse();
        model.setUuid(modelId);
        Mockito.doReturn(model).when(openModelController).getModel(modelName, project);
    }

    @Test
    public void testGetModels() throws Exception {
        Mockito.when(
                nModelController.getModels("model1", true, "default", "ADMIN", "NEW", "", 1, 5, "last_modify", false))
                .thenReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(mockModels(), 0, 10), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/open/models").contentType(MediaType.APPLICATION_JSON)
                .param("page_offset", "1").param("project", "default").param("model_name", "model1")
                .param("page_size", "5").param("exact", "true").param("table", "").param("owner", "ADMIN")
                .param("status", "NEW").param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openModelController).getModels("default", "model1", true, "ADMIN", "NEW", "", 1, 5,
                "last_modify", true);
    }

    @Test
    public void testGetSegments() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);
        Mockito.when(nModelController.getSegments(modelId, project, "", 1, 5, "432", "2234", "end_time", true))
                .thenReturn(
                        new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(mockSegments(), 1, 5), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/open/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("page_offset", "1").param("project", project)
                .param("page_size", "5").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                .param("reverse", "true").param("status", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(openModelController).getSegments(modelName, project, "", 1, 5, "432", "2234", "end_time", true);
    }

    @Test
    public void testBuildSegments() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("0");
        request.setEnd("100");
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nModelController)
                .buildSegmentsManually(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/open/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).buildSegmentsManually(eq(modelName),
                Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testRefreshSegmentsById() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        SegmentsRequest request = new SegmentsRequest();
        request.setProject(project);
        request.setType(SegmentsRequest.SegmentsRequestType.REFRESH);
        request.setIds(new String[] { "1", "2" });
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nModelController)
                .refreshOrMergeSegmentsByIds(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/open/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).refreshOrMergeSegmentsByIds(eq(modelName),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nModelController)
                .deleteSegments(modelId, project, true, null);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/open/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", true, null);
    }

    @Test
    public void testDeleteSegmentsByIds() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        SegmentsRequest request = new SegmentsRequest();
        request.setIds(new String[] { "1", "2" });
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nModelController)
                .deleteSegments(modelId, project, false, request.getIds());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/open/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "false").param("ids", request.getIds())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", false, request.getIds());
    }

}
