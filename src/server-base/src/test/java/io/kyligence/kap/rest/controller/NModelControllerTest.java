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
import java.util.UUID;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.response.IndexEntityResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.NSpanningTreeResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.service.ModelService;
import lombok.val;

public class NModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private NModelController nModelController = Mockito.spy(new NModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nModelController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

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

    @Test
    public void testGetModelRelations() throws Exception {
        Mockito.when(modelService.getSimplifiedModelRelations("model1", "default")).thenReturn(mockRelations());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/relations").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelRelations("model1", "default");
    }

    @Test
    public void testGetModelJson() throws Exception {
        String json = "testjson";
        Mockito.when(modelService.getModelJson("model1", "default")).thenReturn(json);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/json").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelJson("model1", "default");
    }

    @Test
    public void testTableIndices() throws Exception {
        Mockito.when(modelService.getTableIndices("model1", "default")).thenReturn(mockCuboidDescs());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/table_indices").contentType(MediaType.APPLICATION_JSON)
                .param("model", "model1").param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getTableIndices("model1", "default");
    }

    @Test
    public void testAggIndexs() throws Exception {
        Mockito.when(modelService.getAggIndices("model1", "default")).thenReturn(mockCuboidDescs());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/agg_indices").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getAggIndices("model1", "default");
    }

    @Test
    public void testGetCuboids() throws Exception {
        IndexEntity cuboidDesc = new IndexEntity();
        cuboidDesc.setId(432323);
        cuboidDesc.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        IndexEntityResponse indexEntityResponse = new IndexEntityResponse(cuboidDesc);
        Mockito.when(modelService.getCuboidById("model1", "default", 432323L)).thenReturn(indexEntityResponse);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/cuboids").contentType(MediaType.APPLICATION_JSON)
                        .param("id", "432323").param("project", "default").param("model", "model1")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getCuboids(432323L, "default", "model1");
    }

    @Test
    public void testGetCuboidsException() throws Exception {

        Mockito.when(modelService.getCuboidById("model1", "default", 432323L)).thenReturn(null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/cuboids").contentType(MediaType.APPLICATION_JSON)
                .param("id", "432323").param("project", "default").param("model", "model1")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).getCuboids(432323L, "default", "model1");
    }

    @Test
    public void testGetSegments() throws Exception {
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(432L, 2234L);
        Mockito.when(modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "432", "2234",
                "end_time", true, "")).thenReturn(mockSegments());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("limit", "10").param("start", "432").param("end", "2234").param("sortBy", "end_time")
                .param("reverse", "true").param("status", "")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "", 0, 10,
                "432", "2234", "end_time", true);
    }

    @Test
    public void testGetModels() throws Exception {

        Mockito.when(modelService.getModels("model1", "default", true, "ADMIN", "NEW", "last_modify", false))
                .thenReturn(mockModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model", "model1").param("limit", "10")
                .param("exact", "true").param("table", "").param("owner", "ADMIN").param("status", "NEW")
                .param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("model1", true, "default", "ADMIN", "NEW", "", 0, 10, "last_modify",
                true);
    }

    @Test
    public void testGetRelatedModels() throws Exception {

        Mockito.when(modelService.getRelateModels("default", "TEST_KYLIN_FACT", "model1"))
                .thenReturn(mockRelatedModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model", "model1").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("model1", true, "default", "ADMIN", "NEW", "TEST_KYLIN_FACT", 0, 10,
                "last_modify", true);
    }

    @Test
    public void testGetModelsWithOutModelName() throws Exception {
        Mockito.when(modelService.getModels("", "default", true, "ADMIN", "NEW", "last_modify", true))
                .thenReturn(mockModels());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                        .param("offset", "0").param("project", "default").param("model", "").param("limit", "10")
                        .param("exact", "true").param("owner", "ADMIN").param("status", "NEW")
                        .param("sortBy", "last_modify").param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("", true, "default", "ADMIN", "NEW", "TEST_KYLIN_FACT", 0, 10,
                "last_modify", true);
    }

    @Test
    public void testRenameModel() throws Exception {
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/name").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelName(Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testRenameModelException() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setNewModelName("newAlias)))&&&");
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias)))&&&");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/name").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).updateModelName(Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testUpdateModelStatus() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setStatus("DISABLED");
        Mockito.doNothing().when(modelService).updateDataModelStatus("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "OFFLINE");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelStatus(Mockito.any(ModelUpdateRequest.class));
    }

    private ModelUpdateRequest mockModelUpdateRequest() {
        ModelUpdateRequest updateRequest = new ModelUpdateRequest();
        updateRequest.setProject("default");
        updateRequest.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        updateRequest.setNewModelName("newAlias");
        updateRequest.setStatus("DISABLED");
        return updateRequest;
    }

    @Test
    public void testDeleteModel() throws Exception {
        Mockito.doNothing().when(modelService).dropModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/{project}/{model}", "default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        Mockito.doNothing().when(modelService).purgeModelManually("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/segments/{project}/{model}", "default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteSegments("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", null);
    }

    @Test
    public void testDeleteSegmentsByIds() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doNothing().when(modelService).deleteSegmentById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                request.getIds());
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/segments/{project}/{model}", "default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("ids", "ef5e0663-feba-4ed2-b71c-21958122bbff")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteSegments("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                request.getIds());
    }

    @Test
    public void testRefreshSegmentsById() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doNothing().when(modelService).refreshSegmentById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                request.getIds());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).refreshSegmentsByIds(Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testRefreshSegmentsByIdException() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setIds(null);
        Mockito.doNothing().when(modelService).refreshSegmentById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                request.getIds());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).refreshSegmentsByIds(Mockito.any(SegmentsRequest.class));
    }

    private SegmentsRequest mockSegmentRequest() {
        SegmentsRequest segmentsRequest = new SegmentsRequest();
        segmentsRequest.setIds(new String[] { "ef5e0663-feba-4ed2-b71c-21958122bbff" });
        segmentsRequest.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        segmentsRequest.setProject("default");
        return segmentsRequest;
    }

    @Test
    public void testCreateModel() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModel_PartitionColumnNotExistException() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setPartitionDesc(new PartitionDesc());
        request.setProject("default");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setNewModelName("new_model");
        request.setProject("default");
        Mockito.doNothing().when(modelService).cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_model",
                "default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/clone").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).cloneModel(Mockito.any(ModelCloneRequest.class));
        request.setNewModelName("dsf gfdg fds");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/clone").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
    }

    @Test
    public void testUpdateModelDataCheckDesc() throws Exception {
        final ModelCheckRequest request = new ModelCheckRequest();
        request.setProject("default");
        request.setCheckOptions(7);
        request.setFaultThreshold(10);
        request.setFaultActions(2);
        Mockito.doNothing().when(modelService).updateModelDataCheckDesc("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 7, 10, 2);
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{name}/data_check", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelDataCheckDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);

    }

    @Test
    public void testBuildSegments() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setProject("default");
        request.setStart("0");
        request.setEnd("100");
        Mockito.doNothing().when(modelService).buildSegmentsManually("default", "nmodel_basci", "0", "100");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).buildSegmentsManually(Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testUnlinkModel() throws Exception {
        UnlinkModelRequest request = new UnlinkModelRequest();
        request.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setProject("default");
        Mockito.doNothing().when(modelService).unlinkModel("default", "nmodel_basci");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/management_type")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).unlinkModel(Mockito.any(UnlinkModelRequest.class));
    }

    @Test
    public void testGetModelInfo() throws Exception {
        Mockito.doReturn(null).when(modelService).getModelInfo("*", "*", "*", 0, 0);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/model_info").param("suite", "*").param("model", "*")
                .param("project", "*").param("start", "0").param("end", "0")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).getModelInfo("*", "*", "*", 0, 0);
    }

    @Test
    public void testGetModelConfig() throws Exception {
        Mockito.doReturn(new ArrayList<ModelConfigResponse>()).when(modelService).getModelConfig("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("project", "default").param("model", "")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).getModelConfig("", "default", 0, 10);
    }

    @Test
    public void testUpdateModelConfig() throws Exception {
        val request = new ModelConfigRequest();
        request.setAutoMergeEnabled(false);
        request.setProject("default");
        Mockito.doNothing().when(modelService).updateModelConfig("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelConfig("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
    }

    private List<NSpanningTreeResponse> mockRelations() {
        final List<NSpanningTreeResponse> nSpanningTrees = new ArrayList<>();
        NSpanningTreeResponse nSpanningTree = new NSpanningTreeResponse();
        nSpanningTrees.add(nSpanningTree);
        return nSpanningTrees;
    }

    private List<IndexEntityResponse> mockCuboidDescs() {
        final List<IndexEntityResponse> nCuboidDescs = new ArrayList<>();
        IndexEntity cuboidDesc = new IndexEntity();
        cuboidDesc.setId(1234);
        cuboidDesc.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        nCuboidDescs.add(new IndexEntityResponse(cuboidDesc));
        return nCuboidDescs;
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

    private List<RelatedModelResponse> mockRelatedModels() {
        final List<RelatedModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new RelatedModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        models.add(new RelatedModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new RelatedModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new RelatedModelResponse(model3));

        return models;
    }

}
