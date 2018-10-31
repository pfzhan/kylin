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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.service.ModelService;
import org.apache.kylin.common.util.JsonUtil;
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

import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

public class NModelControllerTest {

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

    @After
    public void tearDown() {
    }

    @Test
    public void testGetModelRelations() throws Exception {
        Mockito.when(modelService.getModelRelations("model1", "default")).thenReturn(mockRelations());
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
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(432323);
        CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
        Mockito.when(modelService.getCuboidById("model1", "default", 432323L)).thenReturn(cuboidDescResponse);
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
        Mockito.when(modelService.getSegments("nmodel_basic", "default", "432", "2234")).thenReturn(mockSegments());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model", "nmodel_basic").param("limit", "10")
                .param("start", "432").param("end", "2234")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getSegments("nmodel_basic", "default", 0, 10, "432", "2234");
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

        Mockito.when(modelService.getRelateModels("default", "TEST_KYLIN_FACT", "model1")).thenReturn(mockModels());
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
        Mockito.doNothing().when(modelService).renameDataModel("default", "nmodel_basic", "newAlias");
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
        Mockito.doNothing().when(modelService).renameDataModel("default", "nmodel_basic", "newAlias)))&&&");
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
        Mockito.doNothing().when(modelService).updateDataModelStatus("default", "nmodel_basic", "DISABLED");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/status").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelStatus(Mockito.any(ModelUpdateRequest.class));
    }

    private ModelUpdateRequest mockModelUpdateRequest() {
        ModelUpdateRequest updateRequest = new ModelUpdateRequest();
        updateRequest.setProject("default");
        updateRequest.setModelName("nmodel_basic");
        updateRequest.setNewModelName("newAlias");
        updateRequest.setStatus("DISABLED");
        return updateRequest;
    }

    @Test
    public void testDeleteModel() throws Exception {
        Mockito.doNothing().when(modelService).dropModel("nmodel_basic", "default");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{project}/{model}", "default", "nmodel_basic")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteModel("default", "nmodel_basic");
    }

    @Test
    public void testPurgeModel() throws Exception {
        Mockito.doNothing().when(modelService).purgeModel("nmodel_basic", "default");
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/models/segments/{project}/{model}", "default", "nmodel_basic")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andDo(print());
        Mockito.verify(nModelController).purgeModel("default", "nmodel_basic");
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setModelName("nmodel_basic");
        request.setNewModelName("new_model");
        request.setProject("default");
        Mockito.doNothing().when(modelService).cloneModel("nmodel_basic", "new_model", "default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).cloneModel(Mockito.any(ModelCloneRequest.class));
        request.setNewModelName("dsf gfdg fds");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
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
        Mockito.doNothing().when(modelService).updateModelDataCheckDesc("default", "nmodel_basic", 7, 10, 2);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{name}/data_check", "nmodel_basic")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelDataCheckDesc("nmodel_basic", request);

    }

    private List<NForestSpanningTree> mockRelations() {
        final List<NForestSpanningTree> nSpanningTrees = new ArrayList<>();
        Map<NCuboidDesc, Collection<NCuboidLayout>> cuboids = new HashMap<>();
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(1234);
        List<NCuboidLayout> layouts = new ArrayList<>();
        NCuboidLayout nCuboidLayout = new NCuboidLayout();
        nCuboidLayout.setId(12345);
        layouts.add(nCuboidLayout);
        cuboids.put(cuboidDesc, layouts);
        NForestSpanningTree nSpanningTree = new NForestSpanningTree(cuboids, "test");
        nSpanningTrees.add(nSpanningTree);
        return nSpanningTrees;
    }

    private List<CuboidDescResponse> mockCuboidDescs() {
        final List<CuboidDescResponse> nCuboidDescs = new ArrayList<>();
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(1234);
        nCuboidDescs.add(new CuboidDescResponse(cuboidDesc));
        return nCuboidDescs;
    }

    private Segments<NDataSegment> mockSegments() {
        final Segments<NDataSegment> nDataSegments = new Segments<NDataSegment>();
        NDataSegment segment = new NDataSegment();
        segment.setId(1);
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private List<NDataModelResponse> mockModels() {
        final List<NDataModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setName("model1");
        models.add(new NDataModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setName("model2");
        models.add(new NDataModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setName("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setName("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

}
