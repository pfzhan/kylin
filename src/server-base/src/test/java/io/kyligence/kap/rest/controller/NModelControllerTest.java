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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.junit.After;
import org.junit.Assert;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeForWeb;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.IncrementBuildSegmentsRequest;
import io.kyligence.kap.rest.request.ModelCheckRequest;
import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelConfigRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentFixRequest;
import io.kyligence.kap.rest.request.SegmentTimeRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UnlinkModelRequest;
import io.kyligence.kap.rest.request.UpdateMultiPartitionValueRequest;
import io.kyligence.kap.rest.response.IndicesResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.ModelConfigResponse;
import io.kyligence.kap.rest.response.ModelSaveCheckResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
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

        mockMvc = MockMvcBuilders.standaloneSetup(nModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
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
    public void testGetModelRelations() throws Exception {
        Mockito.when(modelService.getModelRelations("model1", "default")).thenReturn(mockRelations());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/relations", "model1")
                        .contentType(MediaType.APPLICATION_JSON).param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelRelations("model1", "default");
    }

    @Test
    public void testGetModelSql() throws Exception {
        String sql = "SELECT * FROM TABLE1";
        Mockito.when(modelService.getModelSql("model1", "default")).thenReturn(sql);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/sql", "model1")
                        .contentType(MediaType.APPLICATION_JSON).param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelSql("model1", "default");
    }

    @Test
    public void testGetModelJson() throws Exception {
        String json = "testjson";
        Mockito.when(modelService.getModelJson("model1", "default")).thenReturn(json);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{model}/json", "model1")
                        .contentType(MediaType.APPLICATION_JSON).param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelJson("model1", "default");
    }

    @Test
    public void testTableIndices() throws Exception {
        Mockito.when(modelService.getTableIndices("model1", "default")).thenReturn(mockIndicesResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/table_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getTableIndices("model1", "default");
    }

    @Test
    public void testAggIndices() throws Exception {
        Mockito.when(modelService.getAggIndices("model1", "default", null, null, false, 0, 10, null, true))
                .thenReturn(mockIndicesResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/agg_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", "model1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getAggIndices("model1", "default", null, null, false, 0, 10,
                "last_modify_time", true);
    }

    @Test
    public void testGetIndicesById() throws Exception {
        IndexEntity index = new IndexEntity();
        index.setId(432323);
        index.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        IndicesResponse indices = new IndicesResponse(index.getIndexPlan());
        Mockito.when(modelService.getIndicesById("default", "model1", 432323L)).thenReturn(indices);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/agg_indices", "model1")
                .contentType(MediaType.APPLICATION_JSON).param("index", "432323").param("project", "default")
                .param("model", "model1").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getAggIndices("model1", "default", 432323L, null, false, 0, 10,
                "last_modify_time", true);
    }

    @Test
    public void testGetSegments() throws Exception {
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(432L, 2234L);
        Mockito.when(modelService.getSegmentsResponse("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "432", "2234",
                "", "end_time", true)).thenReturn(mockSegments());
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).param("offset", "0").param("project", "default")
                        .param("limit", "10").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                        .param("reverse", "true").param("status", "")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", "", 0, 10,
                "432", "2234", null, null, false, "end_time", true);
    }

    @Test
    public void testGetModels() throws Exception {

        Mockito.when(
                modelService.getModels("model1", "default", true, "ADMIN", Arrays.asList("NEW"), "last_modify", false))
                .thenReturn(mockModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "model1").param("limit", "10")
                .param("exact", "true").param("table", "").param("owner", "ADMIN").param("status", "NEW")
                .param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("model1", true, "default", "ADMIN", Arrays.asList("NEW"), "", 0, 10,
                "last_modify", true, null, null, null);
    }

    @Test
    public void testGetRelatedModels() throws Exception {

        Mockito.when(modelService.getRelateModels("default", "TEST_KYLIN_FACT", "model1"))
                .thenReturn(mockRelatedModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "model1").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("model1", true, "default", "ADMIN", Arrays.asList("NEW"),
                "TEST_KYLIN_FACT", 0, 10, "last_modify", true, null, null, null);
    }

    @Test
    public void testGetModelsWithOutModelName() throws Exception {
        Mockito.when(modelService.getModels("", "default", true, "ADMIN", Arrays.asList("NEW"), "last_modify", true))
                .thenReturn(mockModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("project", "default").param("model_name", "").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("", true, "default", "ADMIN", Arrays.asList("NEW"),
                "TEST_KYLIN_FACT", 0, 10, "last_modify", true, null, null, null);
    }

    @Test
    public void testRenameModel() throws Exception {
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/name", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelName(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testRenameModelException() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setNewModelName("newAlias)))&&&");
        Mockito.doNothing().when(modelService).renameDataModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "newAlias)))&&&");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/name", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).updateModelName(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    @Test
    public void testUpdateModelStatus() throws Exception {
        ModelUpdateRequest modelUpdateRequest = mockModelUpdateRequest();
        modelUpdateRequest.setStatus("DISABLED");
        Mockito.doNothing().when(modelService).updateDataModelStatus("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                "OFFLINE");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model}/status", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(mockModelUpdateRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelStatus(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelUpdateRequest.class));
    }

    private ModelUpdateRequest mockModelUpdateRequest() {
        ModelUpdateRequest updateRequest = new ModelUpdateRequest();
        updateRequest.setProject("default");
        updateRequest.setNewModelName("newAlias");
        updateRequest.setStatus("DISABLED");
        return updateRequest;
    }

    @Test
    public void testDeleteModel() throws Exception {
        Mockito.doNothing().when(modelService).dropModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model}", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        Mockito.doNothing().when(modelService).purgeModelManually("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default");
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .param("project", "default").param("purge", "true")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", true, false,
                null, null);
    }

    @Test
    public void testDeleteSegmentsByIds() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doNothing().when(modelService).deleteSegmentById("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default",
                request.getIds(), false);
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .param("project", "default").param("purge", "false")
                        .param("ids", "ef5e0663-feba-4ed2-b71c-21958122bbff")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).deleteSegments("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "default", false, false,
                request.getIds(), null);
    }

    @Test
    public void testRefreshSegmentsById() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo("78847556-2cdb-4b07-b39e-4c29856309aa",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        SegmentsRequest request = mockSegmentRequest();
        Mockito.doAnswer(x -> jobInfos).when(modelService).refreshSegmentById(Mockito.any());
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        String mvcResult = mockMvc
                .perform(MockMvcRequestBuilders
                        .put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn().getResponse().getContentAsString();
        Assert.assertTrue(mvcResult.contains("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        Mockito.verify(nModelController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testMergeSegments() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setType(SegmentsRequest.SegmentsRequestType.MERGE);
        request.setIds(new String[] { "0", "1" });
        Mockito.doAnswer(x -> new JobInfoResponse.JobInfo("0312bcc1-092e-42b1-ab0e-27807cf54f16",
                "79c27a68-343c-4b73-b406-dd5af0add951")).when(modelService).mergeSegmentsManually(Mockito.any());
        Mockito.doReturn(request.getIds()).when(modelService).convertSegmentIdWithName(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getProject(), request.getIds(), null);
        val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders
                        .put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn().getResponse().getContentAsString();
        Assert.assertTrue(mvcResult.contains("79c27a68-343c-4b73-b406-dd5af0add951"));
        Mockito.verify(nModelController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testMergeSegmentsException() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setType(SegmentsRequest.SegmentsRequestType.MERGE);
        Mockito.doReturn(new JobInfoResponse.JobInfo()).when(modelService).mergeSegmentsManually(
                new MergeSegmentParams("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getIds()));
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testRefreshSegmentsByIdException() throws Exception {
        SegmentsRequest request = mockSegmentRequest();
        request.setIds(null);
        Mockito.doAnswer(x -> null).when(modelService).refreshSegmentById(
                new RefreshSegmentParams("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getIds()));
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).refreshOrMergeSegments(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(SegmentsRequest.class));
    }

    private SegmentsRequest mockSegmentRequest() {
        SegmentsRequest segmentsRequest = new SegmentsRequest();
        segmentsRequest.setIds(new String[] { "ef5e0663-feba-4ed2-b71c-21958122bbff" });
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModel_DataRangeEndLessThanStart() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("1325347200000");
        request.setEnd("1293811200000");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCreateModel_DataRangeLessThan0() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1293811200000");
        Mockito.doReturn(null).when(modelService).createModel(request.getProject(), request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).createModel(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setNewModelName("new_model");
        request.setProject("default");
        Mockito.doNothing().when(modelService).cloneModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", "new_model",
                "default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model}/clone", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).cloneModel(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(ModelCloneRequest.class));
        request.setNewModelName("dsf gfdg fds");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model}/clone", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
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
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelDataCheckDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);

    }

    @Test
    public void testBuildSegments() throws Exception {
        BuildSegmentsRequest request1 = new BuildSegmentsRequest();
        request1.setProject("default");
        Mockito.doAnswer(x -> null).when(modelService).buildSegmentsManually("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "", "");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request1))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));

        IncrementBuildSegmentsRequest request2 = new IncrementBuildSegmentsRequest();
        request2.setProject("default");
        request2.setStart("100");
        request2.setEnd("200");
        request2.setPartitionDesc(new PartitionDesc());
        Mockito.doAnswer(x -> null).when(modelService).incrementBuildSegmentsManually("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request2.getStart(), request2.getEnd(),
                request2.getPartitionDesc(), request2.getSegmentHoles());
        mockMvc.perform(
                MockMvcRequestBuilders.put("/api/models/{model}/model_segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request2))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).incrementBuildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(IncrementBuildSegmentsRequest.class));
    }

    @Test
    public void testBuildSegments_DataRangeEndLessThanStart() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("100");
        request.setEnd("1");
        Mockito.doAnswer(x -> null).when(modelService).buildSegmentsManually("default", "nmodel_basci", "100", "1");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testBuildSegments_DataRangeLessThan0() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1");
        Mockito.doAnswer(x -> null).when(modelService).buildSegmentsManually("default", "nmodel_basci", "-1", "1");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segments", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).buildSegmentsManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildSegmentsRequest.class));
    }

    @Test
    public void testUpdateModelSemantics_DataRangeEndLessThanStart() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("100");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).updateSemantic(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testUpdateModelSemantics_DataRangeLessThan0() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("default");
        request.setStart("-1");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/semantic").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nModelController).updateSemantic(Mockito.any(ModelRequest.class));
    }

    @Test
    public void testBuildIndex() throws Exception {
        BuildIndexRequest request = new BuildIndexRequest();
        request.setProject("default");
        Mockito.doAnswer(x -> null).when(modelService).buildSegmentsManually("default", "nmodel_basci", "0", "100");
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/indices", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).buildIndicesManually(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(BuildIndexRequest.class));
    }

    @Test
    public void testUnlinkModel() throws Exception {
        UnlinkModelRequest request = new UnlinkModelRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService).unlinkModel("default", "nmodel_basci");
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/management_type", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).unlinkModel(eq("89af4ee2-2cdb-4b07-b39e-4c29856309aa"),
                Mockito.any(UnlinkModelRequest.class));
    }

    @Test
    public void testOfflineAllModelsInProject() throws Exception {
        Mockito.doNothing().when(modelService).offlineAllModelsInProject("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/disable_all_models").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).offlineAllModelsInProject("default");
    }

    @Test
    public void testOnlineAllModelsInProject() throws Exception {
        Mockito.doNothing().when(modelService).onlineAllModelsInProject("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/enable_all_models").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).onlineAllModelsInProject("default");
    }

    @Test
    public void testGetModelConfig() throws Exception {
        Mockito.doReturn(new ArrayList<ModelConfigResponse>()).when(modelService).getModelConfig("default", null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/config").param("project", "default")
                .param("model_name", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).updateModelConfig("89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
    }

    @Test
    public void testBatchSaveModels() throws Exception {
        ModelRequest request = new ModelRequest();
        Mockito.doNothing().when(modelService).batchCreateModel("gc_test", Mockito.spy(Lists.newArrayList(request)),
                Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/batch_save_models").param("project", "gc_test")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(Lists.newArrayList(request)))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).batchSaveModels(eq("gc_test"), Mockito.anyList());
    }

    @Test
    public void testSuggestModelWithReuseExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        SqlAccelerateRequest favoriteRequest = new SqlAccelerateRequest("gc_test", sqls, true);
        // reuse existed model
        Mockito.doReturn(null).when(modelService).suggestModel(favoriteRequest.getProject(), Mockito.spy(sqls), true,
                true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).suggestModel(Mockito.any());
    }

    @Test
    public void testSuggestModelWithoutReuseExistedModel() throws Exception {
        // don't reuse existed model
        String sql = "SELECT lstg_format_name, test_cal_dt.week_beg_dt, sum(price)\n" + "FROM test_kylin_fact\n"
                + "INNER JOIN edw.test_cal_dt AS test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "GROUP BY lstg_format_name, test_cal_dt.week_beg_dt";
        List<String> sqls = Lists.newArrayList(sql);
        SqlAccelerateRequest accerelateRequest = new SqlAccelerateRequest("gc_test", sqls, false);
        Mockito.doReturn(null).when(modelService).suggestModel(accerelateRequest.getProject(), Mockito.spy(sqls), false,
                true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(accerelateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).suggestModel(Mockito.any());
    }

    @Test
    public void test_api_can_answered_by_existed_model() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("gc_test", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/can_answered_by_existed_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testFixSegmentHole() throws Exception {
        SegmentFixRequest request = new SegmentFixRequest();
        request.setProject("default");
        SegmentTimeRequest timeRequest = new SegmentTimeRequest();
        timeRequest.setEnd("2");
        timeRequest.setStart("1");
        request.setSegmentHoles(Lists.newArrayList(timeRequest));
        mockMvc.perform(
                MockMvcRequestBuilders.post("/api/models/{model}/segment_holes", "e0e90065-e7c3-49a0-a801-20465ca64799")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).fixSegHoles(eq("e0e90065-e7c3-49a0-a801-20465ca64799"), eq(request));
    }

    @Test
    public void testCheckSegmentHoles() throws Exception {
        BuildSegmentsRequest request = new BuildSegmentsRequest();
        request.setProject("default");
        request.setStart("0");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/segment/validation", "e0e90065-e7c3-49a0-a801-20465ca64799")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelController).checkSegment(eq("e0e90065-e7c3-49a0-a801-20465ca64799"), eq(request));

    }

    private List<NSpanningTreeForWeb> mockRelations() {
        final List<NSpanningTreeForWeb> nSpanningTrees = new ArrayList<>();
        NSpanningTreeForWeb nSpanningTree = new NSpanningTreeForWeb(Maps.newHashMap(), new IndexPlan());
        nSpanningTrees.add(nSpanningTree);
        return nSpanningTrees;
    }

    private IndicesResponse mockIndicesResponse() {
        IndexEntity index = new IndexEntity();
        index.setId(1234);
        index.setIndexPlan(NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), "default")
                .getIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        IndicesResponse indices = new IndicesResponse(index.getIndexPlan());
        indices.addIndexEntity(index);
        return indices;
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

    @Test
    public void testCheckBeforeModelSave() {
        ModelRequest modelRequest = new ModelRequest();
        modelRequest.setProject("default");
        Mockito.doReturn(new ModelSaveCheckResponse()).when(modelService).checkBeforeModelSave(Mockito.any());
        nModelController.checkBeforeModelSave(modelRequest);
        Mockito.verify(nModelController).checkBeforeModelSave(modelRequest);
    }

    @Test
    public void testUpdateModelOwner() {
        String project = "default";
        String owner = "test";
        String modelId = UUID.randomUUID().toString();

        OwnerChangeRequest ownerChangeRequest = new OwnerChangeRequest();
        ownerChangeRequest.setProject(project);
        ownerChangeRequest.setOwner(owner);

        Mockito.doNothing().when(modelService).updateModelOwner(project, modelId, ownerChangeRequest);
        nModelController.updateModelOwner(modelId, ownerChangeRequest);
        Mockito.verify(nModelController).updateModelOwner(modelId, ownerChangeRequest);
    }

    @Test
    public void testBuildMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_BUILD.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        PartitionsBuildRequest param = new PartitionsBuildRequest();
        param.setProject("default");
        param.setSegmentId("73570f31-05a5-448f-973c-44209830dd01");
        param.setPartitionValues(Lists.newArrayList());
        Mockito.doReturn(new ModelSaveCheckResponse()).when(modelService).checkBeforeModelSave(Mockito.any());
        Mockito.doReturn(new JobInfoResponse()).when(modelService).buildSegmentPartitionByValue(param.getProject(), "", param.getSegmentId(),
                param.getPartitionValues(), param.isParallelBuildBySegment());
        Mockito.doNothing().when(modelService).validateCCType(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(param))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testRefreshMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_REFRESH.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);

        PartitionsRefreshRequest param = new PartitionsRefreshRequest();
        param.setProject("default");
        param.setSegmentId("73570f31-05a5-448f-973c-44209830dd01");

        Mockito.doReturn(response).when(modelService).refreshSegmentPartition(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(param))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testDeleteMultiPartition() throws Exception {
        List<JobInfoResponse.JobInfo> jobInfos = new ArrayList<>();
        jobInfos.add(new JobInfoResponse.JobInfo(JobTypeEnum.SUB_PARTITION_REFRESH.toString(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
        JobInfoResponse response = new JobInfoResponse();
        response.setJobs(jobInfos);
        Mockito.doReturn(response).when(modelService).refreshSegmentPartition(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/model_segments/multi_partition")
                .param("model", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default")
                .param("segment", "73570f31-05a5-448f-973c-44209830dd01")
                .param("ids", new String[]{"1", "2"})
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetMultiPartition() throws Exception {
        List<SegmentPartitionResponse> responses = Lists.newArrayList();
        Mockito.doReturn(responses).when(modelService)
                .getSegmentPartitions("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", "73570f31-05a5-448f-973c-44209830dd01",
                        Lists.newArrayList(), "last_modify_time", true);
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/models/{model}/model_segments/multi_partition", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default")
                .param("model", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("segment_id", "73570f31-05a5-448f-973c-44209830dd01")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateMultiPartitionMapping() throws Exception {
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService)
                .updateMultiPartitionMapping(request.getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/multi_partition/mapping", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetMultiPartitionValues() throws Exception {
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService)
                .updateMultiPartitionMapping(request.getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders
                .put("/api/models/{model}/multi_partition/mapping", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAddMultiPartitionValues() throws Exception {
        UpdateMultiPartitionValueRequest request = new UpdateMultiPartitionValueRequest();
        request.setProject("default");
        Mockito.doNothing().when(modelService)
                .addMultiPartitionValues(request.getProject(), "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request.getValues());
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model}/multi_partition/values", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testDeleteMultiPartitionValuess() throws Exception {
        Mockito.doNothing().when(modelService)
                .deletePartitions("default", null, "89af4ee2-2cdb-4b07-b39e-4c29856309aa", Sets.newHashSet(1L, 2L));
        mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/models/{model}/multi_partition/values", "89af4ee2-2cdb-4b07-b39e-4c29856309aa")
                .param("project", "default")
                .param("ids", new String[]{"1", "2"})
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}