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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.kyligence.kap.tool.bisync.SyncContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.MultiPartitionDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.CheckSegmentRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.rest.request.MultiPartitionMappingRequest;
import io.kyligence.kap.rest.request.PartitionColumnRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.request.UpdateMultiPartitionValueRequest;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.OpenGetIndexResponse;
import io.kyligence.kap.rest.response.OpenValidationResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.service.FusionIndexService;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.RawRecService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelCreateContextOfSemiV2;
import io.kyligence.kap.smart.ModelReuseContextOfSemiV2;
import io.kyligence.kap.smart.ModelSelectContextOfSemiV2;
import lombok.val;
import lombok.var;

public class OpenModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NModelController nModelController;

    @Mock
    private IndexPlanService indexPlanService;

    @Mock
    private FusionIndexService fusionIndexService;

    @Mock
    private ModelService modelService;

    @Mock
    private ModelSmartService modelSmartService;

    @Mock
    private FusionModelService fusionModelService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private RawRecService rawRecService;

    @InjectMocks
    private OpenModelController openModelController = Mockito.spy(new OpenModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        Mockito.doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
        Mockito.doNothing().when(rawRecService).transferAndSaveRecommendations(Mockito.any());
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private List<NDataModel> mockModels() {
        final List<NDataModel> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new NDataModelResponse(model));
        NDataModel model1 = new NDataModel();
        model1.setUuid("model2");
        models.add(new NDataModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model2.setUuid("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model3.setUuid("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

    private Segments<NDataSegmentResponse> mockSegments() {
        final Segments<NDataSegmentResponse> nDataSegments = new Segments<>();
        NDataSegmentResponse segment = new NDataSegmentResponse();
        segment.setId(RandomUtil.randomUUIDStr());
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private NDataModelResponse mockGetModelName(String modelName, String project, String modelId) {
        NDataModelResponse model = new NDataModelResponse();
        model.setUuid(modelId);
        Mockito.doReturn(model).when(openModelController).getModel(modelName, project);
        return model;
    }

    @Test
    public void testGetModels() throws Exception {
        Mockito.when(nModelController.getModels("model1", "model1", true, "default", "ADMIN", Arrays.asList("NEW"), "",
                1, 5, "last_modify", false, null, null, null, null, true)).thenReturn(
                        new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(mockModels(), 0, 10), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("page_offset", "1").param("project", "default").param("model_id", "model1")
                .param("model_name", "model1").param("page_size", "5").param("exact", "true").param("table", "")
                .param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openModelController).getModels("default", "model1", "model1", true, "ADMIN",
                Arrays.asList("NEW"), "", 1, 5, "last_modify", true, null, null, null, true);
    }

    @Test
    public void testGetSegments() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);
        Mockito.when(nModelController.getSegments(modelId, project, "", 1, 5, "432", "2234", null, null, false,
                "end_time", true)).thenReturn(
                        new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(mockSegments(), 1, 5), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("page_offset", "1").param("project", project)
                .param("page_size", "5").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                .param("reverse", "true").param("status", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(openModelController).getSegments(modelName, project, "", 1, 5, "432", "2234", "end_time", true);
    }

    @Test
    public void testGetIndexes() throws Exception {
        List<Long> ids = Lists.newArrayList(1L, 20000020001L);
        String project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String modelName = "default_model_name";
        NDataModel model = new NDataModel();
        model.setUuid(modelId);
        model.setAlias(modelName);
        val modelManager = Mockito.mock(NDataModelManager.class);
        Mockito.when(modelService.getDataModelManager(Mockito.anyString())).thenReturn(modelManager);
        Mockito.when(modelManager.listAllModels()).thenReturn(Lists.newArrayList(model));

        Mockito.when(fusionIndexService.getIndexesWithRelatedTables(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(getIndexResponses());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/indexes", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        OpenGetIndexResponse response = openModelController.getIndexes(project, modelName, null, 0, 10, //
                null, "last_modified", "", true, ids).getData();
        Assert.assertArrayEquals(response.getAbsentBatchIndexIds().toArray(),
                Lists.newArrayList(20000020001L).toArray());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/indexes", modelId)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model}/indexes", modelId + modelName)
                .contentType(MediaType.APPLICATION_JSON).param("project", project) //
                .param("batch_index_ids", "1,20000020001")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

    }

    private List<IndexResponse> getIndexResponses() throws Exception {
        IndexResponse index = new IndexResponse();
        index.setId(1L);
        index.setRelatedTables(Lists.newArrayList("table1", "table2"));
        return Lists.newArrayList(index);
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
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .buildSegmentsManually(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
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
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .refreshOrMergeSegments(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).refreshOrMergeSegments(eq(modelName), Mockito.any(SegmentsRequest.class));
    }

    @Test
    public void testCompleteSegments() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        String[] ids = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(project);
        req.setParallelBuildBySegment(false);
        req.setSegmentIds(Lists.newArrayList(ids));
        mockGetModelName(modelName, project, modelId);
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .addIndexesToSegments(modelId, req);
        Mockito.doReturn(new Pair("model_id", ids)).when(fusionModelService).convertSegmentIdWithName(modelId, project,
                ids, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                .param("project", "default") //
                .param("parallel", "false") //
                .param("ids", ids) //
                .param("names", (String) null) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).completeSegments(modelName, project, false, ids, null, null, false, 3, null,
                null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                .param("project", "default") //
                .param("parallel", "false") //
                .param("ids", ids) //
                .param("names", (String) null) //
                .param("priority", "0").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).completeSegments(modelName, project, false, ids, null, null, false, 0, null,
                null);
    }

    @Test
    public void testCompleteSegmentsPartialBuild() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        String[] ids = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        Pair pair = new Pair<>(modelId, ids);
        List<Long> batchIndexIds = Lists.newArrayList(1L, 2L);
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(project);
        req.setParallelBuildBySegment(false);
        req.setSegmentIds(Lists.newArrayList(ids));
        mockGetModelName(modelName, project, modelId);
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .addIndexesToSegments(modelId, req);
        Mockito.doReturn(pair).when(fusionModelService).convertSegmentIdWithName(modelId, project, ids, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                .param("project", "default") //
                .param("parallel", "false") //
                .param("ids", ids) //
                .param("names", (String) null) //
                .param("partial_build", "true") //
                .param("batch_index_ids", "1,2") //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).completeSegments(modelName, project, false, ids, null, batchIndexIds, true,
                3, null, null);
    }

    @Test
    public void testCompleteSegmentsWithoutIdsAndNames() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(project);
        req.setParallelBuildBySegment(false);
        mockGetModelName(modelName, project, modelId);
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .addIndexesToSegments(modelId, req);
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                        .param("project", "default") //
                        .param("parallel", "false") //
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        String contentAsString = result.getResponse().getContentAsString();
        Assert.assertTrue(contentAsString.contains(MsgPicker.getMsg().getEMPTY_SEGMENT_PARAMETER()));
    }

    @Test
    public void testCompleteSegmentsWithIdsAndNames() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        String[] ids = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        String[] names = { "ef5e0663-feba-4ed2-b71c-21958122bbff" };
        IndexesToSegmentsRequest req = new IndexesToSegmentsRequest();
        req.setProject(project);
        req.setParallelBuildBySegment(false);
        mockGetModelName(modelName, project, modelId);
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .addIndexesToSegments(modelId, req);
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                        .param("project", "default") //
                        .param("parallel", "false") //
                        .param("ids", ids) //
                        .param("names", names) //
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        String contentAsString = result.getResponse().getContentAsString();
        Assert.assertTrue(contentAsString.contains(MsgPicker.getMsg().getCONFLICT_SEGMENT_PARAMETER()));
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .deleteSegments(modelId, project, true, false, null, null);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", true, false, null, null);
    }

    @Test
    public void testDeleteSegmentsByIds() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        SegmentsRequest request = new SegmentsRequest();
        request.setIds(new String[] { "1", "2" });
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .deleteSegments(modelId, project, false, false, request.getIds(), null);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "false").param("ids", request.getIds())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", false, false, request.getIds(), null);
    }

    @Test
    public void testGetModelDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
    }

    @Test
    public void testGetModelDescWithTableRefsAndCCInfo() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
        Assert.assertNotNull(result.getResponse().getContentAsString().contains("all_table_refs"));
        Assert.assertNotNull(result.getResponse().getContentAsString().contains("computed_columns"));
    }

    @Test
    public void testGetModelDescForStreaming() throws Exception {
        String project = "streaming_test";
        String modelAlias = "model_streaming";
        val model = mockGetModelName(modelAlias, project, RandomUtil.randomUUIDStr());
        model.setModelType(NDataModel.ModelType.STREAMING);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
    }

    @Test
    public void testUpdateParatitionDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        mockGetModelName(modelAlias, project, "modelId");
        ModelParatitionDescRequest modelParatitionDescRequest = new ModelParatitionDescRequest();
        modelParatitionDescRequest.setPartitionDesc(null);
        Mockito.doNothing().when(modelService).updateDataModelParatitionDesc(project, modelAlias,
                modelParatitionDescRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{project}/{model}/partition_desc", project, modelAlias)
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelParatitionDescRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updatePartitionDesc(project, modelAlias, modelParatitionDescRequest);
    }

    @Test
    public void testCouldAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        AbstractContext proposeContext = new ModelSelectContextOfSemiV2(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        Mockito.doReturn(proposeContext).when(modelSmartService).probeRecommendation(favoriteRequest.getProject(),
                favoriteRequest.getSqls());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    @Test
    public void testPostModelSuggestionCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    @Test
    public void testPostModelOptimizationCouldAnsweredByExistedModelWithNullSqls() throws Exception {
        List<String> sqls = new ArrayList<>();
        try {
            FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                    .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)));
        } catch (KylinException e) {
            Assert.assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode(), e.getErrorCode());
            Assert.assertEquals("Please enter the array parameter \"sqls\".", e.getMessage());
        }

    }

    @Test
    public void testAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        Mockito.doReturn(new OpenValidationResponse()).when(openModelController).batchSqlValidate("default", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).answeredByExistedModel(Mockito.any());
    }

    @Test
    public void testBuildIndicesManually() throws Exception {
        BuildIndexRequest request = new BuildIndexRequest();
        request.setProject("default");
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        mockGetModelName(modelName, request.getProject(), modelId);
        Mockito.doAnswer(x -> null).when(nModelController).buildIndicesManually(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model}/indexes", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).buildIndicesManually(eq(modelName), Mockito.any(BuildIndexRequest.class));
    }

    @Test
    public void testSuggestModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelCreateContextOfSemiV2(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, false, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).suggestModels(Mockito.any());
    }

    @Test
    public void testOptimizeModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContextOfSemiV2(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).optimizeModels(Mockito.any());
    }

    @Test
    public void testAccSqls() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContextOfSemiV2(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/accelerate_sqls")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).accelerateSqls(Mockito.any());
    }

    @Test
    public void testCheckSegments() throws Exception {
        mockGetModelName("test", "default", "modelId");
        Mockito.doAnswer(x -> null).when(modelService).checkSegments(Mockito.any(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString());
        CheckSegmentRequest request = new CheckSegmentRequest();
        request.setProject("default");
        request.setStart("0");
        request.setEnd("1");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/test/segments/check")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).checkSegments(Mockito.anyString(), Mockito.any());

    }

    @Test
    public void testGetMultiPartitions() throws Exception {
        String modelName = "default_model_name";
        String project = "default";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String segmentId = "73570f31-05a5-448f-973c-44209830dd01";
        mockGetModelName(modelName, project, modelId);
        DataResult<List<SegmentPartitionResponse>> result;
        Mockito.when(nModelController.getMultiPartition(modelId, project, segmentId, Lists.newArrayList(), 0, 10,
                "last_modify_time", true)).thenReturn(null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/segments/multi_partition", modelName)
                .param("project", project).param("segment_id", segmentId)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateMultiPartitionMapping() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        MultiPartitionMappingRequest request = new MultiPartitionMappingRequest();
        request.setProject("multi_level_partition");
        Mockito.doNothing().when(modelService).updateMultiPartitionMapping(request.getProject(),
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/multi_partition/mapping", modelName)
                .param("project", project).contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testBuildMultiPartition() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        PartitionsBuildRequest request = new PartitionsBuildRequest();
        request.setProject("multi_level_partition");
        Mockito.doReturn(null).when(nModelController).buildMultiPartition(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/multi_partition", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testRefreshMultiPartition() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        PartitionsRefreshRequest request = new PartitionsRefreshRequest();
        request.setProject("multi_level_partition");
        Mockito.doReturn(null).when(nModelController).refreshMultiPartition(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/segments/multi_partition", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testDeleteMultiPartition() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        String segmentId = "8892fa3f-f607-4eec-8159-7c5ae2f16942";
        mockGetModelName(modelName, project, modelId);
        PartitionsRefreshRequest request = new PartitionsRefreshRequest();
        request.setProject("multi_level_partition");
        Mockito.doNothing().when(modelService).deletePartitionsByValues(anyString(), anyString(), anyString(),
                anyList());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/segments/multi_partition").param("model", modelName)
                .param("project", project).param("segment_id", segmentId).param("sub_partition_values", "1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAddMultiValues() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        UpdateMultiPartitionValueRequest request = new UpdateMultiPartitionValueRequest();
        request.setProject("multi_level_partition");

        Mockito.doReturn(null).when(nModelController).addMultiPartitionValues(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model_name}/segments/multi_partition/sub_partition_values", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        // test exception
        request.setProject("default");
        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/models/{model_name}/segments/multi_partition/sub_partition_values", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());
    }

    @Test
    public void testCheckSqlListNotEmpty() {
        List<String> sqls = null;
        try {
            OpenModelController.checkNotEmpty(sqls);
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getNULL_EMPTY_SQL(), e.getMessage());
        }
    }

    @Test
    public void testCheckMLPNotEmpty() {
        String fieldName = "sub_partition_values";
        List<String[]> subPartitionValues = new ArrayList<>();
        try {
            OpenModelController.checkMLP(fieldName, subPartitionValues);
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(String.format(Locale.ROOT, "'%s' cannot be empty.", fieldName), e.getMessage());
        }

    }

    @Test
    public void testUpdatePartition() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        PartitionColumnRequest request = new PartitionColumnRequest();
        request.setMultiPartitionDesc(new MultiPartitionDesc());
        request.setProject("multi_level_partition");
        Mockito.doReturn(null).when(nModelController).updatePartitionSemantic(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/partition", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testCheckIndex() {
        List<String> statusStringList = Lists.newArrayList("recommended_table_index", "RECOMMENDED_AGG_INDEX",
                "CUSTOM_AGG_INDEX", "custom_table_index");
        List<IndexEntity.Source> statuses = OpenModelController.checkSources(statusStringList);
        Assert.assertEquals(IndexEntity.Source.RECOMMENDED_TABLE_INDEX, statuses.get(0));
        Assert.assertEquals(IndexEntity.Source.RECOMMENDED_AGG_INDEX, statuses.get(1));
        Assert.assertEquals(IndexEntity.Source.CUSTOM_AGG_INDEX, statuses.get(2));
        Assert.assertEquals(IndexEntity.Source.CUSTOM_TABLE_INDEX, statuses.get(3));

        try {
            OpenModelController.checkSources(Lists.newArrayList("ab"));
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getINDEX_SOURCE_TYPE_ERROR(), e.getMessage());
        }
    }

    @Test
    public void testCheckIndexStatus() {
        List<String> statusStringList = Lists.newArrayList("NO_BUILD", "BUILDING", "LOCKED", "ONLINE");
        List<IndexEntity.Status> statuses = OpenModelController.checkIndexStatus(statusStringList);
        Assert.assertEquals(IndexEntity.Status.NO_BUILD, statuses.get(0));
        Assert.assertEquals(IndexEntity.Status.BUILDING, statuses.get(1));
        Assert.assertEquals(IndexEntity.Status.LOCKED, statuses.get(2));
        Assert.assertEquals(IndexEntity.Status.ONLINE, statuses.get(3));

        try {
            OpenModelController.checkIndexStatus(Lists.newArrayList("ab"));
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getINDEX_STATUS_TYPE_ERROR(), e.getMessage());
        }
    }

    @Test
    public void testCheckIndexSortBy() {
        List<String> orderBy = Lists.newArrayList("last_modified", "usage", "data_size");
        orderBy.forEach(element -> {
            String actual = OpenModelController.checkIndexSortBy(element);
            Assert.assertEquals(element.toLowerCase(Locale.ROOT), actual);
        });

        try {
            OpenModelController.checkIndexSortBy("ab");
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getINDEX_SORT_BY_ERROR(), e.getMessage());
        }
    }

    @Test
    public void testOpenapiUpdateModelName() throws Exception {
        String project = "default";
        String new_model_name = "newName";
        String model = "model1";
        mockGetModelName(model, project, RandomUtil.randomUUIDStr());
        ModelUpdateRequest modelUpdateRequest = new ModelUpdateRequest();
        modelUpdateRequest.setProject(project);
        modelUpdateRequest.setNewModelName(new_model_name);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/name", model)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(modelUpdateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateModelName(model, modelUpdateRequest);
    }

    private void changeProjectToSemiAutoMode(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @Test
    public void testOpenAPIBIExport() throws Exception {
        String modelName = "multi_level_partition";
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        mockGetModelName(modelName, project, modelId);
        Mockito.doReturn(null).when(modelService).exportCustomModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", SyncContext.BI.TABLEAU_CONNECTOR_TDS,
                SyncContext.ModelElement.AGG_INDEX_COL, "localhost", 8080);
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/models/bi_export")
                .param("model_name", modelName).param("project", project)
                .param("export_as", "TABLEAU_ODBC_TDS").param("element", "AGG_INDEX_COL")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
