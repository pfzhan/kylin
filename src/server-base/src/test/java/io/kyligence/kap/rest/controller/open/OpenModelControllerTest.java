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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccerelateRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.AclEvaluate;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.ModelParatitionDescRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.OpenApplyRecommendationsRequest;
import io.kyligence.kap.rest.request.OpenBatchApplyRecommendationsRequest;
import io.kyligence.kap.rest.response.NRecomendationListResponse;
import io.kyligence.kap.rest.service.ProjectService;

public class OpenModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NModelController nModelController;

    @Mock
    private ModelService modelService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private ProjectService projectService;

    @InjectMocks
    private OpenModelController openModelController = Mockito.spy(new OpenModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        Mockito.doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("default");
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService)
                .getReadableProjects(projectInstance.getName(), true);
    }

    @Before
    public void setupResource() throws Exception {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
        restoreAllSystemProp();
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
        Mockito.when(nModelController.getModels("model1", true, "default", "ADMIN", Arrays.asList("NEW"), "", 1, 5,
                "last_modify", false))
                .thenReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(mockModels(), 0, 10), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("page_offset", "1").param("project", "default").param("model_name", "model1")
                .param("page_size", "5").param("exact", "true").param("table", "").param("owner", "ADMIN")
                .param("status", "NEW").param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openModelController).getModels("default", "model1", true, "ADMIN", Arrays.asList("NEW"), "", 1,
                5, "last_modify", true);
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
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("page_offset", "1").param("project", project)
                .param("page_size", "5").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                .param("reverse", "true").param("status", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
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
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nModelController)
                .refreshOrMergeSegmentsByIds(modelId, request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
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
                .deleteSegments(modelId, project, true, false, null);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", true, false, null);
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
                .deleteSegments(modelId, project, false, false, request.getIds());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "false").param("ids", request.getIds())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).deleteSegments(modelName, "default", false, false, request.getIds());
    }

    @Test
    public void testGetModelDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        Mockito.doAnswer(x -> null).when(modelService).getModelDesc(modelAlias, project);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{project}/{model}/model_desc", project, modelAlias)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getModelDesc(project, modelAlias);
    }

    @Test
    public void testUpdateParatitionDesc() throws Exception {
        String project = "default";
        String modelAlias = "model1";
        ModelParatitionDescRequest modelParatitionDescRequest = new ModelParatitionDescRequest();
        modelParatitionDescRequest.setPartitionDesc(null);
        Mockito.doNothing().when(modelService).updateDataModelParatitionDesc(project, modelAlias,
                modelParatitionDescRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{project}/{model}/partition_desc", project, modelAlias)
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(modelParatitionDescRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).updateParatitionDesc(project, modelAlias, modelParatitionDescRequest);
    }

    @Test
    public void testCouldAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        Mockito.doReturn(new Pair<>(Sets.newHashSet(sqls), Sets.newHashSet())).when(openModelController)
                .batchSqlValidate("default", sqls);
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
    public void testGetRecommendationsByModel() throws Exception {
        String project = "default";
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        mockGetModelName(modelName, project, modelId);

        List<String> sources = Lists.newArrayList();
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "")).when(nModelController)
                .getOptimizeRecommendations(modelId, project, sources);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models//{model_name:.+}/recommendations", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("project", project)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getOptimizeRecommendations(modelName, project, sources);
    }

    @Test
    public void testApplyRecommendation() throws Exception {
        String project = "default";
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        mockGetModelName(modelName, project, modelId);

        val request = new OpenApplyRecommendationsRequest();
        request.setProject(project);

        Mockito.doAnswer(x -> null).when(nModelController).applyOptimizeRecommendations(eq(modelId), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/{model_name:.+}/recommendations", modelName)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).applyOptimizeRecommendations(eq(modelName),
                Mockito.any(OpenApplyRecommendationsRequest.class));
    }

    @Test
    public void testSuggestModelWithReuseExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccerelateRequest favoriteRequest = new OpenSqlAccerelateRequest("default", sqls, false);

        // reuse existed model
        val result = new NRecomendationListResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(result).when(modelService).suggestModel(favoriteRequest.getProject(), sqls, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).suggestModel(Mockito.any());
    }

    @Test
    public void testSuggestModels() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccerelateRequest favoriteRequest = new OpenSqlAccerelateRequest("default", sqls, null);

        // reuse existed model
        val result = new NRecomendationListResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(result).when(modelService).suggestModel(favoriteRequest.getProject(), sqls, false);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).suggestModels(Mockito.any());
    }

    @Test
    public void testOptimizeModels() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccerelateRequest favoriteRequest = new OpenSqlAccerelateRequest("default", sqls, null);

        // reuse existed model
        val result = new NRecomendationListResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(result).when(modelService).suggestModel(favoriteRequest.getProject(), sqls, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).optimizeModels(Mockito.any());
    }

    @Test
    public void testGetRecommendationsByProject() throws Exception {
        Mockito.doReturn(null).when(nModelController).getRecommendationsByProject("default");
        // model and project is empty
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/recommendations").param("project", "default")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).getRecommendationsByProject("default");

    }

    @Test
    public void testBatchApplyRecommendations() throws Exception {
        Mockito.doReturn(null).when(nModelController).batchApplyRecommendations(eq("default"), Mockito.anyList());

        List<String> modelNames = Lists.newArrayList("model1, model2");
        OpenBatchApplyRecommendationsRequest request = new OpenBatchApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelNames(modelNames);

        Mockito.doReturn(null).when(openModelController).getModel(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/recommendations/batch")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openModelController).batchApplyRecommendations(Mockito.any());
    }

    @Test
    public void testBatchApplyRecommendations_emptyModelNames() throws Exception {
        Mockito.doReturn(null).when(nModelController).batchApplyRecommendations(eq("default"), Mockito.anyList());

        List<String> modelNames = Lists.newArrayList();
        OpenBatchApplyRecommendationsRequest request = new OpenBatchApplyRecommendationsRequest();
        request.setProject("default");
        request.setModelNames(modelNames);

        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/recommendations/batch")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(openModelController).batchApplyRecommendations(Mockito.any());
    }

}
