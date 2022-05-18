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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_MULTI_PARTITION_DISABLE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_CONFLICT_PARAMETER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.controller.SegmentController;
import io.kyligence.kap.rest.request.BuildIndexRequest;
import io.kyligence.kap.rest.request.BuildSegmentsRequest;
import io.kyligence.kap.rest.request.CheckSegmentRequest;
import io.kyligence.kap.rest.request.IndexesToSegmentsRequest;
import io.kyligence.kap.rest.request.PartitionsBuildRequest;
import io.kyligence.kap.rest.request.PartitionsRefreshRequest;
import io.kyligence.kap.rest.request.SegmentsRequest;
import io.kyligence.kap.rest.response.IndexResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.SegmentPartitionResponse;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.ModelService;

@RunWith(MockitoJUnitRunner.class)
public class OpenSegmentControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private SegmentController nModelController;

    @Mock
    private ModelService modelService;

    @Mock
    private FusionModelService fusionModelService;

    @Mock
    private AclEvaluate aclEvaluate;

    @InjectMocks
    private final OpenSegmentController openSegmentController = Mockito.spy(new OpenSegmentController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openSegmentController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        lenient().doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        lenient().doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
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
        Mockito.doReturn(model).when(openSegmentController).getModel(modelName, project);
        return model;
    }

    @Test
    public void testGetSegments() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);
        when(nModelController.getSegments(modelId, project, "", 1, 5, "432", "2234", null, null, false,
                "end_time", true)).thenReturn(
                        new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(mockSegments(), 1, 5), ""));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/segments", modelName)
                .contentType(MediaType.APPLICATION_JSON).param("page_offset", "1").param("project", project)
                .param("page_size", "5").param("start", "432").param("end", "2234").param("sort_by", "end_time")
                .param("reverse", "true").param("status", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(openSegmentController).getSegments(modelName, project, "", 1, 5, "432", "2234", "end_time",
                true);
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
        Mockito.verify(openSegmentController).buildSegmentsManually(eq(modelName),
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
        Mockito.verify(openSegmentController).refreshOrMergeSegments(eq(modelName), Mockito.any(SegmentsRequest.class));
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
        lenient().doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
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
        Mockito.verify(openSegmentController).completeSegments(modelName, project, false, ids, null, null, false, 3,
                null, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                .param("project", "default") //
                .param("parallel", "false") //
                .param("ids", ids) //
                .param("names", (String) null) //
                .param("priority", "0").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSegmentController).completeSegments(modelName, project, false, ids, null, null, false, 0,
                null, null);
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
        lenient().doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
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
        Mockito.verify(openSegmentController).completeSegments(modelName, project, false, ids, null, batchIndexIds,
                true, 3, null, null);
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
        lenient().doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .addIndexesToSegments(modelId, req);
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/{model_name}/segments/completion", modelName)
                        .param("project", "default") //
                        .param("parallel", "false") //
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        String contentAsString = result.getResponse().getContentAsString();
        Assert.assertTrue(contentAsString.contains(MsgPicker.getMsg().getEmptySegmentParameter()));
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
        lenient().doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
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
        Assert.assertTrue(contentAsString.contains(SEGMENT_CONFLICT_PARAMETER.getMsg()));
    }

    @Test
    public void testDeleteSegmentsAll() throws Exception {
        String modelName = "default_model_name";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String project = "default";
        mockGetModelName(modelName, project, modelId);

        lenient().doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nModelController)
                .deleteSegments(modelId, project, true, false, null, null);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/models/{model_name}/segments", modelName)
                .param("project", "default").param("purge", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSegmentController).deleteSegments(modelName, "default", true, false, null, null);
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
        Mockito.verify(openSegmentController).deleteSegments(modelName, "default", false, false, request.getIds(),
                null);
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
        Mockito.verify(openSegmentController).buildIndicesManually(eq(modelName), Mockito.any(BuildIndexRequest.class));
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
        Mockito.verify(openSegmentController).checkSegments(Mockito.anyString(), Mockito.any());

    }

    @Test
    public void testGetMultiPartitions() throws Exception {
        String modelName = "default_model_name";
        String project = "default";
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        String segmentId = "73570f31-05a5-448f-973c-44209830dd01";
        mockGetModelName(modelName, project, modelId);
        DataResult<List<SegmentPartitionResponse>> result;
        lenient().when(nModelController.getMultiPartition(modelId, project, segmentId, Lists.newArrayList(), 0, 10,
                "last_modify_time", true)).thenReturn(null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models/{model_name}/segments/multi_partition", modelName)
                .param("project", project).param("segment_id", segmentId)
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
    public void testGetModel() {
        NDataModelManager nDataModelManager = Mockito.mock(NDataModelManager.class);
        when(modelService.getManager(any(), anyString())).thenReturn(nDataModelManager);

        when(nDataModelManager.listAllModels()).thenReturn(Collections.emptyList());
        try {
            openSegmentController.getModel("SOME_ALIAS", "SOME_PROJECT");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(MODEL_NAME_NOT_EXIST.getCodeMsg("SOME_ALIAS"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckProjectMLP() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = Mockito.mockStatic(KylinConfig.class);
             MockedStatic<NProjectManager> nProjectManagerMockedStatic = Mockito.mockStatic(NProjectManager.class)) {
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(Mockito.mock(KylinConfig.class));
            NProjectManager projectManager = Mockito.mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(Mockito.any())).thenReturn(projectManager);
            ProjectInstance projectInstance = Mockito.mock(ProjectInstance.class);
            when(projectManager.getProject(anyString())).thenReturn(projectInstance);
            KylinConfigExt kylinConfigExt = Mockito.mock(KylinConfigExt.class);
            when(projectInstance.getName()).thenReturn("TEST_PROJECT_NAME");
            when(projectInstance.getConfig()).thenReturn(kylinConfigExt);
            when(kylinConfigExt.isMultiPartitionEnabled()).thenReturn(false);
            try {
                ReflectionTestUtils.invokeMethod(openSegmentController, "checkProjectMLP", "SOME_PROJECT");
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(PROJECT_MULTI_PARTITION_DISABLE.getCodeMsg("TEST_PROJECT_NAME"), e.getLocalizedMessage());
            }
        }
    }

}
