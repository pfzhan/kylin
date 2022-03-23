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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclPermissionEnum;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.constant.NonCustomProjectLevelConfig;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.DefaultDatabaseRequest;
import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JdbcSourceInfoRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.ProjectConfigRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.PushDownProjectConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.request.SnapshotConfigRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.request.YarnQueueRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;

public class NProjectControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ProjectService projectService;

    @InjectMocks
    private NProjectController nProjectController = Mockito.spy(new NProjectController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nProjectController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private ProjectRequest mockProjectRequest() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName("test");
        projectRequest.setDescription("test");
        projectRequest.setOverrideKylinProps(new LinkedHashMap<>());
        projectRequest.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
        return projectRequest;
    }

    @Test
    public void testGetProjects() throws Exception {
        List<ProjectInstance> projects = new ArrayList<>();
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project1");
        projects.add(projectInstance);
        Mockito.when(projectService.getReadableProjects("default", false)).thenReturn(projects);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").param("page_offset", "0").param("page_size", "10")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getProjects("default", 0, 10, false, AclPermissionEnum.READ.name());

    }

    @Test
    public void testInvalidProjectName() {
        ProjectRequest projectRequest = mockProjectRequest();
        projectRequest.setName("^project");
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_PROJECT_NAME());
        nProjectController.saveProject(projectRequest);
    }

    @Test
    public void testSaveProjects() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("test");
        ProjectRequest projectRequest = mockProjectRequest();
        projectRequest.setName("test");
        Mockito.when(projectService.createProject(projectInstance.getName(), projectInstance))
                .thenReturn(projectInstance);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(projectRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).saveProject(Mockito.any(ProjectRequest.class));
    }

    @Test
    public void testUpdateQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdRequest favoriteQueryThresholdRequest = new FavoriteQueryThresholdRequest();
        favoriteQueryThresholdRequest.setThreshold(20);
        favoriteQueryThresholdRequest.setTipsEnabled(true);
        Mockito.doNothing().when(projectService).updateQueryAccelerateThresholdConfig("default", 20, true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/query_accelerate_threshold", "default")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteQueryThresholdRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateQueryAccelerateThresholdConfig(eq("default"),
                Mockito.any(FavoriteQueryThresholdRequest.class));
    }

    @Test
    public void testGetQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdResponse favoriteQueryThresholdResponse = new FavoriteQueryThresholdResponse();
        favoriteQueryThresholdResponse.setTipsEnabled(true);
        favoriteQueryThresholdResponse.setThreshold(20);
        Mockito.doReturn(favoriteQueryThresholdResponse).when(projectService)
                .getQueryAccelerateThresholdConfig("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/query_accelerate_threshold", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getQueryAccelerateThresholdConfig("default");
    }

    @Test
    public void testGetStorageVolumeInfoResponse() throws Exception {
        StorageVolumeInfoResponse storageVolumeInfoResponse = new StorageVolumeInfoResponse();
        Mockito.doReturn(storageVolumeInfoResponse).when(projectService).getStorageVolumeInfoResponse("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/{project}/storage_volume_info", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getStorageVolumeInfo("default");
    }

    @Test
    public void testUpdateStorageQuotaConfig() throws Exception {
        StorageQuotaRequest storageQuotaRequest = new StorageQuotaRequest();
        storageQuotaRequest.setStorageQuotaSize(2147483648L);
        Mockito.doNothing().when(projectService).updateStorageQuotaConfig("default", 2147483648L);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/storage_quota", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(storageQuotaRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateStorageQuotaConfig(eq("default"),
                Mockito.any(StorageQuotaRequest.class));
    }

    @Test
    public void testStorageCleanup() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        NProjectManager projectManager = Mockito.mock(NProjectManager.class);
        Mockito.doReturn(projectInstance).when(projectManager).getProject("default");
        Mockito.doReturn(projectManager).when(projectService).getManager(NProjectManager.class);
        Mockito.doNothing().when(projectService).cleanupGarbage("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/storage", "default")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).cleanupProjectStorage("default");
    }

    @Test
    public void testUpdateJobNotificationConfig() throws Exception {
        val request = new JobNotificationConfigRequest();

        request.setJobErrorNotificationEnabled(true);
        request.setDataLoadEmptyNotificationEnabled(true);
        request.setJobNotificationEmails(Arrays.asList("fff@g.com"));

        Mockito.doNothing().when(projectService).updateJobNotificationConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/job_notification_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateJobNotificationConfig("default", request);
    }

    @Test
    public void testUpdatePushDownConfig() throws Exception {
        val request = new PushDownConfigRequest();
        request.setPushDownEnabled(true);

        Mockito.doNothing().when(projectService).updatePushDownConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/push_down_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updatePushDownConfig("default", request);
    }

    @Test
    public void testUpdatePushDownProjectConfig() throws Exception {
        val request = new PushDownProjectConfigRequest();
        request.setConverterClassNames("io.kyligence.kap.query.util.SparkSQLFunctionConverter");
        request.setRunnerClassName("io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");

        Mockito.doNothing().when(projectService).updatePushDownProjectConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/push_down_project_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updatePushDownProjectConfig("default", request);
    }

    @Test
    public void testUpdateSnapshotConfig() throws Exception {
        val request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);

        Mockito.doNothing().when(projectService).updateSnapshotConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/snapshot_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSnapshotConfig("default", request);
    }

    @Test
    public void testUpdateShardNumConfig() throws Exception {
        val request = new ShardNumConfigRequest();
        Mockito.doNothing().when(projectService).updateShardNumConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/shard_num_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateShardNumConfig("default", request);
    }

    @Test
    public void testUpdateSegmentConfig() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange());
        request.setAutoMergeEnabled(true);
        request.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.DAY));
        request.setCreateEmptySegmentEnabled(true);

        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSegmentConfig(eq("default"), Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalRetentionRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange(-1, true, AutoMergeTimeEnum.DAY));
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig("default", request);
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalVolatileRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setRetentionRange(new RetentionRange());
        request.setVolatileRange(new VolatileRange(-1, true, AutoMergeTimeEnum.DAY));
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/segment_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig("default", request);
    }

    @Test
    public void testUpdateProjectGeneralInfo() throws Exception {
        val request = new ProjectGeneralInfoRequest();
        request.setSemiAutoMode(true);
        Mockito.doNothing().when(projectService).updateProjectGeneralInfo("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/project_general_info", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateProjectGeneralInfo("default", request);
    }

    @Test
    public void testGetProjectConfig() throws Exception {
        val response = new ProjectConfigResponse();
        Mockito.doReturn(response).when(projectService).getProjectConfig("default");
        final MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/projects/{project}/project_config", "default")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Assert.assertTrue(mvcResult.getResponse().getContentAsString().contains("\"semi_automatic_mode\":false"));
        Assert.assertTrue(
                mvcResult.getResponse().getContentAsString().contains("\"maintain_model_type\":\"AUTO_MAINTAIN\""));
        Mockito.verify(nProjectController).getProjectConfig("default");
    }

    @Test
    public void testDeleteProjectConfig() throws Exception {
        ProjectConfigRequest request = new ProjectConfigRequest();
        request.setProject("default");
        request.setConfigName("a");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects/config/deletion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectController).deleteProjectConfig(Mockito.any(ProjectConfigRequest.class));
    }

    @Test
    public void testUpdateProjectConfig() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(map))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectController).updateProjectConfig("default", map);

        map.put("kylin.source.default", "1");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(map))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.doThrow(KylinException.class).when(nProjectController).updateProjectConfig("default", map);
    }

    @Test
    public void testDeleteProjectConfigException() throws Exception {
        ProjectConfigRequest request = new ProjectConfigRequest();
        request.setProject("default");
        request.setConfigName("kylin.source.default");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects/config/deletion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.doThrow(KylinException.class).when(nProjectController).deleteProjectConfig(request);
    }

    @Test
    public void testGetNonCustomProjectConfigs() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/default_configs")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nProjectController).getNonCustomProjectConfigs();
        Assert.assertEquals(17, NonCustomProjectLevelConfig.listAllConfigNames().size());
    }

    @Test
    public void testUpdateDefaultDatabase() throws Exception {
        val request = new DefaultDatabaseRequest();
        request.setDefaultDatabase("EDW");
        Mockito.doNothing().when(projectService).updateDefaultDatabase("default", request.getDefaultDatabase());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/default_database", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateDefaultDatabase("default", request);
    }

    @Test
    public void testUpdateYarnQueue() throws Exception {
        val request = new YarnQueueRequest();
        request.setQueueName("q.queue");
        Mockito.doNothing().when(projectService).updateYarnQueue("project", request.getQueueName());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/yarn_queue", "project")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateYarnQueue("project", request);
    }

    @Test
    public void testUpdateProjectOwner() {
        String project = "default";
        String owner = "test";

        OwnerChangeRequest ownerChangeRequest = new OwnerChangeRequest();
        ownerChangeRequest.setOwner(owner);

        Mockito.doNothing().when(projectService).updateProjectOwner(project, ownerChangeRequest);
        nProjectController.updateProjectOwner(project, ownerChangeRequest);
        Mockito.verify(nProjectController).updateProjectOwner(project, ownerChangeRequest);
    }

    @Test
    public void testUpdateGarbageCleanupConfig() throws Exception {
        GarbageCleanUpConfigRequest request = new GarbageCleanUpConfigRequest();
        request.setLowFrequencyThreshold(1L);
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.DAY);
        Mockito.doAnswer(x -> null).when(projectService).updateGarbageCleanupConfig(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/garbage_cleanup_config", "default")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nProjectController).updateGarbageCleanupConfig(Mockito.any(), Mockito.any());
    }

    @Test
    public void testUpdateJdbcSourceConfig() throws Exception {
        val request = new JdbcSourceInfoRequest();
        request.setJdbcSourceEnable(true);
        Mockito.doNothing().when(projectService).updateJdbcInfo("project", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/{project}/jdbc_source_info_config", "project")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(projectService).updateJdbcInfo(any(), Mockito.any());
    }
}
