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

import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.DefaultDatabaseRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.rest.request.FavoriteQueryThresholdRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.request.StorageQuotaRequest;
import io.kyligence.kap.rest.response.FavoriteQueryThresholdResponse;
import io.kyligence.kap.rest.response.ProjectConfigResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.junit.After;
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

public class NProjectControllerTest {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private ProjectService projectService;

    @InjectMocks
    private NProjectController nProjectController = Mockito.spy(new NProjectController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nProjectController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    private ProjectRequest mockProjectRequest() {
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setProjectDescData("{\"name\":\"test\"}");
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
                        .param("project", "default").param("pageOffset", "0").param("pageSize", "10")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getProjects("default", 0, 10, false);

    }

    @Test
    public void testInvalidProjectName() {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("^project");
        ProjectRequest projectRequest = mockProjectRequest();
        Mockito.when(projectService.deserializeProjectDesc(projectRequest)).thenReturn(projectInstance);
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_PROJECT_NAME());
        nProjectController.saveProject(projectRequest);
    }

    @Test
    public void testSaveProjects() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("test");
        ProjectRequest projectRequest = mockProjectRequest();
        Mockito.when(projectService.deserializeProjectDesc(projectRequest)).thenReturn(projectInstance);
        Mockito.when(projectService.createProject(projectInstance.getName(), projectInstance)).thenReturn(projectInstance);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(projectRequest))
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).saveProject(Mockito.any(ProjectRequest.class));

    }

    @Test
    public void testUpdateQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdRequest favoriteQueryThresholdRequest = new FavoriteQueryThresholdRequest();
        favoriteQueryThresholdRequest.setProject("default");
        favoriteQueryThresholdRequest.setThreshold(20);
        favoriteQueryThresholdRequest.setTipsEnabled(true);
        Mockito.doNothing().when(projectService).updateQueryAccelerateThresholdConfig("default", 20, true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/query_accelerate_threshold")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteQueryThresholdRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateQueryAccelerateThresholdConfig(Mockito.any(FavoriteQueryThresholdRequest.class));
    }


    @Test
    public void testGetQueryAccelerateThreshold() throws Exception {
        FavoriteQueryThresholdResponse favoriteQueryThresholdResponse = new FavoriteQueryThresholdResponse();
        favoriteQueryThresholdResponse.setTipsEnabled(true);
        favoriteQueryThresholdResponse.setThreshold(20);
        Mockito.doReturn(favoriteQueryThresholdResponse).when(projectService).getQueryAccelerateThresholdConfig("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/query_accelerate_threshold")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getQueryAccelerateThresholdConfig("default");
    }

    @Test
    public void testGetStorageVolumeInfoResponse() throws Exception {
        StorageVolumeInfoResponse storageVolumeInfoResponse = new StorageVolumeInfoResponse();
        Mockito.doReturn(storageVolumeInfoResponse).when(projectService).getStorageVolumeInfoResponse("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/storage_volume_info")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getStorageVolumeInfo("default");
    }

    @Test
    public void testUpdateStorageQuotaConfig() throws Exception {
        StorageQuotaRequest storageQuotaRequest = new StorageQuotaRequest();
        storageQuotaRequest.setProject("default");
        storageQuotaRequest.setStorageQuotaSize(2147483648L);
        Mockito.doNothing().when(projectService).updateStorageQuotaConfig("default", 2147483648L);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/storage_quota")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(storageQuotaRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).updateStorageQuotaConfig(Mockito.any(StorageQuotaRequest.class));
    }

    @Test
    public void testStorageCleanup() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        NProjectManager projectManager = Mockito.mock(NProjectManager.class);
        Mockito.doReturn(projectInstance).when(projectManager).getProject("default");
        Mockito.doReturn(projectManager).when(projectService).getProjectManager();
        Mockito.doNothing().when(projectService).cleanupGarbage("default");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/storage")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).cleanupProjectStorage("default");
    }

    @Test
    public void testUpdateJobNotificationConfig() throws Exception {
        val request = new JobNotificationConfigRequest();
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateJobNotificationConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/job_notification_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateJobNotificationConfig(request);
    }

    @Test
    public void testUpdatePushDownConfig() throws Exception {
        val request = new PushDownConfigRequest();
        request.setProject("default");
        Mockito.doNothing().when(projectService).updatePushDownConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/push_down_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updatePushDownConfig(request);
    }

    @Test
    public void testUpdateShardNumConfig() throws Exception {
        val request = new ShardNumConfigRequest();
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateShardNumConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/shard_num_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateShardNumConfig(request);
    }

    @Test
    public void testUpdateSegmentConfig() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange());
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/segment_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateSegmentConfig(Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalRetentionRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setVolatileRange(new VolatileRange());
        request.setRetentionRange(new RetentionRange(-1, true, AutoMergeTimeEnum.DAY));
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/segment_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig(request);
    }

    @Test
    public void testUpdateSegmentConfigWithIllegalVolatileRange() throws Exception {
        val request = new SegmentConfigRequest();
        request.setRetentionRange(new RetentionRange());
        request.setVolatileRange(new VolatileRange(-1, true, AutoMergeTimeEnum.DAY));
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateSegmentConfig("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/segment_config")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nProjectController, Mockito.never()).updateSegmentConfig(request);
    }

    @Test
    public void testUpdateProjectGeneralInfo() throws Exception {
        val request = new ProjectGeneralInfoRequest();
        request.setProject("default");
        Mockito.doNothing().when(projectService).updateProjectGeneralInfo("default", request);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/project_general_info")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateProjectGeneralInfo(request);
    }

    @Test
    public void testGetProjectConfig() throws Exception {
        val response = new ProjectConfigResponse();
        Mockito.doReturn(response).when(projectService).getProjectConfig("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/project_config")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nProjectController).getProjectConfig("default");
    }

    @Test
    public void testUpdateDefaultDatabase() throws Exception {
        val request = new DefaultDatabaseRequest();
        request.setProject("default");
        request.setDefaultDatabase("EDW");
        Mockito.doNothing().when(projectService).updateDefaultDatabase(request.getProject(), request.getDefaultDatabase());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/default_database")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nProjectController).updateDefaultDatabase(request);
    }
}
