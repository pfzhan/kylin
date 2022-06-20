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
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.hamcrest.CoreMatchers.containsString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.BatchAccessRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.ProjectService;

public class NAccessControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private AccessService accessService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private ProjectService projectService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NAccessController nAccessController = Mockito.spy(new NAccessController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private String type = "ProjectInstance";

    private String uuid = "u126snk32242152";

    private String sid = "user_g1";

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nAccessController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetUserPermissionInPrj() throws Exception {
        List<JobStatusEnum> status = new ArrayList<>();
        status.add(JobStatusEnum.NEW);
        ArrayList<AbstractExecutable> jobs = new ArrayList<>();
        Integer[] statusInt = { 4 };
        String[] subjects = {};
        Mockito.when(accessService.getCurrentNormalUserPermissionInProject("default")).thenReturn("ADMIN");
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/access/permission/project_permission")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nAccessController).getUserPermissionInPrj("default");
    }

    @Test
    public void testGrantPermissionForValidUser() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    @Ignore
    public void testGrantPermissionForInvalidUser() throws Exception {
        String sid = "1/";
        String expectedErrorMsg = "User/Group name should only contain alphanumerics and underscores.";
        testGrantPermissionForUser(sid, expectedErrorMsg);
    }

    @Test
    public void testUpdateAcl() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        accessRequest.setPrincipal(true);
        accessRequest.setPermission("OPERATION");

        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).updateAclTCR(uuid, null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).updateAcl(type, uuid, accessRequest);
    }

    @Test
    public void testRevokeAcl() throws Exception {
        Mockito.doReturn(true).when(userService).userExists(sid);
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, true);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("access_entry_id", "1").param("sid", sid)
                .param("principal", "true").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, sid, true);
    }

    @Test
    public void testRevokeAclWithNotExistSid() throws Exception {
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(uuid, false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).param("access_entry_id", "1").param("sid", "NotExist")
                .param("principal", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).revokeAcl(type, uuid, 1, "NotExist", false);
    }

    @Test
    public void testBatchRevokeAcl() throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        List<AccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}/deletion", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).deleteAces(type, uuid, requests);
    }

    @Test
    public void testGetAvailableUsersForProject() throws Exception {
        List<ProjectInstance> list = Lists.newArrayList();
        list.add(Mockito.mock(ProjectInstance.class));
        Mockito.doReturn(list).when(projectService).getReadableProjects("default", true);
        Mockito.doReturn(new HashSet<>()).when(accessService).getProjectAdminUsers("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", uuid)
                .param("name", "").param("is_case_sensitive", "false").param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);
    }

    @Test
    public void testGetProjectUsersAndGroups() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/{uuid:.+}/all", uuid)
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()
                );
        Mockito.verify(nAccessController).getProjectUsersAndGroups(uuid);
    }

    @Test
    public void testGetAvailableUsersForModel() throws Exception {
        String type = AclEntityType.N_DATA_MODEL;

        NDataModelManager nDataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn(dataModel).when(nDataModelManager).getDataModelDesc(uuid);
        Mockito.doReturn(nDataModelManager).when(projectService).getManager(NDataModelManager.class, "default");
        Mockito.doReturn(new HashSet<>()).when(accessService).getProjectManagementUsers("default");

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", uuid)
                .param("name", "").param("is_case_sensitive", "false").param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/available/{entity_type:.+}", type)
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .param("model", RandomUtil.randomUUIDStr()).param("name", "").param("is_case_sensitive", "false")
                .param("page_offset", "0").param("page_size", "10")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nAccessController).getAvailableUsers(type, "default", uuid, "", false, 0, 10);
    }

    private void testGrantPermissionForUser(String sid, String expectedMsg) throws Exception {
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setSid(sid);
        AclEntity ae = accessService.getAclEntity(type, uuid);
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(accessRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString(expectedMsg)));
        Mockito.verify(nAccessController).grant(type, uuid, accessRequest);
    }

    @Test
    public void testBatchGrant() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests)).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nAccessController).batchGrant(type, uuid, true, requests);
    }

    @Test
    public void testBatchGrantDuplicateName() throws Exception {
        BatchAccessRequest accessRequest = new BatchAccessRequest();
        List<String> sids = Lists.newArrayList(sid, sid);
        accessRequest.setSids(sids);
        accessRequest.setPrincipal(true);
        List<BatchAccessRequest> requests = Lists.newArrayList(accessRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/batch/{type}/{uuid}", type, uuid)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(requests)).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(nAccessController).batchGrant(type, uuid, true, requests);
    }
}
