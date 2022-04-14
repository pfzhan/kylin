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
import static org.apache.kylin.rest.security.AclEntityType.PROJECT_INSTANCE;

import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AccessRequest;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.BatchProjectPermissionRequest;
import io.kyligence.kap.rest.request.ProjectPermissionRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.ProjectService;

public class OpenAccessControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @InjectMocks
    private OpenAccessController openAccessController = Mockito.spy(new OpenAccessController());

    @Mock
    private AccessService accessService;

    @Mock
    private UserService userService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private ProjectService projectService;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openAccessController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetProjectAccessPermissions() throws Exception {
        AclEntity ae = accessService.getAclEntity(PROJECT_INSTANCE, "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");
        Sid sid = new PrincipalSid("user1");
        Permission permission = BasePermission.ADMINISTRATION;
        AccessEntryResponse accessEntryResponse = new AccessEntryResponse("1L", sid, permission, false);

        List<AccessEntryResponse> accessEntryResponses = new ArrayList<>();
        accessEntryResponses.add(accessEntryResponse);
        sid = new GrantedAuthoritySid("group1");
        accessEntryResponse = new AccessEntryResponse("1L", sid, permission, false);
        accessEntryResponses.add(accessEntryResponse);
        Mockito.when(accessService.generateAceResponsesByFuzzMatching(null, "test", false))
                .thenReturn(accessEntryResponses);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getProjectAccessPermissions("default", "test", false, 0, 10);
    }

    @Test
    public void testGrantProjectPermission() throws Exception {
        BatchProjectPermissionRequest batchProjectPermissionRequest = new BatchProjectPermissionRequest();
        batchProjectPermissionRequest.setProject("default");
        batchProjectPermissionRequest.setType("user");
        batchProjectPermissionRequest.setPermission("QUERY");
        batchProjectPermissionRequest.setNames(Lists.newArrayList("test"));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(batchProjectPermissionRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).grantProjectPermission(batchProjectPermissionRequest);
    }

    @Test
    public void testUpdateProjectPermission() throws Exception {
        ProjectPermissionRequest projectPermissionRequest = new ProjectPermissionRequest();
        projectPermissionRequest.setProject("default");
        projectPermissionRequest.setType("user");
        projectPermissionRequest.setPermission("QUERY");
        projectPermissionRequest.setName("test");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectPermissionRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).updateProjectPermission(projectPermissionRequest);
    }

    @Test
    public void testRevokeProjectPermission() throws Exception {
        List<AccessEntryResponse> accessEntryResponses = new ArrayList<>();
        accessEntryResponses.add(new AccessEntryResponse());
        Mockito.when(accessService.generateAceResponsesByFuzzMatching(null, "test", false))
                .thenReturn(accessEntryResponses);

        AclEntity ae = accessService.getAclEntity(PROJECT_INSTANCE, "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Mockito.doNothing().when(accessService).grant(ae, "1", true, "ADMIN");

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("type", "user").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).revokeProjectPermission("default", "user", "test");
    }

    @Test
    public void testRevokeProjectPermissionWithException() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/access/project").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("type", "user").param("name", "test")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(openAccessController).revokeProjectPermission("default", "user", "test");
    }

    @Test
    public void testGetUserOrGroupAclPermissions() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "user").param("name", "test").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("user", "test", "default");
    }

    @Test
    public void testGetUserOrGroupAclPermissionsWithProjectBlank() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "user").param("name", "test").param("project", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("user", "test", "");
    }

    @Test
    public void testGetUserOrGroupAclPermissionsWithTypeError() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/access/acls").contentType(MediaType.APPLICATION_JSON)
                .param("type", "error").param("name", "test").param("project", "")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Mockito.verify(openAccessController).getUserOrGroupAclPermissions("error", "test", "");
    }

    @Test
    public void testConvertAccessRequests() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject("default");
        AclEntity ae = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        BatchProjectPermissionRequest request = new BatchProjectPermissionRequest();
        request.setNames(Lists.newArrayList("U5", "newUser"));
        request.setPermission("ADMIN");
        request.setProject("default");
        request.setType(MetadataConstants.TYPE_USER);
        List<AccessRequest> accessRequests = openAccessController.convertBatchPermissionRequestToAccessRequests(ae,
                request);
        Assert.assertEquals("U5", accessRequests.get(0).getSid());
        Assert.assertEquals("newUser", accessRequests.get(1).getSid());

        request.setType(MetadataConstants.TYPE_GROUP);
        request.setNames(Lists.newArrayList("newGroup"));
        accessRequests = openAccessController.convertBatchPermissionRequestToAccessRequests(ae, request);
        Assert.assertEquals("newGroup", accessRequests.get(0).getSid());
    }
}
