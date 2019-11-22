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

import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.service.AclTCRService;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

public class AclTCRControllerTest {

    private MockMvc mockMvc;

    @Mock
    private UserService userService;

    @Mock
    private IUserGroupService userGroupService;

    @Mock
    private AclTCRService aclTCRService;

    @InjectMocks
    private AclTCRController aclTCRController = Mockito.spy(new AclTCRController());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(aclTCRController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void testEmptySid() throws IOException {
        thrown.expect(BadRequestException.class);
        aclTCRController.checkSid("", true);
    }

    @Test
    public void testInvalidSidPattern() throws IOException {
        thrown.expect(BadRequestException.class);
        aclTCRController.checkSid("1#s$", true);
    }

    @Test
    public void testInvalidUser() throws IOException {
        Mockito.doReturn(false).when(userService).userExists(Mockito.anyString());
        thrown.expect(BadRequestException.class);
        aclTCRController.checkSid("uuu", true);
    }

    @Test
    public void testInvalidGroup() throws IOException {
        Mockito.doReturn(false).when(userGroupService).exists(Mockito.anyString());
        thrown.expect(BadRequestException.class);
        aclTCRController.checkSid("ggg", false);
    }

    @Test
    public void testGetProjectSidTCR() throws Exception {

        Mockito.doReturn(true).when(userService).userExists(Mockito.anyString());
        Mockito.doReturn(true).when(userGroupService).exists(Mockito.anyString());

        Mockito.doReturn(Lists.newArrayList()).when(aclTCRService).getAclTCRResponse(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyBoolean());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/sid/{sidType}/{sid}", "user", "u1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCR("user", "u1", "default", false);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/acl/sid/{sidType}/{sid}", "group", "g1") //
                .param("project", "default") //
                .param("authorizedOnly", "false") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(aclTCRController).getProjectSidTCR("group", "g1", "default", false);
    }

    @Test
    public void testUpdateProject() throws Exception {

        Mockito.doReturn(true).when(userService).userExists(Mockito.anyString());
        Mockito.doReturn(true).when(userGroupService).exists(Mockito.anyString());

        Mockito.doNothing().when(aclTCRService).updateAclTCR(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyBoolean(), Mockito.anyList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/acl/sid/{sidType}/{sid}", "user", "u1") //
                .param("project", "default") //
                .content(JsonUtil.writeValueAsBytes(Lists.<AclTCRRequest> newArrayList())) //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(aclTCRController).updateProject("user", "u1", "default", Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/acl/sid/{sidType}/{sid}", "group", "g1") //
                .param("project", "default") //
                .content(JsonUtil.writeValueAsBytes(Lists.<AclTCRRequest> newArrayList())) //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(aclTCRController).updateProject("group", "g1", "default", Lists.newArrayList());
    }
}
