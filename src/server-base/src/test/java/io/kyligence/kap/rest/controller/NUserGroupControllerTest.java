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

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.service.UserService;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.UpdateGroupRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.NUserGroupService;
import lombok.val;

public class NUserGroupControllerTest {

    private MockMvc mockMvc;

    @Mock
    private NUserGroupService userGroupService;

    @Mock
    private AclTCRService aclTCRService;

    @Mock
    private UserService userService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NUserGroupController nUserGroupController = Mockito.spy(new NUserGroupController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserGroupController)
                .setContentNegotiationManager(contentNegotiationManager)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetUsersByGroup() throws Exception {
        Mockito.doReturn(mockManagedUser()).when(userGroupService).getGroupMembersByName(Mockito.anyString());
        Mockito.doNothing().when(aclTCRService).revokeAclTCR(Mockito.anyString(), Mockito.anyBoolean());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/groupMembers/{groupName:.+}", "g1@.h")
                .contentType(MediaType.APPLICATION_JSON).param("pageOffset", "0").param("pageSize", "10")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupController).getUsersByGroup("g1@.h", 0, 10);
    }

    @Test
    public void testGetGroups() throws Exception {
        Mockito.doReturn(null).when(userGroupService).listAllAuthorities(Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/groups").contentType(MediaType.APPLICATION_JSON)
                .param("project", "").accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupController).listUserAuthorities("");
    }

    @Test
    public void testGetUserWithGroup() throws Exception {
        Mockito.doReturn(null).when(userGroupService).listAllAuthorities(Mockito.anyString());
        Mockito.doReturn(mockManagedUser()).when(userGroupService).getGroupMembersByName(Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/usersWithGroup")
                .contentType(MediaType.APPLICATION_JSON).param("pageOffset", "0").param("pageSize", "10")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupController).getUsersWithGroup(0, 10, "");
    }

    @Test
    public void testAddGroup() throws Exception {
        Mockito.doNothing().when(userGroupService).addGroup("g1@.h");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user_group/{groupName:.+}", "g1@.h")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).addUserGroup("g1@.h");
    }

    @Test
    public void testAddEmptyGroup() throws Exception {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("User group name should not be empty.");
        nUserGroupController.addUserGroup("");
    }

    @Test
    public void testDelGroup() throws Exception {
        Mockito.doNothing().when(userGroupService).deleteGroup("g1@.h");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user_group/{groupName:.+}", "g1@.h")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).delUserGroup("g1@.h");
    }

    @Test
    public void testAddOrDelUser() throws Exception {
        val request = new UpdateGroupRequest();
        request.setGroup("g1");
        request.setUsers(Lists.newArrayList("u1", "u2"));
        Mockito.doNothing().when(userGroupService).modifyGroupUsers("g1", request.getUsers());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user_group/users").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nUserGroupController).addOrDelUsers(Mockito.any(UpdateGroupRequest.class));
    }

    private List<ManagedUser> mockManagedUser() {
        val user1 = new ManagedUser();
        user1.setUsername("user1");
        val user2 = new ManagedUser();
        user1.setUsername("user2");
        return Lists.newArrayList(user1, user2);
    }
}
