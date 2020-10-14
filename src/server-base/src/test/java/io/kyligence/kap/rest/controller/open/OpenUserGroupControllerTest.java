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

import io.kyligence.kap.rest.request.UserGroupRequest;
import org.apache.kylin.common.response.ResponseCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.accept.ContentNegotiationManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.controller.NUserGroupController;
import io.kyligence.kap.rest.request.UpdateGroupRequest;
import io.kyligence.kap.rest.service.NUserGroupService;
import lombok.val;

public class OpenUserGroupControllerTest {
    private MockMvc mockMvc;

    @Mock
    private NUserGroupService userGroupService;

    @Mock
    private NUserGroupController userGroupController;

    @InjectMocks
    private OpenUserGroupController openUserGroupController = Mockito.spy(new OpenUserGroupController());


    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        FilterProvider filterProvider = new SimpleFilterProvider().addFilter("passwordFilter",
                SimpleBeanPropertyFilter.serializeAllExcept("password", "defaultPassword"));
        ObjectMapper objectMapper = new ObjectMapper().setFilterProvider(filterProvider);
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(objectMapper);
        MockitoAnnotations.initMocks(this);
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(openUserGroupController).setMessageConverters(converter)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void testGetGroups() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/groups").contentType(MediaType.APPLICATION_JSON)
                .param("group_name", "").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openUserGroupController).listGroups(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void testGetUsersByGroup() throws Exception {
        Mockito.doReturn(
                new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(Lists.newArrayList(), 0, 10), ""))
                .when(userGroupController).getUsersByGroupName("g1@.h", "", 0, 10);

        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/group_members/{group_name:.+}", "g1@.h")
                .contentType(MediaType.APPLICATION_JSON)
                .param("username", "")
                .param("page_offset", "0")
                .param("page_size", "10").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openUserGroupController)
                .getUsersByGroup("g1@.h", "", 0, 10);
    }

    @Test
    public void testAddGroup() throws Exception {
        Mockito.doNothing().when(userGroupService).addGroup("g1");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user_group/{groupName:.+}", "g1")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserGroupController).addUserGroup("g1");
    }

    @Test
    public void testAddGroupWithBody() throws Exception {
        Mockito.doNothing().when(userGroupService).addGroup("g1");
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName("g1");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user_group")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserGroupController).addUserGroupWithBody(request);
    }

    @Test
    public void testDelGroup() throws Exception {
        Mockito.doNothing().when(userGroupService).deleteGroup("g1@.h");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user_group/{groupName:.+}", "g1@.h")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserGroupController).delUserGroup("g1@.h");
    }

    @Test
    public void testDelGroupWithBody() throws Exception {
        Mockito.doNothing().when(userGroupService).deleteGroup("g1@.h");
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName("g1@.h");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/user_group")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserGroupController).delUserGroupWithBody(request);
    }

    @Test
    public void testAddOrDelUser() throws Exception {
        val request = new UpdateGroupRequest();
        request.setGroup("g1");
        request.setUsers(Lists.newArrayList("u1", "u2"));
        Mockito.doNothing().when(userGroupService).modifyGroupUsers("g1", request.getUsers());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user_group/users").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openUserGroupController).addOrDelUsersInGroup(Mockito.any(UpdateGroupRequest.class));
    }
}
