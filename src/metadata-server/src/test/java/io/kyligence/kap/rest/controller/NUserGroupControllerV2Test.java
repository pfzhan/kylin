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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.List;

import org.apache.kylin.rest.constant.Constant;
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
import io.kyligence.kap.rest.controller.v2.NUserGroupControllerV2;
import io.kyligence.kap.rest.service.NUserGroupService;
import lombok.val;

public class NUserGroupControllerV2Test {

    private MockMvc mockMvc;

    @Mock
    private NUserGroupService userGroupService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NUserGroupControllerV2 nUserGroupControllerV2 = Mockito.spy(new NUserGroupControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();
        mockMvc = MockMvcBuilders.standaloneSetup(nUserGroupControllerV2)
                .setContentNegotiationManager(contentNegotiationManager).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    private List<ManagedUser> mockManagedUser() {
        val user1 = new ManagedUser();
        user1.setUsername("user1");
        val user2 = new ManagedUser();
        user1.setUsername("user2");
        return Lists.newArrayList(user1, user2);
    }

    @Test
    public void testGetUserWithGroup() throws Exception {
        Mockito.doReturn(null).when(userGroupService).listAllAuthorities();
        Mockito.doReturn(mockManagedUser()).when(userGroupService).getGroupMembersByName(Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/user_group/usersWithGroup")
                .contentType(MediaType.APPLICATION_JSON).param("pageOffset", "1").param("pageSize", "12")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nUserGroupControllerV2).getUsersWithGroup(1, 12, "");
    }

}
