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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.rest.request.PasswordChangeRequest;
import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NUserControllerTest {

    private MockMvc mockMvc;

    @InjectMocks
    private NUserController nUserController = Mockito.spy(new NUserController());

    @Mock
    private UserService userService;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private AccessService accessService;

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nUserController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();
        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        ManagedUser user = new ManagedUser("ADMIN", "ADMIN", false, authorities);
        Authentication authentication = new TestingAuthenticationToken(user, "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @BeforeClass
    public static void setupProfile() throws IOException {
        System.setProperty("spring.profiles.active", "testing");
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testBasics() throws IOException {
        EnvelopeResponse<UserDetails> userDetailsEnvelopeResponse = nUserController.authenticatedUser();
        Assert.assertNotNull(userDetailsEnvelopeResponse);
        Assert.assertTrue(userDetailsEnvelopeResponse.code.equals(ResponseCode.CODE_SUCCESS));
    }

    @Test
    public void testCreateUser() throws Exception {
        val user = new ManagedUser();
        user.setUsername("u1");
        user.setPassword("p1");
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).createUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testCreateUser_PasswordLenth_exception() throws Exception {
        val user = new ManagedUser();
        user.setUsername("u1");
        user.setPassword("p1");
        Mockito.doNothing().when(userService).createUser(Mockito.any(UserDetails.class));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(user))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).createUser(Mockito.any(ManagedUser.class));
    }

    @Test
    public void testDelUser() throws Exception {
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(
                MockMvcRequestBuilders.delete("/api/user/{username}", "u1").contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nUserController).delete("u1");
    }

    @Test
    public void testUpdatePasswordUser_UserNotFound() throws Exception {
        val request = new PasswordChangeRequest();
        request.setUsername("ADMIN");
        request.setPassword("KYLIN");
        request.setNewPassword("KYLIN1234@");
        Mockito.doNothing().when(userService).deleteUser(Mockito.anyString());
        Mockito.doNothing().when(accessService).revokeProjectPermission(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        Mockito.verify(nUserController).updateUserPassword(Mockito.any(PasswordChangeRequest.class));
    }

}
