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
package io.kyligence.kap.reset.controller;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.util.Arrays;
import java.util.Collections;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;

public class NUserControllerTest extends AbstractMVCIntegrationTestCase {

    static BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    ManagedUser request;
    String username = "test_user";
    String password = "1234567890Q!";

    @Override
    public void setUp() {
        super.setUp();

        try {
            request = new ManagedUser();
            request.setUsername(username);
            request.setPassword(Base64.encodeBase64String(password.getBytes("utf-8")));
            request.setDisabled(false);
            request.setAuthorities(Collections.singletonList(new SimpleGrantedAuthority(GROUP_ALL_USERS)));
            mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                    .content(JsonUtil.writeValueAsString(request))
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveUser() throws Exception {
        request.setUsername(username.toUpperCase());
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        Assert.assertTrue(result.getResponse().getContentAsString().contains("Username:[test_user] already exists"));
    }

    @Test
    public void testCreateUserWithBase64EncodePwd() {
        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(password, user.getPassword()));
    }

    @Test
    public void testUpdateUserPasswordWithBase64EncodePwd() throws Exception {

        String newPassword = "kylin@2020";

        PasswordChangeRequest passwordChangeRequest = new PasswordChangeRequest();
        passwordChangeRequest.setUsername(username);
        passwordChangeRequest.setPassword(Base64.encodeBase64String(password.getBytes("utf-8")));
        passwordChangeRequest.setNewPassword(Base64.encodeBase64String(newPassword.getBytes("utf-8")));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/user/password").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(passwordChangeRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(newPassword, user.getPassword()));
    }

    @Test
    public void testUpdateUserWithBase64EncodePwd() throws Exception {
        String newPassword = "kylin@2022";
        request = new ManagedUser();
        request.setUsername(username);
        request.setPassword(Base64.encodeBase64String(newPassword.getBytes("utf-8")));
        request.setDisabled(false);
        request.setAuthorities(
                Arrays.asList(new SimpleGrantedAuthority(GROUP_ALL_USERS), new SimpleGrantedAuthority(ROLE_ADMIN)));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ManagedUser user = NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv()).get(username);

        Assert.assertNotEquals(null, user);
        Assert.assertTrue(pwdEncoder.matches(newPassword, user.getPassword()));
        Assert.assertEquals(2, user.getAuthorities().size());
        Assert.assertTrue(user.getAuthorities().contains(new SimpleGrantedAuthority(GROUP_ALL_USERS)));
        Assert.assertTrue(user.getAuthorities().contains(new SimpleGrantedAuthority(ROLE_ADMIN)));
    }

    @Test
    public void testSaveUserReturnValueHasNotSensitiveElements() throws Exception {
        ManagedUser request = new ManagedUser();
        request.setUsername("test_user_2");
        request.setPassword("1234567890Q!");
        request.setDisabled(false);
        request.setAuthorities(Collections.singletonList(new SimpleGrantedAuthority(GROUP_ALL_USERS)));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/user").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andExpect(jsonPath("$.password").doesNotExist())
                .andExpect(jsonPath("$.default_password").doesNotExist());

    }
}
