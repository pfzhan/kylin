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
package org.apache.kylin.rest.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinUserServiceTest extends NLocalFileMetadataTestCase {

    private KylinUserService kylinUserService;

    @Before
    public void setup() {
        createTestMetadata();
        kylinUserService = Mockito.spy(new KylinUserService());
        NKylinUserManager userManager = NKylinUserManager.getInstance(getTestConfig());
        userManager.update(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void loadUserByUsername() {
    }

    @Test
    public void testLoadUser() {
        UserDetails user = kylinUserService.loadUserByUsername("ADMIN");
        Assert.assertEquals("ADMIN", user.getUsername());
        user = kylinUserService.loadUserByUsername("AdMIn");
        Assert.assertEquals("ADMIN", user.getUsername());
    }

    @Test(expected = UsernameNotFoundException.class)
    public void testLoadUserWithWhiteSpace() {
        String username = "ADMI N";
        kylinUserService.loadUserByUsername(username);
    }

    @Test
    public void testUpdateUser() {
        String username = "ADMIN";
        ManagedUser user = (ManagedUser) kylinUserService.loadUserByUsername(username);
        Assert.assertFalse(user.isLocked());
        user.setLocked(true);
        kylinUserService.updateUser(user);
        user = (ManagedUser) kylinUserService.loadUserByUsername(username);
        Assert.assertTrue(user.isLocked());
        user.setLocked(false);
        kylinUserService.updateUser(user);
    }

    @Test
    public void testUserExists() {
        ManagedUser user = new ManagedUser();
        user.setUsername("tTtUser");
        List<SimpleGrantedAuthority> roles = new ArrayList<>();
        roles.add(new SimpleGrantedAuthority("ALL_USERS"));
        user.setGrantedAuthorities(roles);
        kylinUserService.createUser(user);
        Assert.assertTrue(kylinUserService.userExists("tTtUser"));
        Assert.assertTrue(kylinUserService.userExists("tttuser"));
        Assert.assertTrue(kylinUserService.userExists("TTTUSER"));
        Assert.assertFalse(kylinUserService.userExists("NOTEXIST"));
    }

}