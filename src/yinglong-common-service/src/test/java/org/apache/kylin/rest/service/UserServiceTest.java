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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rest.constant.Constant;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.metadata.user.ManagedUser;

public class UserServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Test
    public void testBasics() {
        userService.deleteUser("MODELER");

        Assert.assertTrue(!userService.userExists("MODELER"));

        List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
        authorities.add(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
        ManagedUser user = new ManagedUser("MODELER", "PWD", false, authorities);
        userService.createUser(user);

        Assert.assertTrue(userService.userExists("MODELER"));

        UserDetails ud = userService.loadUserByUsername("MODELER");
        Assert.assertEquals("MODELER", ud.getUsername());
        Assert.assertEquals("PWD", ud.getPassword());
        Assert.assertEquals(Constant.ROLE_ADMIN, ud.getAuthorities().iterator().next().getAuthority());
        Assert.assertEquals(2, ud.getAuthorities().size());

    }

    @Test
    public void testAddExistingUser() {
        List<GrantedAuthority> authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority(Constant.ROLE_ADMIN));
        ManagedUser user = new ManagedUser("ADMIN", "PWD", false, authorities);
        try {
            userService.createUser(user);
            Assert.fail();
        } catch (TransactionException e) {
            Assert.assertTrue(StringUtils.contains(e.getCause().getCause().getMessage(),
                    "Username:[ADMIN] already exists"));
        }
    }

    @Test
    public void testDeleteAdmin() {
        try {
            userService.deleteUser("ADMIN");
            Assert.fail();
        } catch (TransactionException e) {
            Assert.assertTrue(StringUtils.contains(e.getCause().getCause().getMessage(),
                    "User ADMIN is not allowed to be deleted."));
        }

    }

    @Test
    public void testListAdminUsers() throws IOException {
        List<String> adminUsers = userService.listAdminUsers();
        Assert.assertEquals(1, adminUsers.size());
        Assert.assertTrue(adminUsers.contains("ADMIN"));
    }

    @Test
    public void testIsGlobalAdmin() throws IOException {
        Assert.assertTrue(userService.isGlobalAdmin("ADMIN"));
        Assert.assertTrue(userService.isGlobalAdmin("AdMIN"));

        Assert.assertFalse(userService.isGlobalAdmin("NOTEXISTS"));
    }

    @Test
    public void testRetainsNormalUser() throws IOException {
        Set<String> normalUsers = userService.retainsNormalUser(Sets.newHashSet("ADMIN", "adMIN", "NOTEXISTS"));
        Assert.assertEquals(1, normalUsers.size());
        Assert.assertTrue(normalUsers.contains("NOTEXISTS"));
    }

    @Test
    public void testContainsGlobalAdmin() throws IOException {
        Assert.assertTrue(userService.containsGlobalAdmin(Sets.newHashSet("ADMIN")));
        Assert.assertTrue(userService.containsGlobalAdmin(Sets.newHashSet("adMIN")));
        Assert.assertFalse(userService.containsGlobalAdmin(Sets.newHashSet("adMI N")));
    }
}
