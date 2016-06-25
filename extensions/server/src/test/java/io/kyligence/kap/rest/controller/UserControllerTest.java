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
import java.util.List;

import org.apache.kylin.rest.service.UserService.UserGrantedAuthority;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import io.kyligence.kap.rest.ServiceTestBase;
import io.kyligence.kap.rest.controller.UserController.UserObj;

/**
 */
public class UserControllerTest extends ServiceTestBase {

    @Autowired
    UserController userController;

    @Test
    public void testBasics() throws IOException {
        userController.delete("TEST");

        // save
        UserObj u = userController.save("TEST", new UserObj("TEST", "pwd", "R1", "R2", "R3"));
        assertEquals(u, "TEST", "pwd", false, "R1", "R2", "R3");

        // update
        u = userController.save("TEST", new UserObj("TEST", "pwd22", "R4", "R5"));
        assertEquals(u, "TEST", "pwd22", false, "R4", "R5");

        // disable
        UserObj disable = new UserObj();
        disable.setDisabled(true);
        u = userController.save("TEST", disable);
        assertEquals(u, "TEST", "pwd22", true, "R4", "R5");

        // list all
        for (UserObj uu : userController.listAllUsers()) {
            if ("TEST".equals(uu.getUsername())) {
                assertEquals(u, "TEST", "pwd22", true, "R4", "R5");
            }
        }

        // list authorities
        List<String> authorities = userController.listAllAuthorities();
        Assert.assertTrue(authorities.contains("R4"));
        Assert.assertTrue(authorities.contains("R5"));

        userController.delete("TEST");

        // exception getting non-exist user
        try {
            userController.get("TEST");
            Assert.fail();
        } catch (UsernameNotFoundException e) {
            // expected
        }
    }

    private void assertEquals(UserObj u, String username, String password, boolean disabled, String... authorities) {
        Assert.assertEquals(username, u.getUsername());
        Assert.assertTrue(UserController.pwdMatches(password, u.getPassword()));
        Assert.assertEquals(disabled, u.isDisabled());
        Assert.assertEquals(authorities.length, u.getAuthorities().size());
        for (String a : authorities) {
            Assert.assertTrue(u.getAuthorities().contains(new UserGrantedAuthority(a)));
        }
    }
}
