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
import java.util.List;

import org.apache.kylin.rest.service.UserGrantedAuthority;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import io.kyligence.kap.rest.controller.KapUserController.UserObj;
import io.kyligence.kap.rest.service.ServiceTestBase;

/**
 */
public class KapUserControllerTest extends ServiceTestBase {

    @Autowired
    KapUserController kapUserController;

    BCryptPasswordEncoder pwdEncoder = new BCryptPasswordEncoder();

    @Test
    public void testBasics() throws IOException {
        kapUserController.delete("TEST");

        // save
        UserObj u = kapUserController.save("TEST", new UserObj("TEST", "pwd", true, "R1", "R2", "R3"));
        assertEquals(u, "TEST", "pwd", false, "R1", "R2", "R3");

        // update
        u = kapUserController.save("TEST", new UserObj("TEST", "pwd22", true, "R4", "R5"));
        assertEquals(u, "TEST", "pwd22", false, "R4", "R5");

        // disable
        UserObj disable = new UserObj();
        disable.setDisabled(true);
        disable.setPassword("abc.1234");
        u = kapUserController.save("TEST", disable);
        assertEquals(u, "TEST", "abc.1234", true, "R4", "R5");

        // list all
        for (UserObj uu : kapUserController.listAllUsers()) {
            if ("TEST".equals(uu.getUsername())) {
                assertEquals(u, "TEST", "abc.1234", true, "R4", "R5");
            }
        }

        // list authorities
        List<String> authorities = kapUserController.listAllAuthorities();
        Assert.assertTrue(authorities.contains("R4"));
        Assert.assertTrue(authorities.contains("R5"));

        kapUserController.delete("TEST");

        // exception getting non-exist user
        try {
            kapUserController.get("TEST");
            Assert.fail();
        } catch (UsernameNotFoundException e) {
            // expected
        }
    }

    private void assertEquals(UserObj u, String username, String password, boolean disabled, String... authorities) {
        Assert.assertEquals(username, u.getUsername());
        Assert.assertTrue(pwdEncoder.matches(password, u.getPassword()));
        Assert.assertEquals(disabled, u.isDisabled());
        Assert.assertEquals(authorities.length, u.getAuthorities().size());
        for (String a : authorities) {
            Assert.assertTrue(u.getAuthorities().contains(new UserGrantedAuthority(a)));
        }
    }
}
