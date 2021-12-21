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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;

public class CaseInsensitiveKylinUserServiceTest extends NLocalFileMetadataTestCase {

    private CaseInsensitiveKylinUserService kylinUserService;

    @Before
    public void setup() {
        createTestMetadata();
        overwriteSystemProp("kylin.metadata.key-case-insensitive", "true");
        kylinUserService = Mockito.spy(new CaseInsensitiveKylinUserService());
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

    @Test
    public void testListAdminUsers() throws IOException {
        List<String> adminUsers = kylinUserService.listAdminUsers();
        Assert.assertEquals(1, adminUsers.size());
        Assert.assertTrue(adminUsers.contains("ADMIN"));
    }

    @Test
    public void testIsGlobalAdmin() throws IOException {
        Assert.assertTrue(kylinUserService.isGlobalAdmin("ADMIN"));
        Assert.assertTrue(kylinUserService.isGlobalAdmin("AdMIN"));

        Assert.assertFalse(kylinUserService.isGlobalAdmin("NOTEXISTS"));
    }

    @Test
    public void testRetainsNormalUser() throws IOException {
        Set<String> normalUsers = kylinUserService.retainsNormalUser(Sets.newHashSet("ADMIN", "adMIN", "NOTEXISTS"));
        Assert.assertEquals(1, normalUsers.size());
        Assert.assertTrue(normalUsers.contains("NOTEXISTS"));
    }

    @Test
    public void testContainsGlobalAdmin() throws IOException {
        Assert.assertTrue(kylinUserService.containsGlobalAdmin(Sets.newHashSet("ADMIN")));
        Assert.assertTrue(kylinUserService.containsGlobalAdmin(Sets.newHashSet("adMIN")));
        Assert.assertFalse(kylinUserService.containsGlobalAdmin(Sets.newHashSet("adMI N")));
    }
}