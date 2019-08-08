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

package io.kyligence.kap.rest.service;

import static java.util.stream.Collectors.toSet;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.authenticated;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.unauthenticated;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ldap.test.unboundid.LdapTestUtils;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.user.ManagedUser;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextHierarchy({ @ContextConfiguration(locations = { "classpath:applicationContext.xml" }),
        @ContextConfiguration(locations = { "classpath:kylinSecurity.xml" }) })
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({ "ldap", "ldap-test" })
public class LdapUserServiceTest extends NLocalFileMetadataTestCase {

    private static final String LDAP_CONFIG = "ut_ldap/ldap-config.properties";

    private static final String LDAP_SERVER = "ut_ldap/ldap-server.ldif";

    private static InMemoryDirectoryServer directoryServer;

    @Autowired
    private WebApplicationContext context;

    private MockMvc mockMvc;

    @Autowired
    @Qualifier("userService")
    LdapUserService ldapUserService;

    @Autowired
    @Qualifier("userGroupService")
    LdapUserGroupService userGroupService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();
        Properties ldapConfig = new Properties();
        ldapConfig.load(new FileInputStream(new ClassPathResource(LDAP_CONFIG).getFile()));
        final KylinConfig kylinConfig = getTestConfig();
        ldapConfig.forEach((k, v) -> kylinConfig.setProperty(k.toString(), v.toString()));

        String dn = ldapConfig.getProperty("kylin.security.ldap.connection-username");
        String password = ldapConfig.getProperty("kylin.security.ldap.connection-password");
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials(dn, password);
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("LDAP", 8389));
        config.setEnforceSingleStructuralObjectClass(false);
        config.setEnforceAttributeSyntaxCompliance(true);
        directoryServer = new InMemoryDirectoryServer(config);
        directoryServer.startListening();
        LdapTestUtils.loadLdif(directoryServer, new ClassPathResource(LDAP_SERVER));
    }

    @AfterClass
    public static void cleanupResource() throws Exception {
        directoryServer.shutDown(true);
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).apply(springSecurity()).build();
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testLoginWithValidUser() throws Exception {

        SecurityMockMvcRequestBuilders.FormLoginRequestBuilder login = formLogin().user("johnny")
                .password("example123");

        mockMvc.perform(login).andExpect(authenticated().withUsername("johnny"));
        //login again, cache hit
        mockMvc.perform(login).andExpect(authenticated().withUsername("johnny"));
    }

    @Test
    public void testLoginWithInvalidUser() throws Exception {
        SecurityMockMvcRequestBuilders.FormLoginRequestBuilder login = formLogin().user("invaliduser")
                .password("invalidpassword");

        mockMvc.perform(login).andExpect(unauthenticated());
    }

    @Test
    public void testCreateUser() {
        thrown.expect(UnsupportedOperationException.class);
        ldapUserService.createUser(null);
    }

    @Test
    public void testUpdateUser() {
        thrown.expect(UnsupportedOperationException.class);
        ldapUserService.updateUser(null);
    }

    @Test
    public void testDeleteUser() {
        thrown.expect(UnsupportedOperationException.class);
        ldapUserService.deleteUser("ben");
    }

    @Test
    public void testChangePassword() {
        thrown.expect(UnsupportedOperationException.class);
        ldapUserService.changePassword("old", "new");
    }

    @Test
    public void testUserExists() {
        Assert.assertTrue(ldapUserService.userExists("johnny"));
    }

    @Test
    public void testUserNotExists() {
        Assert.assertFalse(ldapUserService.userExists("ben"));
    }

    @Test
    public void testListUsers() throws Exception {
        Set<String> users = ldapUserService.listUsers().stream().map(x -> x.getUsername()).collect(toSet());
        Assert.assertEquals(3, users.size());
    }

    @Test
    public void testListAdminUsers() throws Exception {
        Assert.assertEquals("jenny", ldapUserService.listAdminUsers().get(0));
    }

    @Test
    public void testLoadUserByUsername() {
        Assert.assertTrue(ldapUserService.loadUserByUsername("jenny").getAuthorities().stream()
                .map(x -> x.getAuthority()).collect(toSet()).contains("ROLE_ADMIN"));
    }

    @Test
    public void testCompleteUserInfo() {
        ManagedUser user = new ManagedUser("oliver", "", false);
        ldapUserService.completeUserInfo(user);
        Set<String> authorities = user.getAuthorities().stream().map(x -> x.getAuthority()).collect(toSet());
        Assert.assertFalse(authorities.contains("ROLE_ADMIN"));
        Assert.assertTrue(authorities.contains("itpeople"));
    }

    @Test
    public void testOnNewUserAdded() throws Exception {
        Assert.assertFalse(ldapUserService.userExists("rick"));
        directoryServer.add("dn: cn=rick,ou=People,dc=example,dc=com", "objectClass: inetOrgPerson",
                "objectClass: organizationalPerson", "objectClass: person", "objectClass: top", "cn: rick",
                "sn: rick gan", "mail: rick@example.io", "ou: Modeler", "userPassword:: ZXhhbXBsZTEyMw==");
        ldapUserService.onUserAuthenticated("rick");
        Assert.assertTrue(ldapUserService.userExists("rick"));
    }
}
