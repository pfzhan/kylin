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

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders.formLogin;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.authenticated;
import static org.springframework.security.test.web.servlet.response.SecurityMockMvcResultMatchers.unauthenticated;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.metadata.usergroup.UserGroup;
import org.apache.kylin.rest.response.UserGroupResponseKI;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.ldap.test.unboundid.LdapTestUtils;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestBuilders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextHierarchy({ @ContextConfiguration(locations = { "classpath:applicationContext.xml" }),
        @ContextConfiguration(locations = { "classpath:kylinSecurity.xml" }) })
@WebAppConfiguration(value = "src/main/resources")
@ActiveProfiles({ "ldap", "ldap-test", "test" })
public class LdapUserServiceTest extends NLocalFileMetadataTestCase {

    private static final String LDAP_CONFIG = "ut_ldap/ldap-config.properties";

    private static final String LDAP_SERVER = "ut_ldap/ldap-server.ldif";

    private static InMemoryDirectoryServer directoryServer;

    @Autowired
    private WebApplicationContext context;

    private MockMvc mockMvc;

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

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
        overwriteSystemPropBeforeClass("kylin.security.ldap.max-page-size", "1");
        ldapConfig.forEach((k, v) -> kylinConfig.setProperty(k.toString(), v.toString()));

        String dn = ldapConfig.getProperty("kylin.security.ldap.connection-username");
        String password = ldapConfig.getProperty("kylin.security.ldap.connection-password");
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
        config.addAdditionalBindCredentials(dn, EncryptUtil.decrypt(password));
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("LDAP", 8389));
        config.setEnforceSingleStructuralObjectClass(false);
        config.setEnforceAttributeSyntaxCompliance(true);
        config.setMaxSizeLimit(1);
        directoryServer = new InMemoryDirectoryServer(config);
        directoryServer.startListening();
        log.info("current directory server listen on {}", directoryServer.getListenPort());
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
        //Ignore case
        Assert.assertTrue(ldapUserService.userExists("JOHNNY"));
    }

    @Test
    public void testUserNotExists() {
        Assert.assertFalse(ldapUserService.userExists("ben"));
    }

    @Test
    public void testListUsers() throws Exception {
        Set<String> users = ldapUserService.listUsers().stream().map(x -> x.getUsername()).collect(toSet());
        Assert.assertEquals(4, users.size());
        List<ManagedUser> managedUserList = ldapUserService.listUsers();
        for (val user : managedUserList) {
            Assert.assertTrue(user.getAuthorities().size() > 1);
        }
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
    public void testCompleteUserInfoInternal() {
        ManagedUser user = new ManagedUser("oliver", "", false);
        ldapUserService.completeUserInfoInternal(user);
        Set<String> authorities = user.getAuthorities().stream().map(x -> x.getAuthority()).collect(toSet());
        Assert.assertFalse(authorities.contains("ROLE_ADMIN"));
        Assert.assertTrue(authorities.contains("itpeople"));
    }

    @Test
    public void testCompleteUserInfoWithNotExistUser() {
        ManagedUser user = new ManagedUser("NotExist", "", false);
        ldapUserService.completeUserInfo(user);
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

    @Test
    public void testAddGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.addGroup("gg");
    }

    @Test
    public void testUpdateUserGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.modifyGroupUsers("gg", Lists.newArrayList());
    }

    @Test
    public void testDeleteUserGroup() {
        thrown.expect(UnsupportedOperationException.class);
        userGroupService.deleteGroup("gg");
    }

    @Test
    public void testGetAllUserGroups() {
        List<String> groups = userGroupService.getAllUserGroups();
        Assert.assertTrue(groups.contains("admin"));
        Assert.assertTrue(groups.contains("itpeople"));
        Assert.assertTrue(groups.contains("empty"));
    }

    @Test
    public void testGetUserAndUserGroup() {
        Map<String, List<String>> groupUsers = userGroupService.getUserAndUserGroup();
        Assert.assertTrue(groupUsers.containsKey("admin"));
        Assert.assertTrue(groupUsers.containsKey("itpeople"));
        Assert.assertTrue(groupUsers.get("admin").contains("jenny"));
        Assert.assertTrue(groupUsers.get("itpeople").contains("johnny"));
        Assert.assertTrue(groupUsers.get("itpeople").contains("oliver"));
        Assert.assertTrue(groupUsers.get("empty").isEmpty());
    }

    @Test
    public void testGetGroupMembersByName() {
        Set<String> users = userGroupService.getGroupMembersByName("itpeople").stream().map(ManagedUser::getUsername)
                .collect(toSet());
        Assert.assertTrue(users.contains("johnny"));
        Assert.assertTrue(users.contains("oliver"));
        List<ManagedUser> managedUsers = userGroupService.getGroupMembersByName("itpeople");
        for (val user : managedUsers) {
            Assert.assertTrue(user.getAuthorities().contains(new SimpleGrantedAuthority("itpeople")));
        }

        users = userGroupService.getGroupMembersByName("empty").stream().map(ManagedUser::getUsername).collect(toSet());
        Assert.assertTrue(users.isEmpty());
    }

    @Test
    public void testGetUserGroupsFilterByGroupName() {
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(userGroupService, "aclEvaluate", aclEvaluate);
        List<UserGroup> groups = userGroupService.getUserGroupsFilterByGroupName(null);
        Assert.assertEquals(3, groups.size());

        groups = userGroupService.getUserGroupsFilterByGroupName("");
        Assert.assertEquals(3, groups.size());

        groups = userGroupService.getUserGroupsFilterByGroupName("i");
        Assert.assertEquals(2, groups.size());
        Assert.assertTrue(groups.stream().map(UserGroup::getGroupName).anyMatch(name -> name.contains("admin")));
        Assert.assertTrue(groups.stream().map(UserGroup::getGroupName).anyMatch(name -> name.contains("itpeople")));
    }

    @Test
    public void testGetUserGroupResponse() throws IOException {
        List<String> allUserGroups = userGroupService.getAllUserGroups();
        List<UserGroupResponseKI> userGroupResponse = userGroupService
                .getUserGroupResponse(allUserGroups.stream().map(UserGroup::new).collect(Collectors.toList()));
        Map<String, Set<String>> groupUsersMap = userGroupResponse.stream()
                .map(UserGroupResponseKI::getUserGroupAndUsers).collect(toMap(Pair::getKey, Pair::getValue));
        Assert.assertTrue(groupUsersMap.containsKey("admin"));
        Assert.assertTrue(groupUsersMap.containsKey("itpeople"));
        Assert.assertTrue(groupUsersMap.get("admin").contains("jenny"));
        Assert.assertTrue(groupUsersMap.get("itpeople").contains("johnny"));
        Assert.assertTrue(groupUsersMap.get("itpeople").contains("oliver"));
        Assert.assertTrue(groupUsersMap.get("empty").isEmpty());
    }

    @Test
    public void testGroupNameByUuidAndUuidByGroupName() throws IOException {
        List<UserGroupResponseKI> userGroupResponse = userGroupService.getUserGroupResponse(
                userGroupService.getAllUserGroups().stream().map(UserGroup::new).collect(Collectors.toList()));
        userGroupResponse.stream().forEach(response -> {
            String groupName = response.getGroupName();
            String uuidByGroupName = userGroupService.getUuidByGroupName(response.getGroupName());
            Assert.assertEquals(groupName, uuidByGroupName);
            String uuid = response.getUuid();
            String groupNameByUuid = userGroupService.getGroupNameByUuid(response.getUuid());
            Assert.assertEquals(uuid, groupNameByUuid);
        });

    }

    @Test
    public void testGetDnMapperMap() throws Exception {
        String cacheKey = ReflectionTestUtils.getField(LdapUserService.class, "LDAP_VALID_DN_MAP_KEY").toString();
        Cache cache = (Cache) ReflectionTestUtils.getField(LdapUserService.class, "LDAP_VALID_DN_MAP_CACHE");
        cache.invalidate(cacheKey);
        Map<String, String> dnMapperMap = ldapUserService.getDnMapperMap();
        Assert.assertTrue(dnMapperMap instanceof ImmutableMap);
    }

    @Test
    public void testSameNameUserInvalidation() {
        Assert.assertFalse(
                ldapUserService.listUsers().stream().map(ManagedUser::getUsername).collect(toSet()).contains("user"));
        Assert.assertFalse(userGroupService.getUserAndUserGroup().entrySet().stream().map(Map.Entry::getValue)
                .map(HashSet::new).map(set -> set.contains("user")).reduce(Boolean::logicalOr).get().booleanValue());
    }

    @Test
    public void testMultipleCNs() {
        UserDetails user = ldapUserService.loadUserByUsername("user1");

        Assert.assertTrue(user.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(Collectors.toSet())
                .contains("itpeople"));
    }

}
