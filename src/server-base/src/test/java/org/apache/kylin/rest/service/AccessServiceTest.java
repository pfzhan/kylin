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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.ProjectService;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
public class AccessServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    AccessService accessService = Mockito.spy(AccessService.class);

    @InjectMocks
    ProjectService projectService = Mockito.spy(ProjectService.class);;

    @InjectMocks
    private IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);;

    @Mock
    AclService aclService = Mockito.spy(AclService.class);

    @Mock
    UserService userService = Mockito.spy(UserService.class);

    @Mock
    AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Mock
    AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(SpringContext.class);

        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata("src/test/resources/ut_access");
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);

        ReflectionTestUtils.setField(aclTCRService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclTCRService, "accessService", accessService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(aclTCRService, "userService", userService);
        ReflectionTestUtils.setField(aclTCRService, "userGroupService", userGroupService);

        // Init users
        ManagedUser adminUser = new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        ManagedUser modelerUser = new ManagedUser("MODELER", "MODELER", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST), new SimpleGrantedAuthority(Constant.ROLE_MODELER)));
        ManagedUser analystUser = new ManagedUser("ANALYST", "ANALYST", false, Arrays.asList(//
                new SimpleGrantedAuthority(Constant.ROLE_ANALYST)));

        List<ManagedUser> users = Lists.newArrayList(adminUser, modelerUser, analystUser);

        Mockito.when(userService.listUsers()).thenReturn(users);
        Mockito.when(userService.loadUserByUsername("ADMIN")).thenReturn(adminUser);
        Mockito.when(userService.loadUserByUsername("MODELER")).thenReturn(modelerUser);
        Mockito.when(userService.loadUserByUsername("ANALYST")).thenReturn(analystUser);
        Mockito.when(userService.userExists("ADMIN")).thenReturn(true);
        Mockito.when(userService.userExists("MODELER")).thenReturn(true);
        Mockito.when(userService.userExists("ANALYST")).thenReturn(true);
        Mockito.when(userService.getGlobalAdmin()).thenReturn(Sets.newHashSet("ADMIN"));

        // for SpringContext.getBean() in AclManager

        ApplicationContext applicationContext = PowerMockito.mock(ApplicationContext.class);
        PowerMockito.when(SpringContext.getApplicationContext()).thenReturn(applicationContext);
        PowerMockito.when(SpringContext.getBean(PermissionFactory.class))
                .thenReturn(PowerMockito.mock(PermissionFactory.class));
        PowerMockito.when(SpringContext.getBean(PermissionGrantingStrategy.class))
                .thenReturn(PowerMockito.mock(PermissionGrantingStrategy.class));
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        Sid adminSid = accessService.getSid("ADMIN", true);
        Assert.assertNotNull(adminSid);
        Assert.assertNotNull(AclPermissionFactory.getPermissions());

        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.clean(ae, true);
        AclEntity attachedEntity = new AclServiceTest.MockAclEntity("attached-domain-object");
        accessService.clean(attachedEntity, true);

        // test getAcl
        Acl acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        // test init
        acl = accessService.init(ae, AclPermission.ADMINISTRATION);
        Assert.assertTrue(((PrincipalSid) acl.getOwner()).getPrincipal().equals("ADMIN"));
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 1);
        AccessEntryResponse aer = accessService.generateAceResponses(acl).get(0);
        Assert.assertTrue(aer.getId() != null);
        Assert.assertTrue(aer.getPermission() == AclPermission.ADMINISTRATION);
        Assert.assertTrue(((PrincipalSid) aer.getSid()).getPrincipal().equals("ADMIN"));

        // test grant
        Sid modeler = accessService.getSid("MODELER", true);
        acl = accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 2);

        int modelerEntryId = 0;
        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertTrue(ace.getPermission() == AclPermission.ADMINISTRATION);
            }
        }

        // test update
        acl = accessService.update(ae, modelerEntryId, AclPermission.READ);

        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 2);

        for (AccessControlEntry ace : acl.getEntries()) {
            PrincipalSid sid = (PrincipalSid) ace.getSid();

            if (sid.getPrincipal().equals("MODELER")) {
                modelerEntryId = (Integer) ace.getId();
                Assert.assertTrue(ace.getPermission() == AclPermission.READ);
            }
        }

        accessService.clean(attachedEntity, true);

        Acl attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
        attachedEntityAcl = accessService.init(attachedEntity, AclPermission.ADMINISTRATION);

        accessService.inherit(attachedEntity, ae);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertTrue(attachedEntityAcl.getParentAcl() != null);
        Assert.assertTrue(
                attachedEntityAcl.getParentAcl().getObjectIdentity().getIdentifier().equals("test-domain-object"));
        Assert.assertTrue(attachedEntityAcl.getEntries().size() == 1);

        // test revoke
        acl = accessService.revoke(ae, modelerEntryId);
        Assert.assertEquals(accessService.generateAceResponses(acl).size(), 1);

        // test clean
        accessService.clean(ae, true);
        acl = accessService.getAcl(ae);
        Assert.assertNull(acl);

        attachedEntityAcl = accessService.getAcl(attachedEntity);
        Assert.assertNull(attachedEntityAcl);
    }

    @Test
    public void testBatchGrantAndRevoke() {
        AclEntity ae = new AclServiceTest.MockAclEntity("batch-grant");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            sidToPerm.put(new PrincipalSid("u" + i), AclPermission.ADMINISTRATION);
        }
        accessService.batchGrant(ae, sidToPerm);
        MutableAclRecord acl = accessService.getAcl(ae);
        List<AccessControlEntry> e = acl.getEntries();
        Assert.assertEquals(10, e.size());
        for (int i = 0; i < e.size(); i++) {
            Assert.assertEquals(new PrincipalSid("u" + i), e.get(i).getSid());
        }
        //test batch revoke
        List<AccessRequest> requests = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            AccessRequest request = new AccessRequest();
            request.setSid("u" + i);
            request.setPrincipal(true);
            requests.add(request);
        }
        accessService.batchRevoke(ae, requests);
        acl = accessService.getAcl(ae);
        e = acl.getEntries();
        Assert.assertEquals(0, e.size());

        thrown.expect(KylinException.class);
        accessService.batchRevoke(null, requests);
    }

    @Ignore
    @Test
    public void test100000Entries() throws JsonProcessingException {
        AclServiceTest.MockAclEntity ae = new AclServiceTest.MockAclEntity("100000Entries");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            if (i % 10 == 0) {
                long now = System.currentTimeMillis();
                System.out.println((now - time) + " ms for last 10 entries, total " + i);
                time = now;
            }
            Sid sid = accessService.getSid("USER" + i, true);
            accessService.grant(ae, AclPermission.OPERATION, sid);
        }
    }

    @Test(expected = KylinException.class)
    public void testCheckGlobalAdminException() throws IOException {
        accessService.checkGlobalAdmin("ADMIN");
    }

    @Test
    public void testCheckGlobalAdmin() throws IOException {
        accessService.checkGlobalAdmin("ANALYSIS");
        accessService.checkGlobalAdmin(Arrays.asList("ANALYSIS", "MODEL", "AAA"));
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatching() throws Exception {
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("role_ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(ae, sidToPerm);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("ANALYST", ((PrincipalSid) result.get(0).getSid()).getPrincipal());
    }

    @Test
    public void testGenerateAceResponsesByFuzzMatchingWhenHasSameNameUserAndGroupName() throws Exception {
        AclEntity ae = new AclServiceTest.MockAclEntity("test");
        final Map<Sid, Permission> sidToPerm = new HashMap<>();
        sidToPerm.put(new GrantedAuthoritySid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(ae, sidToPerm);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        Assert.assertEquals(1, result.size());
        // expect ADMIN group is in acl
        Assert.assertEquals("ADMIN", ((GrantedAuthoritySid) result.get(0).getSid()).getGrantedAuthority());
    }

    @Test
    public void testGetProjectAdminUsers() throws IOException {
        String project = "default";
        Set<String> result = accessService.getProjectAdminUsers(project);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testGetProjectManagementUsers() throws IOException {
        String project = "default";
        Set<String> result = accessService.getProjectManagementUsers(project);
        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testRevokeWithSid() {
        AclEntity ae = new AclServiceTest.MockAclEntity("test-domain-object");
        accessService.init(ae, AclPermission.ADMINISTRATION);

        Sid modeler = accessService.getSid("MODELER", true);
        accessService.grant(ae, AclPermission.ADMINISTRATION, modeler);

        Acl acl = accessService.revokeWithSid(ae, "MODELER", true);
        Assert.assertEquals(1, accessService.generateAceResponses(acl).size());

        thrown.expect(KylinException.class);
        accessService.revokeWithSid(null, "MODELER", true);
    }

    @Test
    public void testGetCurrentUserPermissionInProject() throws IOException {
        String result = accessService.getCurrentUserPermissionInProject("default");
        Assert.assertEquals("ADMIN", result);
    }

    @Test
    public void testGetGrantedProjectsOfUser() throws IOException {
        List<String> result = accessService.getGrantedProjectsOfUser("ADMIN");
        Assert.assertEquals(27, result.size());
    }

    @Test
    public void testGetGrantedProjectsOfUserOrGroup() throws IOException {
        // admin user
        List<String> result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN", true);
        Assert.assertEquals(27, result.size());

        // normal user
        result = accessService.getGrantedProjectsOfUserOrGroup("ANALYST", true);
        Assert.assertEquals(0, result.size());

        // granted admin group
        addGroupAndGrantPermission("ADMIN_GROUP", AclPermission.ADMINISTRATION);
        Mockito.when(userGroupService.exists("ADMIN_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN_GROUP", false);
        Assert.assertEquals(1, result.size());

        // granted normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        Mockito.when(userGroupService.exists("MANAGEMENT_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        Assert.assertEquals(1, result.size());

        // does not grant, normal group
        userGroupService.addGroup("NORMAL_GROUP");
        Mockito.when(userGroupService.exists("NORMAL_GROUP")).thenReturn(true);
        result = accessService.getGrantedProjectsOfUserOrGroup("NORMAL_GROUP", false);
        Assert.assertEquals(0, result.size());

        // not exist user
        thrown.expectMessage("Operation failed, user:[nouser] not exists, please add it first");
        accessService.getGrantedProjectsOfUser("nouser");
    }

    @Test
    public void testGetGrantedProjectsOfUserOrGroupWithNotExistGroup() throws IOException {
        thrown.expectMessage("Operation failed, group:[nogroup] not exists, please add it first");
        accessService.getGrantedProjectsOfUserOrGroup("nogroup", false);
    }

    @Test
    public void testGetUserOrGroupAclPermissions() throws IOException {
        // test admin user
        List<String> projects = accessService.getGrantedProjectsOfUserOrGroup("ADMIN", true);
        Mockito.when(userService.isGlobalAdmin("ADMIN")).thenReturn(true);
        List<SidPermissionWithAclResponse> responses = accessService.getUserOrGroupAclPermissions(projects, "ADMIN",
                true);
        Assert.assertEquals(27, responses.size());
        Assert.assertTrue(responses.stream().allMatch(response -> "ADMIN".equals(response.getProjectPermission())));

        // test normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        Mockito.when(userGroupService.exists("MANAGEMENT_GROUP")).thenReturn(true);
        projects = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        responses = accessService.getUserOrGroupAclPermissions(projects, "MANAGEMENT_GROUP", false);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("MANAGEMENT", responses.get(0).getProjectPermission());

        // add ANALYST user to a granted normal group
        addGroupAndGrantPermission("ROLE_ANALYST", AclPermission.OPERATION);
        Mockito.when(userGroupService.exists("ROLE_ANALYST")).thenReturn(true);
        userGroupService.modifyGroupUsers("ROLE_ANALYST", Lists.newArrayList("ANALYST"));
        responses = accessService.getUserOrGroupAclPermissions(projects, "ANALYST", true);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("OPERATION", responses.get(0).getProjectPermission());
    }

    private void addGroupAndGrantPermission(String group, Permission permission) throws IOException {
        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject("default");
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        userGroupService.addGroup(group);
        Sid sid = accessService.getSid(group, false);
        accessService.grant(ae, permission, sid);
    }

    @Test
    public void testCheckAccessRequestList() throws IOException {
        List<AccessRequest> accessRequests = new ArrayList<>();
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(0);
        accessRequest.setPermission("MANAGEMENT");
        accessRequest.setSid("ANALYST");
        accessRequest.setPrincipal(true);
        accessRequests.add(accessRequest);
        accessService.checkAccessRequestList(accessRequests);

        AccessRequest accessRequest2 = new AccessRequest();
        accessRequest2.setAccessEntryId(0);
        accessRequest2.setPermission("ADMIN");
        accessRequest2.setSid("ADMIN");
        accessRequest2.setPrincipal(true);
        accessRequests.add(accessRequest2);
        thrown.expectMessage("You cannot add,modify or remove the system administrator’s rights");
        accessService.checkAccessRequestList(accessRequests);
    }

    @Test
    public void testCheckSid() throws IOException {
        accessService.checkSid(new ArrayList<>());

        List<AccessRequest> accessRequests = new ArrayList<>();
        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(0);
        accessRequest.setPermission("MANAGEMENT");
        accessRequest.setSid("ANALYST");
        accessRequest.setPrincipal(true);
        accessRequests.add(accessRequest);
        accessService.checkSid(accessRequests);

        accessService.checkSid("ADMIN", true);

        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSid("", true);
    }

    @Test
    public void testCheckEmptySid() {
        accessService.checkSidNotEmpty("ADMIN", true);

        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSidNotEmpty("", true);
    }

    @Test
    public void testCheckSidWithEmptyUser() throws IOException {
        thrown.expectMessage("User/Group name should not be empty.");
        accessService.checkSid("", false);
    }

    @Test
    public void testCheckSidWithNotExistUser() throws IOException {
        thrown.expectMessage("Operation failed, user:[nouser] not exists, please add it first");
        accessService.checkSid("nouser", true);
    }

    @Test
    public void testCheckSidWithNotExistGroup() throws IOException {
        thrown.expectMessage("Operation failed, group:[nogroup] not exists, please add it first");
        accessService.checkSid("nogroup", false);
    }

    @Test
    public void testIsGlobalAdmin() throws IOException {
        boolean result = accessService.isGlobalAdmin("ADMIN");
        Assert.assertTrue(result);

        result = accessService.isGlobalAdmin("ANALYST");
        Assert.assertFalse(result);
    }

    @Test
    public void testGetGroupsOfCurrentUser() {
        List<String> result = accessService.getGroupsOfCurrentUser();
        Assert.assertEquals(4, result.size());
    }

    @Test
    public void testGetProjectUsersAndGroups() throws IOException {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");
        Map<String, List<String>> map = accessService.getProjectUsersAndGroups(ae);
        Assert.assertTrue(map.get("user").contains("ADMIN"));
        Assert.assertTrue(map.get("group").contains("ROLE_ADMIN"));
    }

    @Test
    public void testAclWithUnNaturalOrder() {
        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE,
                "1eaca32a-a33e-4b69-83dd-0bb8b1f8c91b");

        // read from metadata
        MutableAclRecord acl = accessService.getAcl(ae);
        // order by sid_order in aceImpl
        // ADL6911(group), BDL6911(group), aCL6911(group), ACZ5815(user), ACZ5815(user), czw9976(user)
        List<AccessControlEntry> entries = acl.getEntries();

        Assert.assertEquals(6, entries.size());

        Assert.assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        Assert.assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        Assert.assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        Assert.assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        Assert.assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        Assert.assertEquals("czw9976", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());


        // grant
        acl = accessService.grant(ae, BasePermission.ADMINISTRATION, accessService.getSid("atest1", true));

        entries = acl.getEntries();
        Assert.assertEquals(7, entries.size());

        Assert.assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        Assert.assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        Assert.assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        Assert.assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        Assert.assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        Assert.assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
        Assert.assertEquals("czw9976", ((PrincipalSid) entries.get(6).getSid()).getPrincipal());

        // revoke czw9976
        acl = accessService.revoke(ae, 6);

        entries = acl.getEntries();
        Assert.assertEquals(6, entries.size());

        Assert.assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        Assert.assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        Assert.assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        Assert.assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        Assert.assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        Assert.assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());


        // update atest1
        Assert.assertEquals(BasePermission.ADMINISTRATION, entries.get(5).getPermission());

        acl = accessService.update(ae, 5, BasePermission.READ);
        entries = acl.getEntries();
        Assert.assertEquals("ADL6911", ((GrantedAuthoritySid) entries.get(0).getSid()).getGrantedAuthority());
        Assert.assertEquals("BDL6911", ((GrantedAuthoritySid) entries.get(1).getSid()).getGrantedAuthority());
        Assert.assertEquals("aCL6911", ((GrantedAuthoritySid) entries.get(2).getSid()).getGrantedAuthority());
        Assert.assertEquals("ACZ5815", ((PrincipalSid) entries.get(3).getSid()).getPrincipal());
        Assert.assertEquals("CCL6911", ((PrincipalSid) entries.get(4).getSid()).getPrincipal());
        Assert.assertEquals("atest1", ((PrincipalSid) entries.get(5).getSid()).getPrincipal());
        Assert.assertEquals(BasePermission.READ, entries.get(5).getPermission());
    }
}
