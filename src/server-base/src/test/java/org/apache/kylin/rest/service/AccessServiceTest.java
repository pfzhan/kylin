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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.response.SidPermissionWithAclResponse;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.AclPermissionFactory;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.service.ProjectService;

/**
 */
public class AccessServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("accessService")
    AccessService accessService;

    @Autowired
    @Qualifier("projectService")
    ProjectService projectService;

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Mock
    AclService aclService = Mockito.spy(AclService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testBasics() throws JsonProcessingException {
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
    public void testBatchGrant() {
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
        sidToPerm.put(new GrantedAuthoritySid("ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("admin"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new GrantedAuthoritySid("ANALYST"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("ROLE_ADMIN"), AclPermission.ADMINISTRATION);
        sidToPerm.put(new PrincipalSid("role_ADMIN"), AclPermission.ADMINISTRATION);
        accessService.batchGrant(ae, sidToPerm);
        List<AccessEntryResponse> result = accessService.generateAceResponsesByFuzzMatching(ae, "", false);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("ANALYST", ((GrantedAuthoritySid) result.get(0).getSid()).getGrantedAuthority());
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

        thrown.expect(TransactionException.class);
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
        Assert.assertEquals(19, result.size());
    }

    @Test
    public void testGetGrantedProjectsOfUserOrGroup() throws IOException {
        // admin user
        List<String> result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN", true);
        Assert.assertEquals(19, result.size());

        // normal user
        result = accessService.getGrantedProjectsOfUserOrGroup("ANALYST", true);
        Assert.assertEquals(0, result.size());

        // granted admin group
        addGroupAndGrantPermission("ADMIN_GROUP", AclPermission.ADMINISTRATION);
        result = accessService.getGrantedProjectsOfUserOrGroup("ADMIN_GROUP", false);
        Assert.assertEquals(1, result.size());

        // granted normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        result = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        Assert.assertEquals(1, result.size());

        // does not grant, normal group
        userGroupService.addGroup("NORMAL_GROUP");
        result = accessService.getGrantedProjectsOfUserOrGroup("NORMAL_GROUP", false);
        Assert.assertEquals(0, result.size());

        // add ANALYST user to a granted normal group
        userGroupService.modifyGroupUsers("MANAGEMENT_GROUP", Lists.newArrayList("ANALYST"));
        result = accessService.getGrantedProjectsOfUserOrGroup("ANALYST", true);
        Assert.assertEquals(1, result.size());

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
        List<SidPermissionWithAclResponse> responses = accessService.getUserOrGroupAclPermissions(projects, "ADMIN",
                true);
        Assert.assertEquals(19, responses.size());
        Assert.assertTrue(responses.stream().allMatch(response -> "ADMIN".equals(response.getProjectPermission())));

        // test normal group
        addGroupAndGrantPermission("MANAGEMENT_GROUP", AclPermission.MANAGEMENT);
        projects = accessService.getGrantedProjectsOfUserOrGroup("MANAGEMENT_GROUP", false);
        responses = accessService.getUserOrGroupAclPermissions(projects, "MANAGEMENT_GROUP", false);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("MANAGEMENT", responses.get(0).getProjectPermission());

        // add ANALYST user to a granted normal group
        userGroupService.modifyGroupUsers("MANAGEMENT_GROUP", Lists.newArrayList("ANALYST"));
        responses = accessService.getUserOrGroupAclPermissions(projects, "ANALYST", true);
        Assert.assertEquals(1, responses.size());
        Assert.assertEquals("MANAGEMENT", responses.get(0).getProjectPermission());
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
}
