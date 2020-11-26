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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.springframework.security.acls.domain.BasePermission.ADMINISTRATION;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclEntityFactory;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Permission;
import org.springframework.security.acls.model.Sid;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.ManagedUser;

public class AclPermissionUtil {

    private AclPermissionUtil() {
    }

    public static List<String> transformAuthorities(Collection<? extends GrantedAuthority> authorities) {
        List<String> ret = Lists.newArrayList();
        for (GrantedAuthority auth : authorities) {
            if (!ret.contains(auth.getAuthority())) {
                ret.add(auth.getAuthority());
            }
        }
        return ret;
    }

    public static String getCurrentUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return Objects.isNull(auth) ? null : auth.getName();
    }

    private static MutableAclRecord getProjectAcl(String project) {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        AclEntity ae = AclEntityFactory.createAclEntity(AclEntityType.PROJECT_INSTANCE, projectInstance.getUuid());
        return AclManager.getInstance(KylinConfig.getInstanceFromEnv()).readAcl(new ObjectIdentityImpl(ae));
    }

    public static Set<String> getCurrentUserGroupsInProject(String project, Set<String> groups) {
        return filterGroupsInProject(groups, project);
    }

    public static Set<String> getUserGroupsInProject(ManagedUser user, String project) {
        Set<String> groups = user.getAuthorities().stream().map(SimpleGrantedAuthority::toString)
                .collect(Collectors.toSet());
        return filterGroupsInProject(groups, project);
    }

    public static Set<String> filterGroupsInProject(Set<String> groups, String project) {
        MutableAclRecord acl = getProjectAcl(project);
        if (Objects.isNull(acl)) {
            return groups;
        }
        Sid sid;
        Set<String> groupsInProject = Sets.newHashSet();
        for (AccessControlEntry ace : acl.getEntries()) {
            sid = ace.getSid();
            if (sid instanceof PrincipalSid) {
                continue;
            }
            groupsInProject.add(getName(sid));
        }
        return groups.stream().filter(g -> groupsInProject.contains(g)).collect(Collectors.toSet());
    }

    public static Set<String> getGroupsInProject(Set<String> groups, String project) {
        if (groups.isEmpty()) {
            return groups;
        }
        MutableAclRecord acl = getProjectAcl(project);
        if (Objects.isNull(acl)) {
            return groups;
        }
        return acl.getEntries().stream()
                .filter(accessControlEntry -> accessControlEntry.getSid() instanceof GrantedAuthoritySid)
                .filter(accessControlEntry -> groups.contains(getName(accessControlEntry.getSid())))
                .map(accessControlEntry -> getName(accessControlEntry.getSid())).collect(Collectors.toSet());
    }

    public static boolean isAdmin(Set<String> groups) {
        return Objects.nonNull(groups) && groups.stream().anyMatch(Constant.ROLE_ADMIN::equals);
    }

    public static boolean isAdmin() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return Objects.nonNull(auth) && auth.getAuthorities().stream().map(GrantedAuthority::getAuthority)
                .anyMatch(Constant.ROLE_ADMIN::equals);
    }

    public static boolean canUseACLGreenChannel(String project, Set<String> groups, boolean showTable) {
        if (showTable) {
            return !KylinConfig.getInstanceFromEnv().isAclTCREnabled() || isAdmin();
        }
        return !KylinConfig.getInstanceFromEnv().isAclTCREnabled() || isAdmin() || isAdminInProject(project, groups);
    }

    public static boolean isAdminInProject(String project, Set<String> usergroups) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (Objects.isNull(auth)) {
            return false;
        }

        Set<String> groups = getCurrentUserGroupsInProject(project, usergroups);
        return isSpecificPermissionInProject(auth.getName(), groups, project, ADMINISTRATION);
    }

    public static boolean isSpecificPermissionInProject(ManagedUser user, String project, Permission aclPermission) {
        Set<String> groups = getUserGroupsInProject(user, project);
        return isSpecificPermissionInProject(user.getUsername(), groups, project, aclPermission);
    }

    public static boolean isSpecificPermissionInProject(String username, Set<String> userGroupsInProject,
            String project, Permission aclPermission) {
        MutableAclRecord acl = getProjectAcl(project);
        if (Objects.isNull(acl)) {
            return false;
        }
        Sid sid;
        for (AccessControlEntry ace : acl.getEntries()) {
            if (ace.getPermission().getMask() == aclPermission.getMask()) {
                sid = ace.getSid();
                if (isCurrentUser(sid, username)) {
                    return true;
                }
                if (isCurrentGroup(sid, userGroupsInProject)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isCurrentUser(Sid sid, String username) {
        return (sid instanceof PrincipalSid) && (username.equals(((PrincipalSid) sid).getPrincipal()));
    }

    private static boolean isCurrentGroup(Sid sid, Set<String> groups) {
        if (!(sid instanceof GrantedAuthoritySid)) {
            return false;
        }
        for (String group : groups) {
            if (group.equals(((GrantedAuthoritySid) sid).getGrantedAuthority())) {
                return true;
            }
        }
        return false;
    }

    public static String objID(ObjectIdentity domainObjId) {
        return String.valueOf(domainObjId.getIdentifier());
    }

    public static String getName(Sid sid) {
        if (sid instanceof PrincipalSid) {
            return ((PrincipalSid) sid).getPrincipal();
        } else {
            return ((GrantedAuthoritySid) sid).getGrantedAuthority();
        }
    }

    public static boolean isAclUpdatable(String project, Set<String> groups) {
        return (isAdmin() || (isAdminInProject(project, groups)
                && KylinConfig.getInstanceFromEnv().isAllowedProjectAdminGrantAcl()));
    }

    public static void checkAclUpdatable(String project, Set<String> groups) {
        if (!AclPermissionUtil.isAclUpdatable(project, groups)) {
            if (KylinConfig.getInstanceFromEnv().isAllowedProjectAdminGrantAcl()) {
                throw new KylinException(PERMISSION_DENIED,
                        MsgPicker.getMsg().getACCESS_DENY_ONLY_ADMIN_AND_PROJECT_ADMIN());
            } else {
                throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getACCESS_DENY_ONLY_ADMIN());
            }
        }
    }

    public static QueryContext.AclInfo prepareQueryContextACLInfo(String project, Set<String> groups) {
        return new QueryContext.AclInfo(getCurrentUsername(), groups, isAdminInProject(project, groups));
    }
}
