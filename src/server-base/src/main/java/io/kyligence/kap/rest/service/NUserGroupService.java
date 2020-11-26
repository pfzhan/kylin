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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_USERGROUP_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.USERGROUP_NOT_EXIST;
import static org.apache.kylin.rest.constant.Constant.GROUP_ALL_USERS;
import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.usergroup.NUserGroupManager;
import io.kyligence.kap.metadata.usergroup.UserGroup;
import io.kyligence.kap.rest.response.UserGroupResponseKI;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("nUserGroupService")
public class NUserGroupService implements IUserGroupService {
    public static final Logger logger = LoggerFactory.getLogger(NUserGroupService.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Override
    public List<String> getAllUserGroups() {
        return getUserGroupManager().getAllGroupNames();
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) throws IOException {
        List<ManagedUser> users = userService.listUsers();
        for (Iterator<ManagedUser> it = users.iterator(); it.hasNext();) {
            ManagedUser user = it.next();
            if (!user.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                it.remove();
            }
        }
        return users;
    }

    @Override
    @Transaction
    public void addGroup(String name) {
        aclEvaluate.checkIsGlobalAdmin();
        getUserGroupManager().add(name);
    }

    @Override
    @Transaction
    public void deleteGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        checkGroupCanBeDeleted(name);
        // remove retained user group in all users
        List<ManagedUser> managedUsers = userService.listUsers();
        for (ManagedUser managedUser : managedUsers) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                managedUser.removeAuthorities(name);
                userService.updateUser(managedUser);
            }
        }
        //delete group's project ACL
        accessService.revokeProjectPermission(name, MetadataConstants.TYPE_GROUP);

        getUserGroupManager().delete(name);
    }

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    @Override
    @Transaction
    public void modifyGroupUsers(String groupName, List<String> users) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        checkGroupNameExist(groupName);

        List<String> groupUsers = new ArrayList<>();
        for (ManagedUser user : getGroupMembersByName(groupName)) {
            groupUsers.add(user.getUsername());
        }
        List<String> moveInUsers = Lists.newArrayList(users);
        List<String> moveOutUsers = Lists.newArrayList(groupUsers);
        moveInUsers.removeAll(groupUsers);
        moveOutUsers.removeAll(users);

        for (String in : moveInUsers) {
            ManagedUser managedUser = (ManagedUser) userService.loadUserByUsername(in);
            managedUser.addAuthorities(groupName);
            userService.updateUser(managedUser);
        }

        for (String out : moveOutUsers) {
            ManagedUser managedUser = (ManagedUser) userService.loadUserByUsername(out);
            managedUser.removeAuthorities(groupName);
            userService.updateUser(managedUser);
        }
    }

    @Override
    public List<String> listAllAuthorities() {
        aclEvaluate.checkIsGlobalAdmin();
        return getAllUserGroups();
    }

    @Override
    public List<String> getAuthoritiesFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return StringUtils.isEmpty(userGroupName) ? getAllUserGroups()
                : getAllUserGroups().stream()
                        .filter(userGroup -> userGroup.toUpperCase().contains(userGroupName.toUpperCase()))
                        .collect(Collectors.toList());
    }

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupManager().getAllGroups();
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return StringUtils.isEmpty(userGroupName) ? listUserGroups()
                : getUserGroupManager().getAllGroups().stream().filter(
                        userGroup -> userGroup.getGroupName().toUpperCase().contains(userGroupName.toUpperCase()))
                        .collect(Collectors.toList());
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        val groups = getUserGroupManager().getAllGroups();
        for (val group : groups) {
            if (StringUtils.equalsIgnoreCase(uuid, group.getUuid())) {
                return group.getGroupName();
            }
        }
        throw new KylinException(USERGROUP_NOT_EXIST,
                String.format(MsgPicker.getMsg().getGROUP_UUID_NOT_EXIST(), uuid));
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        val groups = getUserGroupManager().getAllGroups();
        for (val group : groups) {
            if (StringUtils.equalsIgnoreCase(groupName, group.getGroupName())) {
                return group.getUuid();
            }
        }
        throw new KylinException(USERGROUP_NOT_EXIST,
                String.format(MsgPicker.getMsg().getUSERGROUP_NOT_EXIST(), groupName));
    }

    public boolean exists(String name) {
        return getAllUserGroups().contains(name);
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

    private void checkGroupNameExist(String groupName) {
        val groups = getAllUserGroups();
        if (!groups.contains(groupName)) {
            throw new KylinException(INVALID_PARAMETER,
                    String.format(MsgPicker.getMsg().getUSERGROUP_NOT_EXIST(), groupName));
        }
    }

    public Map<String, List<String>> getUserAndUserGroup() throws IOException {
        Map result = Maps.newHashMap();

        List<String> userNames = userService.getManagedUsersByFuzzMatching(null, false).stream()
                .map(ManagedUser::getUsername).collect(Collectors.toList());
        List<String> groupNames = getAllUserGroups();

        result.put("user", userNames);
        result.put("group", groupNames);
        return result;
    }

    public Set<String> listUserGroups(String username) {
        try {
            List<String> groups = getAllUserGroups();
            Set<String> result = new HashSet<>();
            for (String group : groups) {
                val users = getGroupMembersByName(group);
                for (val user : users) {
                    if (StringUtils.equalsIgnoreCase(username, user.getUsername())) {
                        result.add(group);
                        break;
                    }
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private NUserGroupManager getUserGroupManager() {
        return NUserGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    public List<UserGroupResponseKI> getUserGroupResponse(List<UserGroup> userGroups) throws IOException {
        List<UserGroupResponseKI> result = new ArrayList<>();
        for (UserGroup group : userGroups) {
            Set<String> groupMembers = new TreeSet<>();
            for (ManagedUser user : getGroupMembersByName(group.getGroupName())) {
                groupMembers.add(user.getUsername());
            }
            result.add(new UserGroupResponseKI(group.getUuid(), group.getGroupName(), groupMembers));
        }
        return result;
    }

    protected List<UserGroup> getUserGroupSpecialUuid() {
        List<String> groups = getAllUserGroups();
        List<UserGroup> result = new ArrayList<>();
        for (String group : groups) {
            UserGroup userGroup = new UserGroup();
            userGroup.setUuid(group);
            userGroup.setGroupName(group);
            result.add(userGroup);
        }
        return result;
    }

    private void checkGroupCanBeDeleted(String groupName) {
        if (groupName.equals(GROUP_ALL_USERS) || groupName.equals(ROLE_ADMIN)) {
            throw new KylinException(INVALID_USERGROUP_NAME,
                    "Failed to delete user group, user groups of ALL_USERS and ROLE_ADMIN cannot be deleted.");
        }
    }

}