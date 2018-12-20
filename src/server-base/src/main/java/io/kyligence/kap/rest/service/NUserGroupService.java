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
import java.util.Iterator;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.ManagedUser;
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
import com.google.common.io.ByteStreams;

import io.kyligence.kap.metadata.acl.UserGroup;
import io.kyligence.kap.rest.transaction.Transaction;

@Component("nUserGroupService")
public class NUserGroupService implements IUserGroupService {
    public static final Logger logger = LoggerFactory.getLogger(NUserGroupService.class);

    private static final String PATH = "/user_group";
    private static final Serializer<UserGroup> USER_GROUP_SERIALIZER = new JsonSerializer<>(UserGroup.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @PostConstruct
    public void init() throws IOException, InterruptedException {
        int retry = 100;
        while (retry > 0) {
            UserGroup userGroup = getUserGroup();
            if (!userGroup.exists(Constant.GROUP_ALL_USERS)) {
                userGroup.add(Constant.GROUP_ALL_USERS);
            }
            if (!userGroup.exists(Constant.ROLE_ADMIN)) {
                userGroup.add(Constant.ROLE_ADMIN);
            }
            if (!userGroup.exists(Constant.ROLE_MODELER)) {
                userGroup.add(Constant.ROLE_MODELER);
            }
            if (!userGroup.exists(Constant.ROLE_ANALYST)) {
                userGroup.add(Constant.ROLE_ANALYST);
            }

            try {
                if (getStore().getResource(PATH) != null) {
                    return;
                }
                getStore().putResourceWithoutCheck(PATH,
                        ByteStreams.asByteSource(JsonUtil.writeValueAsBytes(userGroup)), 0);
                return;
            } catch (WriteConflictException e) {
                logger.info("Find WriteConflictException, sleep 100 ms.", e);
                Thread.sleep(100L);
                retry--;
            }
        }
        logger.error("Failed to update user group's metadata.");
    }

    private UserGroup getUserGroup() throws IOException {
        UserGroup userGroup = getStore().getResource(PATH, USER_GROUP_SERIALIZER);
        if (userGroup == null) {
            userGroup = new UserGroup();
        }
        return userGroup;
    }

    @Override
    public List<String> getAllUserGroups() throws IOException {
        return getUserGroup().getAllGroups();
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
    public void addGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        UserGroup userGroup = getUserGroup();
        getStore().checkAndPutResource(PATH, userGroup.add(name), USER_GROUP_SERIALIZER);
    }

    @Override
    @Transaction
    public void deleteGroup(String name) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
        // remove retained user group in all users
        List<ManagedUser> managedUsers = userService.listUsers();
        for (ManagedUser managedUser : managedUsers) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                managedUser.removeAuthoritie(name);
                userService.updateUser(managedUser);
            }
        }
        //delete group's project ACL
        accessService.revokeProjectPermission(name, MetadataConstants.TYPE_GROUP);

        getStore().checkAndPutResource(PATH, getUserGroup().delete(name), USER_GROUP_SERIALIZER);
    }

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    @Override
    @Transaction
    public void modifyGroupUsers(String groupName, List<String> users) throws IOException {
        aclEvaluate.checkIsGlobalAdmin();
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
            managedUser.addAuthoritie(groupName);
            userService.updateUser(managedUser);
        }

        for (String out : moveOutUsers) {
            ManagedUser managedUser = (ManagedUser) userService.loadUserByUsername(out);
            managedUser.removeAuthoritie(groupName);
            userService.updateUser(managedUser);
        }
    }

    // add param project to check user's permission
    public List<String> listAllAuthorities(String project) throws IOException {
        if (StringUtils.isEmpty(project)) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        return getAllUserGroups();
    }

    public boolean exists(String name) throws IOException {
        return getAllUserGroups().contains(name);
    }

    public ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }
}