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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.user.ManagedUser;

public class LdapUserService implements UserService {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserService.class);

    private static final String LDAP_USERS = "ldap_users";

    private static final String SKIPPED_LDAP = "skipped-ldap";

    private static final com.google.common.cache.Cache<String, List<ManagedUser>> ldapUsersCache = CacheBuilder
            .newBuilder().maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate ldapTemplate;

    @Autowired
    @Qualifier("ldapUserDetailsService")
    private LdapUserDetailsService ldapUserDetailsService;

    @Autowired
    @Qualifier("userGroupService")
    private LdapUserGroupService userGroupService;

    private final Message msg = MsgPicker.getMsg();

    private boolean evictCacheFlag = false;

    @Override
    public boolean isEvictCacheFlag() {
        return this.evictCacheFlag;
    }

    @Override
    public void setEvictCacheFlag(boolean evictCacheFlag) {
        this.evictCacheFlag = evictCacheFlag;
    }

    @Override
    public void createUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void updateUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteUser(String s) {
        throw new UnsupportedOperationException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void changePassword(String s, String s1) {
        throw new UnsupportedOperationException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public boolean userExists(String username) {
        try {
            for (ManagedUser user : listUsers()) {
                if (user.getUsername().equals(username)) {
                    return true;
                }
            }
        } catch (IOException e) {
            logger.error("[UNEXPECTED_THINGS_HAPPENED] username: {}", username, e);
        }
        return false;
    }

    @Override
    public UserDetails loadUserByUsername(String username) {
        UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
        return new ManagedUser(ldapUser.getUsername(), SKIPPED_LDAP, false, ldapUser.getAuthorities());
    }

    @Override
    public List<ManagedUser> listUsers() throws IOException {
        List<ManagedUser> allUsers = ldapUsersCache.getIfPresent(LDAP_USERS);
        if (allUsers == null) {
            logger.info("Failed to read users from cache, reload from ldap server.");
            allUsers = new ArrayList<>();
            Set<String> ldapUsers = getAllUsers();
            for (String user : ldapUsers) {
                allUsers.add(new ManagedUser(user, SKIPPED_LDAP, false, Lists.<GrantedAuthority> newArrayList()));
            }
            ldapUsersCache.put(LDAP_USERS, Preconditions.checkNotNull(allUsers,
                    "Failed to load users from ldap server, something went wrong."));
        }
        logger.info("Get all users size: {}", allUsers.size());
        return allUsers;
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser user : userGroupService
                .getGroupMembersByName(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
            adminUsers.add(user.getUsername());
        }
        return adminUsers;
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        UserDetails details = loadUserByUsername(user.getUsername());
        user.setGrantedAuthorities(details.getAuthorities());
    }

    public void onUserAuthenticated(String username) {
        if (!userExists(username)) {
            logger.info("User {} not exists, invalidate cache {}.", username, LDAP_USERS);
            ldapUsersCache.invalidate(LDAP_USERS);
        }
    }

    private Set<String> getAllUsers() {
        String ldapUserSearchBase = KylinConfig.getInstanceFromEnv().getLDAPUserSearchBase();
        String ldapUserSearchFilter = KapConfig.getInstanceFromEnv().getLDAPUserSearchFilter();
        String ldapUserIDAttr = KapConfig.getInstanceFromEnv().getLDAPUserIDAttr();
        logger.info("ldap user search config, base: {}, filter: {}, identifier attribute: {}", ldapUserSearchBase,
                ldapUserSearchFilter, ldapUserIDAttr);
        return Sets.newTreeSet(ldapTemplate.searchForSingleAttributeValues(ldapUserSearchBase, ldapUserSearchFilter,
                new String[0], ldapUserIDAttr));
    }
}
