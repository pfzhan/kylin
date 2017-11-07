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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

public class LDAPUserService implements UserService {
    private final static com.google.common.cache.Cache<String, List<ManagedUser>> ldapUsersCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate template;

    @Autowired
    @Qualifier("ldapUserDetailsService")
    private LdapUserDetailsService ldapUserDetailsService;

    @Autowired
    @Qualifier("userGroupService")
    private LDAPUserGroupService userGroupService;

    private KapMessage msg = KapMsgPicker.getMsg();

    @Override
    public boolean isEvictCacheFlag() {
        return false;
    }

    @Override
    public void setEvictCacheFlag(boolean evictCacheFlag) {}

    @Override
    public void createUser(UserDetails user) {
        throw new RuntimeException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void updateUser(UserDetails user) {
        throw new RuntimeException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteUser(String username) {
        throw new RuntimeException(msg.getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new RuntimeException(msg.getUSER_EDIT_NOT_ALLOWED());
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
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
        return new ManagedUser(ldapUser.getUsername(), "skippped-ldap", false, ldapUser.getAuthorities());
    }

    //LDAP implement will not bring user with its authority.
    @Override
    public List<ManagedUser> listUsers() throws IOException {
        List<ManagedUser> allUsers = ldapUsersCache.getIfPresent("ldap_users");
        if (allUsers == null) {
            allUsers = new ArrayList<>();
            Set<String> ldapUsers = getAllUsers();
            for (String user : ldapUsers) {
                allUsers.add(new ManagedUser(user, "skippped-ldap", false, Lists.<GrantedAuthority>newArrayList()));
            }
            ldapUsersCache.put("ldap_users", Preconditions.checkNotNull(allUsers, "get users from ldap faild, something went wrong."));
        }
        return allUsers;
    }

    @Override
    public List<String> listAdminUsers() throws IOException{
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser user : userGroupService.getGroupMembersByName(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
            adminUsers.add(user.getUsername());
        }
        return adminUsers;
    }

    @Override
    public void completeUserInfo(ManagedUser user){
        if (user.getAuthorities().isEmpty()) {
            Collection<? extends GrantedAuthority> fullAuthorities = loadUserByUsername(user.getUsername()).getAuthorities();
            user.setGrantedAuthorities(fullAuthorities);
        }
    }

    private Set<String> getAllUsers() {
        return Sets.newTreeSet(
                template.searchForSingleAttributeValues(
                        KylinConfig.getInstanceFromEnv().getLDAPUserSearchBase(),
                        KapConfig.getInstanceFromEnv().getLDAPUserSearchFilter(),
                        new String[0],
                        "cn"));
    }
}