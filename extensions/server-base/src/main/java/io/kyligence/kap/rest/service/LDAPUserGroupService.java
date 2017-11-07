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

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import com.google.common.collect.Sets;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.security.ManagedUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.support.LdapUtils;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

public class LDAPUserGroupService extends UserGroupService {
    private final static com.google.common.cache.Cache<String, Set<String>> ldapGroupsCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    private final static com.google.common.cache.Cache<String, List<ManagedUser>> ldapGroupsMembersCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate template;

    private KapMessage msg = KapMsgPicker.getMsg();

    @Override
    protected List<String> getAllUserGroups() throws IOException {
        Set<String> allGroups = ldapGroupsCache.getIfPresent("ldap_groups");
        if (allGroups == null) {
            allGroups = Sets.newTreeSet(
                    template.searchForSingleAttributeValues(
                            KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase(),
                            KapConfig.getInstanceFromEnv().getLDAPGroupSearchFilter(),
                            new String[0],
                            "cn"));
            ldapGroupsCache.put("ldap_groups", allGroups);
        }
        return Lists.newArrayList(allGroups);
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name){
        List<ManagedUser> members = ldapGroupsMembersCache.getIfPresent(name);
        if (members == null) {
            members = new ArrayList<>();

            Set<String> ldapUserDNs = template.searchForSingleAttributeValues(
                    KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase(),
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter(),
                    new String[]{name},
                    "member");

            for (String u : ldapUserDNs) {
                try {
                    String cn = LdapUtils.getStringValue(new LdapName(u), "cn");
                    if (userService.userExists(cn)) { //guard groups may have ou or groups
                        members.add(new ManagedUser(cn, "skippped-ldap", false, Lists.<GrantedAuthority>newArrayList()));
                    }
                } catch (InvalidNameException e) {
                    throw new RuntimeException("Can not get LDAP group's member. " + e.getMessage());
                }
            }
            ldapGroupsMembersCache.put(name, members);
        }
        return members;
    }

    @Override
    public void addGroup(String name) throws IOException {
        throw new RuntimeException(msg.getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteGroup(String name) throws IOException {
        throw new RuntimeException(msg.getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) throws IOException {
        throw new RuntimeException(msg.getGroup_EDIT_NOT_ALLOWED());
    }
}