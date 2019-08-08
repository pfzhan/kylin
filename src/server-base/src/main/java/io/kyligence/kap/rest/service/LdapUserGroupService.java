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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.support.LdapUtils;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.user.ManagedUser;

public class LdapUserGroupService extends NUserGroupService {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserGroupService.class);

    private static final String LDAP_GROUPS = "ldap_groups";

    private static final String SKIPPED_LDAP = "skipped-ldap";

    private static final com.google.common.cache.Cache<String, Set<String>> ldapGroupsCache = CacheBuilder.newBuilder()
            .maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    private static final com.google.common.cache.Cache<String, List<ManagedUser>> ldapGroupsMembersCache = CacheBuilder
            .newBuilder().maximumSize(KylinConfig.getInstanceFromEnv().getServerUserCacheMaxEntries())
            .expireAfterWrite(KylinConfig.getInstanceFromEnv().getServerUserCacheExpireSeconds(), TimeUnit.SECONDS)
            .build();

    @Autowired
    @Qualifier("ldapTemplate")
    private SpringSecurityLdapTemplate ldapTemplate;

    private Message msg = MsgPicker.getMsg();

    @Override
    public void addGroup(String name) {
        throw new UnsupportedOperationException(msg.getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteGroup(String name) {
        throw new UnsupportedOperationException(msg.getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) {
        throw new UnsupportedOperationException(msg.getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public List<String> getAllUserGroups() {
        Set<String> allGroups = ldapGroupsCache.getIfPresent(LDAP_GROUPS);
        if (allGroups == null) {
            logger.info("Can not get groups from cache, ask ldap instead.");
            String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
            String ldapGroupSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupSearchFilter();
            String ldapGroupIDAttr = KapConfig.getInstanceFromEnv().getLDAPGroupIDAttr();
            logger.info(
                    "ldap group search config, base: {}, filter: {}, identifier attribute: {}, member search filter: {}, member identifier: {}",
                    ldapGroupSearchBase, ldapGroupSearchFilter, ldapGroupIDAttr,
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter(),
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr());
            allGroups = Sets.newTreeSet(ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                    ldapGroupSearchFilter, new String[0], ldapGroupIDAttr));
            ldapGroupsCache.put(LDAP_GROUPS, allGroups);
        }
        logger.info("Get all groups size: {}", allGroups.size());
        return Lists.newArrayList(allGroups);
    }

    @Override
    public Map<String, List<String>> getUserAndUserGroup() {
        Map<String, List<String>> result = Maps.newHashMap();
        for (String group : getAllUserGroups()) {
            result.put(group,
                    getGroupMembersByName(group).stream().map(ManagedUser::getUsername).collect(Collectors.toList()));
        }
        return result;
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) {
        List<ManagedUser> members = ldapGroupsMembersCache.getIfPresent(name);
        if (members == null) {
            logger.info("Can not get the group {}'s all members from cache, ask ldap instead.", name);
            members = new ArrayList<>();

            String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
            String ldapGroupMemberSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter();
            String ldapGroupMemberAttr = KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr();
            String ldapUserIDAttr = KapConfig.getInstanceFromEnv().getLDAPUserIDAttr();
            Set<String> ldapUserDNs = ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                    ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberAttr);

            for (String u : ldapUserDNs) {
                try {
                    String userID = LdapUtils.getStringValue(new LdapName(u), ldapUserIDAttr);
                    if (userService.userExists(userID)) { //guard groups may have ou or groups
                        members.add(
                                new ManagedUser(userID, SKIPPED_LDAP, false, Lists.<GrantedAuthority> newArrayList()));
                    }
                } catch (InvalidNameException ie) {
                    logger.error("Can not get LDAP group's member: {}", u, ie);
                }
            }
            ldapGroupsMembersCache.put(name, members);
        }
        return members;
    }
}
