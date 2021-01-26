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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.naming.InvalidNameException;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.LdapName;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.ldap.support.LdapUtils;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.usergroup.UserGroup;

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

    @Autowired
    @Qualifier("userService")
    private LdapUserService ldapUserService;

    @Autowired
    private SearchControls searchControls;

    @Override
    public void addGroup(String name) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteGroup(String name) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getGroup_EDIT_NOT_ALLOWED());
    }

    @Override
    public List<String> getAllUserGroups() {
        Set<String> allGroups = ldapGroupsCache.getIfPresent(LDAP_GROUPS);
        if (allGroups == null || allGroups.isEmpty()) {
            allGroups = new HashSet<>();
            logger.info("Can not get groups from cache, ask ldap instead.");
            String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
            String ldapGroupSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupSearchFilter();
            String ldapGroupIDAttr = KapConfig.getInstanceFromEnv().getLDAPGroupIDAttr();
            Integer maxPageSize = KapConfig.getInstanceFromEnv().getLDAPMaxPageSize();
            logger.info(
                    "ldap group search config, base: {}, filter: {}, identifier attribute: {}, member search filter: {}, member identifier: {}, maxPageSize: {}",
                    ldapGroupSearchBase, ldapGroupSearchFilter, ldapGroupIDAttr,
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter(),
                    KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr(), maxPageSize);

            final PagedResultsDirContextProcessor processor = new PagedResultsDirContextProcessor(maxPageSize);

            AttributesMapper<String> attributesMapper = attributes -> attributes.get(ldapGroupIDAttr).get().toString();

            do {
                allGroups.addAll(ldapTemplate.search(ldapGroupSearchBase, ldapGroupSearchFilter, searchControls,
                        attributesMapper, processor));
            } while (processor.hasMore());
            ldapGroupsCache.put(LDAP_GROUPS, allGroups);
        }
        logger.info("Get all groups size: {}", allGroups.size());
        return Collections.unmodifiableList(Lists.newArrayList(allGroups));
    }

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupSpecialUuid();
    }

    @Override
    public Map<String, List<String>> getUserAndUserGroup() {
        Map<String, List<String>> result = Maps.newHashMap();
        for (String group : getAllUserGroups()) {
            result.put(group,
                    getGroupMembersByName(group).stream().map(ManagedUser::getUsername).collect(Collectors.toList()));
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return listUserGroups().stream()
                .filter(userGroup -> StringUtils.isEmpty(userGroupName) || userGroup.getGroupName()
                        .toUpperCase(Locale.ROOT).contains(userGroupName.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) {
        List<ManagedUser> members = ldapGroupsMembersCache.getIfPresent(name);
        if (members == null || members.isEmpty()) {
            logger.info("Can not get the group {}'s all members from cache, ask ldap instead.", name);
            members = new ArrayList<>();
            String ldapUserIDAttr = KapConfig.getInstanceFromEnv().getLDAPUserIDAttr();
            Set<String> ldapUserDNs = getAllGroupMembers(name);

            for (String u : ldapUserDNs) {
                try {
                    String username = LdapUtils.getStringValue(new LdapName(u), ldapUserIDAttr);
                    if (userService.userExists(username)) {//guard groups may have ou or groups
                        try {
                            ManagedUser ldapUser = new ManagedUser(username, SKIPPED_LDAP, false,
                                    Lists.<GrantedAuthority> newArrayList());
                            ldapUserService.completeUserInfoInternal(ldapUser);
                            members.add(ldapUser);
                        } catch (UsernameNotFoundException e) {//user maybe not exist because 'userService' is the cache
                            logger.warn(String.format(Locale.ROOT, "User %s not found.", username), e);
                        }
                    }
                } catch (InvalidNameException ie) {
                    logger.error("Can not get LDAP group's member: {}", u, ie);
                }
            }
            ldapGroupsMembersCache.put(name, Collections.unmodifiableList(members));
        }
        return members;
    }


    private Set<String> getAllGroupMembers(String name) {
        String ldapGroupSearchBase = KylinConfig.getInstanceFromEnv().getLDAPGroupSearchBase();
        String ldapGroupMemberSearchFilter = KapConfig.getInstanceFromEnv().getLDAPGroupMemberSearchFilter();
        String ldapGroupMemberAttr = KapConfig.getInstanceFromEnv().getLDAPGroupMemberAttr();
        Set<String> ldapUserDNs = ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberAttr);
        logger.info("Ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberAttr);

        if (ldapUserDNs.isEmpty()) {
            // try using range retrieval
            int left = 0;
            Integer maxValRange = KapConfig.getInstanceFromEnv().getLDAPMaxValRange();
            while (true) {
                String ldapGroupMemberRangeAttr = String.format(Locale.ROOT, "%s;range=%s-%s", ldapGroupMemberAttr,
                        left, left + (maxValRange - 1));
                logger.info("Ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                        ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberRangeAttr);
                Set<String> rangeResults = ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                        ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberRangeAttr);
                if (rangeResults.isEmpty()) {
                    // maybe the last page
                    ldapGroupMemberRangeAttr = String.format(Locale.ROOT, "%s;range=%s-%s", ldapGroupMemberAttr,
                            left, "*");
                    logger.info(
                            "Last page, ldap group members search config, base: {},member search filter: {}, member identifier: {}",
                            ldapGroupSearchBase, ldapGroupMemberSearchFilter, ldapGroupMemberRangeAttr);
                    ldapUserDNs.addAll(ldapTemplate.searchForSingleAttributeValues(ldapGroupSearchBase,
                            ldapGroupMemberSearchFilter, new String[] { name }, ldapGroupMemberRangeAttr));
                    break;
                }
                ldapUserDNs.addAll(rangeResults);
                left += maxValRange;
            }
        }

        return ldapUserDNs;
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        return uuid;
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        return groupName;
    }
}
