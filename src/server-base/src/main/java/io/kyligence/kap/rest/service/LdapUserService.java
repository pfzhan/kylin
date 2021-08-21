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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.directory.SearchControls;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.ldap.control.PagedResultsDirContextProcessor;
import org.springframework.ldap.core.AttributesMapper;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.ldap.SpringSecurityLdapTemplate;
import org.springframework.security.ldap.userdetails.LdapUserDetailsService;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.user.ManagedUser;
import org.springframework.util.CollectionUtils;

public class LdapUserService implements UserService {

    private static final Logger logger = LoggerFactory.getLogger(LdapUserService.class);

    private static final String LDAP_USERS = "ldap_users";

    private static final String SKIPPED_LDAP = "skipped-ldap";

    private static final AtomicBoolean LOAD_TASK_STATUS = new AtomicBoolean(Boolean.FALSE);

    private static final ThreadPoolExecutor LOAD_TASK_POOL = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1), Executors.defaultThreadFactory(), (r, e) -> { });

    private static final com.google.common.cache.Cache<String, Map<String, ManagedUser>> ldapUsersCache = CacheBuilder
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

    @Autowired
    private SearchControls searchControls;

    @Override
    public void createUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void updateUser(UserDetails userDetails) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void deleteUser(String s) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public void changePassword(String s, String s1) {
        throw new UnsupportedOperationException(MsgPicker.getMsg().getUSER_EDIT_NOT_ALLOWED());
    }

    @Override
    public boolean userExists(String username) {
        Map<String, ManagedUser> managedUserMap = this.getLDAPUsersCache();
        if (Objects.nonNull(managedUserMap)) {
            return managedUserMap.containsKey(username);
        }
        UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
        this.asyncLoadCacheData();
        return Objects.nonNull(ldapUser);
    }

    @Override
    public UserDetails loadUserByUsername(String username) {
        Map<String, ManagedUser> managedUserMap = this.getLDAPUsersCache();
        if (Objects.nonNull(managedUserMap)) {
            for (Map.Entry<String, ManagedUser> entry : managedUserMap.entrySet()) {
                if (StringUtils.equalsIgnoreCase(username, entry.getKey())) {
                    return entry.getValue();
                }
            }
            throw new UsernameNotFoundException(
                    String.format(Locale.ROOT, MsgPicker.getMsg().getUSER_NOT_FOUND(), username));
        } else {
            UserDetails ldapUser = ldapUserDetailsService.loadUserByUsername(username);
            this.asyncLoadCacheData();
            if (Objects.isNull(ldapUser)) {
                String msg = String.format(Locale.ROOT, MsgPicker.getMsg().getUSER_NOT_FOUND(), username);
                throw new UsernameNotFoundException(msg);
            }
            return new ManagedUser(ldapUser.getUsername(), SKIPPED_LDAP, false, ldapUser.getAuthorities());
        }
    }

    @Override
    public List<ManagedUser> listUsers() {
        Map<String, ManagedUser> allUsers = ldapUsersCache.getIfPresent(LDAP_USERS);
        if (CollectionUtils.isEmpty(allUsers)) {
            logger.info("Failed to read users from cache, reload from ldap server.");
            allUsers = new HashMap<>();
            Set<String> ldapUsers = getAllUsers();
            for (String user : ldapUsers) {
                ManagedUser ldapUser = new ManagedUser(user, SKIPPED_LDAP, false, Lists.newArrayList());
                try {
                    completeUserInfoInternal(ldapUser);
                    allUsers.put(user, ldapUser);
                } catch (IncorrectResultSizeDataAccessException e) {
                    logger.warn("Complete user {} info exception", ldapUser.getUsername(), e);
                }
            }
            ldapUsersCache.put(LDAP_USERS, Preconditions.checkNotNull(allUsers,
                    "Failed to load users from ldap server, something went wrong."));
        }
        logger.info("Get all users size: {}", allUsers.size());
        return Collections.unmodifiableList(new ArrayList<>(allUsers.values()));
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser user : userGroupService
                .getGroupMembersByName(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
            adminUsers.add(user.getUsername());
        }
        return Collections.unmodifiableList(adminUsers);
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
        //do nothing
    }

    public void completeUserInfoInternal(ManagedUser user) {
        Map<String, List<String>> userAndUserGroup = userGroupService.getUserAndUserGroup();
        for (Map.Entry<String, List<String>> entry : userAndUserGroup.entrySet()) {
            String groupName = entry.getKey();
            Set<String> userSet = new HashSet<>(entry.getValue());
            if(!userSet.contains(user.getUsername())){
                continue;
            }
            if (groupName.equals(KylinConfig.getInstanceFromEnv().getLDAPAdminRole())) {
                user.addAuthorities(groupName);
                user.addAuthorities(Constant.ROLE_ADMIN);
            } else {
                user.addAuthorities(groupName);
            }
        }
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
        Integer maxPageSize = KapConfig.getInstanceFromEnv().getLDAPMaxPageSize();
        logger.info("ldap user search config, base: {}, filter: {}, identifier attribute: {}, maxPageSize: {}",
                ldapUserSearchBase, ldapUserSearchFilter, ldapUserIDAttr, maxPageSize);

        final PagedResultsDirContextProcessor processor = new PagedResultsDirContextProcessor(maxPageSize);

        AttributesMapper<String> attributesMapper = attributes -> attributes.get(ldapUserIDAttr).get().toString();

        Set<String> result = new HashSet<>();
        do {
            result.addAll(ldapTemplate.search(ldapUserSearchBase, ldapUserSearchFilter, searchControls,
                    attributesMapper, processor));
        } while (processor.hasMore());
        logger.info("LDAP user info load success");
        return result;
    }

    private Map<String, ManagedUser> getLDAPUsersCache() {
        return Optional.ofNullable(ldapUsersCache.getIfPresent(LDAP_USERS)).map(Collections::unmodifiableMap)
                .orElse(null);
    }

    private void asyncLoadCacheData() {
        if (null != this.getLDAPUsersCache() || LOAD_TASK_STATUS.get()) {
            return;
        }
        Runnable runnable = () -> {
            if (null != this.getLDAPUsersCache()) {
                return;
            }
            if (!LOAD_TASK_STATUS.compareAndSet(Boolean.FALSE, Boolean.TRUE)) {
                return;
            }
            try {
                this.listUsers();
            } catch (Exception e) {
                logger.error("Failed to refresh cache asynchronously", e);
            } finally {
                LOAD_TASK_STATUS.set(Boolean.FALSE);
            }
        };
        try {
            LOAD_TASK_POOL.execute(runnable);
        } catch (Exception e) {
            logger.error("load user cache task error", e);
        }
    }
}
