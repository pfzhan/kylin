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
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.stereotype.Component;

/**
 */
@Component("userService")
public class UserService implements UserDetailsManager {

    private Logger logger = LoggerFactory.getLogger(UserService.class);

    public static final String DIR_PREFIX = "/user/";

    public static final Serializer<UserInfo> SERIALIZER = new JsonSerializer<>(UserInfo.class);

    protected ResourceStore aclStore;

    @PostConstruct
    public void init() throws IOException {
        aclStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }

    @Override
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void createUser(UserDetails user) {
        updateUser(user);
    }

    @Override
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void updateUser(UserDetails user) {
        try {
            deleteUser(user.getUsername());
            String id = getId(user.getUsername());
            aclStore.putResource(id, new UserInfo(user), 0, SERIALIZER);
            logger.debug("update user : {}", user.getUsername());
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public void deleteUser(String userName) {
        try {
            String id = getId(userName);
            aclStore.deleteResource(id);
            logger.debug("delete user : {}", userName);
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        try {
            logger.debug("judge user exist: {}", userName);
            return aclStore.exists(getId(userName));
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Message msg = MsgPicker.getMsg();
        try {
            UserInfo userInfo = aclStore.getResource(getId(userName), UserInfo.class, SERIALIZER);
            if (userInfo == null) {
                throw new UsernameNotFoundException(String.format(msg.getUSER_NOT_FOUND(), userName));
            }
            logger.debug("load user : {}", userName);
            return wrap(userInfo);
        } catch (IOException e) {
            throw new InternalErrorException(e);
        }
    }

    public List<String> listUserAuthorities() throws IOException {
        List<String> all = new ArrayList<String>();
        for (UserDetails user : listUsers()) {
            for (GrantedAuthority auth : user.getAuthorities()) {
                if (!all.contains(auth.getAuthority())) {
                    all.add(auth.getAuthority());
                }
            }
        }
        return all;
    }

    public List<UserDetails> listUsers() throws IOException {
        List<UserDetails> all = new ArrayList<UserDetails>();
        List<UserInfo> userInfos = aclStore.getAllResources(DIR_PREFIX, UserInfo.class, SERIALIZER);
        for (UserInfo info : userInfos) {
            all.add(wrap(info));
        }
        return all;
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

    protected User wrap(UserInfo userInfo) {
        if (userInfo == null)
            return null;
        List<GrantedAuthority> authorities = new ArrayList<>();
        List<String> auths = userInfo.getAuthorities();
        for (String str : auths) {
            authorities.add(new UserGrantedAuthority(str));
        }
        return new User(userInfo.getUsername(), userInfo.getPassword(), authorities);
    }

}
