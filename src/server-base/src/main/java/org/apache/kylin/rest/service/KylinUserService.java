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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATE_USER_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

public class KylinUserService implements UserService {

    private Logger logger = LoggerFactory.getLogger(KylinUserService.class);

    public static final String DIR_PREFIX = "/user/";

    public static final String SUPER_ADMIN = "ADMIN";

    public static final Serializer<ManagedUser> SERIALIZER = new JsonSerializer<>(ManagedUser.class);

    @Override
    @Transaction
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void createUser(UserDetails user) {
        if (getKylinUserManager().exists(user.getUsername())) {
            throw new KylinException(DUPLICATE_USER_NAME,
                    String.format(MsgPicker.getMsg().getUSER_EXISTS(), user.getUsername()));
        }
        updateUser(user);
    }

    @Override
    @Transaction
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN) --- DON'T DO THIS, CAUSES CIRCULAR DEPENDENCY BETWEEN UserService & AclService
    public void updateUser(UserDetails user) {
        Preconditions.checkState(user instanceof ManagedUser, "User {} is not ManagedUser", user);
        ManagedUser managedUser = (ManagedUser) user;

        if (!managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.GROUP_ALL_USERS))) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_REMOVE_USER_FROM_ALL_USER());
        }
        getKylinUserManager().update(managedUser);
        logger.trace("update user : {}", user.getUsername());
    }

    @Override
    @Transaction
    public void deleteUser(String userName) {
        if (userName.equals(SUPER_ADMIN)) {
            throw new InternalErrorException("User " + userName + " is not allowed to be deleted.");
        }

        getKylinUserManager().delete(userName);
        logger.trace("delete user : {}", userName);
    }

    @Override
    public void changePassword(String oldPassword, String newPassword) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean userExists(String userName) {
        logger.trace("judge user exist: {}", userName);
        val users = listUsers();
        for (val user : users) {
            if (StringUtils.equalsIgnoreCase(userName, user.getUsername())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @return a ManagedUser
     */
    @Override
    public UserDetails loadUserByUsername(String userName) throws UsernameNotFoundException {
        Message msg = MsgPicker.getMsg();
        ManagedUser managedUser = null;
        try {
            managedUser = getKylinUserManager().get(userName);
        } catch (IllegalArgumentException e) {
            logger.error("exception: ", e);
            throw new UsernameNotFoundException(String.format(msg.getUSER_AUTH_FAILED()));
        }
        if (managedUser == null) {
            throw new UsernameNotFoundException(String.format(msg.getUSER_NOT_FOUND(), userName));
        }
        logger.trace("load user : {}", userName);
        return managedUser;
    }

    @Override
    public List<ManagedUser> listUsers() {
        return getKylinUserManager().list();
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        List<String> adminUsers = new ArrayList<>();
        for (ManagedUser managedUser : listUsers()) {
            if (managedUser.getAuthorities().contains(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))) {
                adminUsers.add(managedUser.getUsername());
            }
        }
        return adminUsers;
    }

    @Override
    public void completeUserInfo(ManagedUser user) {
    }

    public static String getId(String userName) {
        return DIR_PREFIX + userName;
    }

    private NKylinUserManager getKylinUserManager() {
        return NKylinUserManager.getInstance(KylinConfig.getInstanceFromEnv());
    }
}
