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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.KylinUserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.userdetails.UserDetails;

import io.kyligence.kap.metadata.user.ManagedUser;

public class CaseInsensitiveKylinUserService extends KylinUserService {

    private final Logger logger = LoggerFactory.getLogger(CaseInsensitiveKylinUserService.class);

    @Override
    public boolean userExists(String userName) {
        logger.trace("judge user exist: {}", userName);
        return getKylinUserManager().exists(userName);
    }

    @Override
    public List<String> listAdminUsers() throws IOException {
        return listUsers().parallelStream()
                .filter(managedUser -> managedUser.getAuthorities().parallelStream().anyMatch(
                        simpleGrantedAuthority -> Constant.ROLE_ADMIN.equals(simpleGrantedAuthority.getAuthority())))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public List<String> listNormalUsers() throws IOException {
        return listUsers()
                .parallelStream()
                .filter(managedUser -> managedUser.getAuthorities().parallelStream().noneMatch(
                        simpleGrantedAuthority -> Constant.ROLE_ADMIN.equals(simpleGrantedAuthority.getAuthority())))
                .map(ManagedUser::getUsername).collect(Collectors.toList());
    }

    @Override
    public boolean isGlobalAdmin(String username) throws IOException {
        try {
            UserDetails userDetails = loadUserByUsername(username);
            return isGlobalAdmin(userDetails);
        } catch (Exception e) {
            logger.warn("Cat not load user by username {}", username, e);
        }

        return false;
    }

    @Override
    public boolean isGlobalAdmin(UserDetails userDetails) throws IOException {
        return userDetails != null && userDetails.getAuthorities().stream()
                .anyMatch(grantedAuthority -> grantedAuthority.getAuthority().equals(ROLE_ADMIN));
    }

    @Override
    public Set<String> retainsNormalUser(Set<String> usernames) throws IOException {
        Set<String> results = new HashSet<>();
        for (String username : usernames) {
            if (!isGlobalAdmin(username)) {
                results.add(username);
            }
        }
        return results;
    }

    @Override
    public boolean containsGlobalAdmin(Set<String> usernames) throws IOException {
        for (String username : usernames) {
            if (isGlobalAdmin(username)) {
                return true;
            }
        }
        return false;
    }
}
