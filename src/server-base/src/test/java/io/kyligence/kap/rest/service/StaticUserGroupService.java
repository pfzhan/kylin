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
import java.util.List;
import java.util.Map;

import org.apache.kylin.rest.constant.Constant;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.user.ManagedUser;

public class StaticUserGroupService extends OpenUserGroupService {
    @Override
    public Map<String, List<String>> getUserAndUserGroup() throws IOException {
        Map<String, List<String>> result = Maps.newHashMap();
        List<ManagedUser> users = userService.listUsers();
        for (ManagedUser user : users) {
            for (SimpleGrantedAuthority authority : user.getAuthorities()) {
                String role = authority.getAuthority();
                List<String> usersInGroup = result.get(role);
                if (usersInGroup == null) {
                    result.put(role, Lists.newArrayList(user.getUsername()));
                } else {
                    usersInGroup.add(user.getUsername());
                }
            }
        }
        return result;
    }

    @Override
    public List<ManagedUser> getGroupMembersByName(String name) {
        try {
            List<ManagedUser> ret = Lists.newArrayList();
            List<ManagedUser> managedUsers = userService.listUsers();
            for (ManagedUser user : managedUsers) {
                if (user.getAuthorities().contains(new SimpleGrantedAuthority(name))) {
                    ret.add(user);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException("");
        }
    }

    @Override
    public List<String> getAllUserGroups() {
        List<String> groups = Lists.newArrayList();
        groups.add(Constant.ROLE_ADMIN);
        groups.add(Constant.ROLE_ANALYST);
        return groups;
    }
}
