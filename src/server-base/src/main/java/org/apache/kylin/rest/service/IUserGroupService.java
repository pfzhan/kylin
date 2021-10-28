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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.usergroup.UserGroup;
import io.kyligence.kap.rest.response.UserGroupResponseKI;

public interface IUserGroupService {
    //need project to indicate user's permission.only global admin and project admin can get.
    List<String> listAllAuthorities() throws IOException;

    List<String> getAuthoritiesFilterByGroupName(String userGroupName) throws IOException;

    List<UserGroup> listUserGroups() throws IOException;

    List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) throws IOException;

    String getGroupNameByUuid(String uuid);

    String getUuidByGroupName(String groupName);

    boolean exists(String name) throws IOException;

    List<ManagedUser> getGroupMembersByName(String name) throws IOException;

    List<String> getAllUserGroups() throws IOException;

    Set<String> listUserGroups(String username);

    void addGroup(String name) throws IOException;

    void deleteGroup(String name) throws IOException;

    void modifyGroupUsers(String groupName, List<String> users) throws IOException;

    Map<String, List<String>> getUserAndUserGroup() throws IOException;

    List<UserGroupResponseKI> getUserGroupResponse(List<UserGroup> userGroups) throws IOException;

    void addGroups(List<String> groups);
}