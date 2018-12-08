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

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public abstract class UserGroupService extends BasicService implements IUserGroupService {

    @Autowired
    AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    // add param project to check user's permission
    public List<String> listAllAuthorities(String project) throws IOException {
        if (StringUtils.isEmpty(project)) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectAdminPermission(project);
        }
        return getAllUserGroups();
    }

    public boolean exists(String name) throws IOException {
        return getAllUserGroups().contains(name);
    }

    public abstract List<ManagedUser> getGroupMembersByName(String name) throws IOException;

    protected abstract List<String> getAllUserGroups() throws IOException;

    public abstract void addGroup(String name) throws IOException;

    public abstract void deleteGroup(String name) throws IOException;

    //user's group information is stored by user its own.Object user group does not hold user's ref.
    public abstract void modifyGroupUsers(String groupName, List<String> users) throws IOException;
}
