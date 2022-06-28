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

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import io.kyligence.kap.common.annotation.ThirdPartyDependencies;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.msg.MsgPicker;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.metadata.usergroup.UserGroup;

@ThirdPartyDependencies({
        @ThirdPartyDependencies.ThirdPartyDependent(repository = "static-user-manager",
                classes = {"StaticUserGroupService", "StaticUserService"})
})
public abstract class OpenUserGroupService extends NUserGroupService {

    public abstract List<ManagedUser> getGroupMembersByName(String name);

    public abstract List<String> getAllUserGroups();

    @Override
    public List<UserGroup> listUserGroups() {
        return getUserGroupSpecialUuid();
    }

    @Override
    public void addGroup(String name) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "addGroup"));
    }

    @Override
    public void deleteGroup(String name) {
        throw new UnsupportedOperationException(
                String.format(Locale.ROOT, MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "deleteGroup"));
    }

    @Override
    public void modifyGroupUsers(String groupName, List<String> users) {
        throw new UnsupportedOperationException(String.format(Locale.ROOT,
                MsgPicker.getMsg().getGroupEditNotAllowedForCustom(), "modifyGroupUsers"));
    }

    @Override
    public String getGroupNameByUuid(String uuid) {
        return uuid;
    }

    @Override
    public String getUuidByGroupName(String groupName) {
        return groupName;
    }

    @Override
    public List<UserGroup> getUserGroupsFilterByGroupName(String userGroupName) {
        aclEvaluate.checkIsGlobalAdmin();
        return StringUtils.isEmpty(userGroupName) ? getUserGroupSpecialUuid()
                : getUserGroupSpecialUuid().stream().filter(userGroup -> userGroup.getGroupName()
                .toUpperCase(Locale.ROOT).contains(userGroupName.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
    }

}
