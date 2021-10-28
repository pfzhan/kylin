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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserInfoResponse;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.controller.NBasicController;
import io.kyligence.kap.rest.controller.NUserGroupController;
import io.kyligence.kap.rest.request.UpdateGroupRequest;
import io.kyligence.kap.rest.request.UserGroupRequest;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/user_group", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenUserGroupController extends NBasicController {
    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    private NUserGroupController userGroupController;

    @Autowired
    private UserService userService;

    @ApiOperation(value = "listGroups", tags = { "MID" })
    @GetMapping(value = "/groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<String>>> listGroups(
            @RequestParam(value = "group_name", required = false) String groupName,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<String> groups = userGroupService.listAllAuthorities();
        if (StringUtils.isNotBlank(groupName)) {
            groups = groups.stream().filter(group -> isCaseSensitive ? group.contains(groupName)
                    : StringUtils.containsIgnoreCase(group, groupName)).collect(Collectors.toList());
        }

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, DataResult.get(groups, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "getUsersByGroup", tags = { "MID" })
    @GetMapping(value = "/group_members/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<UserInfoResponse>>> getUsersByGroup(
            @PathVariable(value = "group_name") String groupName,
            @RequestParam(value = "username", required = false) String username,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> members = userGroupService.getGroupMembersByName(groupName);
        if (StringUtils.isNotBlank(username)) {
            members = members.stream().filter(user -> StringUtils.containsIgnoreCase(user.getUsername(), username))
                    .collect(Collectors.toList());
        }
        //LDAP users dose not have authorities
        for (ManagedUser user : members) {
            userService.completeUserInfo(user);
        }
        List<UserInfoResponse> userInfoResponses = members.stream().map(UserInfoResponse::new)
                .collect(Collectors.toList());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(userInfoResponses, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "addUserGroup", tags = { "MID" })
    @PostMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        UserGroupRequest request = new UserGroupRequest();
        request.setGroupName(groupName);
        return userGroupController.addUserGroup(request);
    }

    @ApiOperation(value = "addUserGroupWithBody", tags = { "MID" })
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroupWithBody(@RequestBody UserGroupRequest request) throws IOException {
        return userGroupController.addUserGroup(request);
    }

    @ApiOperation(value = "delUserGroup", tags = { "MID" })
    @DeleteMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        String groupUuid = userGroupService.getUuidByGroupName(groupName);
        return userGroupController.delUserGroup(groupUuid);
    }

    @ApiOperation(value = "delUserGroupWithBody", tags = { "MID" })
    @DeleteMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroupWithBody(@RequestBody UserGroupRequest request) throws IOException {
        userGroupController.checkGroupName(request.getGroupName());
        String groupUuid = userGroupService.getUuidByGroupName(request.getGroupName());
        return userGroupController.delUserGroup(groupUuid);
    }

    @ApiOperation(value = "addOrDelUsersInGroup", tags = { "MID" })
    @PutMapping(value = "/users")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addOrDelUsersInGroup(@RequestBody UpdateGroupRequest updateGroupRequest)
            throws IOException {
        return userGroupController.addOrDelUsers(updateGroupRequest);
    }

    @ApiOperation(value = "getUsersByGroup", tags = { "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchAddUserGroups(@RequestBody List<String> groups) {
        return userGroupController.batchAddUserGroups(groups);
    }

    @ApiOperation(value = "getUsersByGroup", tags = { "MID" }, notes = "Update URL: group_name; Update Param: group_name")
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelUserGroup(@RequestBody List<String> groups) throws IOException {
        return userGroupController.batchDelUserGroup(groups);
    }
}
