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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
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
import io.kyligence.kap.rest.request.UpdateGroupRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import io.swagger.annotations.ApiOperation;
import lombok.val;

@Controller
@RequestMapping(value = "/api/user_group", produces = { HTTP_VND_APACHE_KYLIN_JSON,
        HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class NUserGroupController extends NBasicController {

    private static final Pattern sidPattern = Pattern.compile("^[a-zA-Z0-9_ ]*$");

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update URL: group_members, group_name; Update Param: group_name, page_offset, page_size; Update Response: total_size")
    @GetMapping(value = "/group_members/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<ManagedUser>>> getUsersByGroup(
            @PathVariable(value = "group_name") String groupName,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> members = userGroupService.getGroupMembersByName(groupName);
        val subList = PagingUtil.cutPage(members, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser user : subList) {
            userService.completeUserInfo(user);
        }

        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(subList, members, pageOffset, pageSize),
                "get groups members");
    }

    @GetMapping(value = "/groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<List<String>> listUserAuthorities(@RequestParam(value = "project") String project)
            throws IOException {
        List<String> groups = userGroupService.listAllAuthorities(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, groups, "get groups");
    }

    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update URL: users_with_group; Update Param: page_offset, page_size, user_group_name; Update Response: total_size")
    @GetMapping(value = "/users_with_group")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<Pair<String, Set<String>>>>> getUsersWithGroup(
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "user_group_name", required = false, defaultValue = "") String userGroupName)
            throws IOException {
        List<Pair<String, Set<String>>> usersWithGroup = new ArrayList<>();
        List<String> groups = userGroupService.getAuthoritiesFilterByGroupName(userGroupName);

        List<String> subList = PagingUtil.cutPage(groups, pageOffset, pageSize);
        for (String group : subList) {
            Set<String> groupMembers = new TreeSet<>();
            for (ManagedUser user : userGroupService.getGroupMembersByName(group)) {
                groupMembers.add(user.getUsername());
            }
            usersWithGroup.add(Pair.newPair(group, groupMembers));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, DataResult.get(usersWithGroup, groups.size()),
                "get users with group");
    }

    @GetMapping(value = "/users_and_groups")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<Map<String, List<String>>> getUsersAndGroups() throws IOException {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, userGroupService.getUserAndUserGroup(), "");
    }

    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update URL: group_name; Update Param: group_name")
    @PostMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        checkGroupName(groupName);
        userGroupService.addGroup(groupName);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "add user group");
    }

    @ApiOperation(value = "getUsersByGroup (update)", notes = "Update URL: group_name; Update Param: group_name")
    @DeleteMapping(value = "/{group_name:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delUserGroup(@PathVariable(value = "group_name") String groupName)
            throws IOException {
        userGroupService.deleteGroup(groupName);
        aclTCRService.revokeAclTCR(groupName, false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "del user group");
    }

    //move users in/out from groups
    @PutMapping(value = "/users")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> addOrDelUsers(@RequestBody UpdateGroupRequest updateGroupRequest)
            throws IOException {
        userGroupService.modifyGroupUsers(updateGroupRequest.getGroup(), updateGroupRequest.getUsers());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "modify users in user group");
    }

    private void checkGroupName(String groupName) {
        if (StringUtils.isEmpty(groupName)) {
            throw new BadRequestException(MsgPicker.getMsg().getEMPTY_GROUP_NAME());
        }
        if (!sidPattern.matcher(groupName).matches()) {
            throw new BadRequestException(MsgPicker.getMsg().getINVALID_SID());
        }
    }
}