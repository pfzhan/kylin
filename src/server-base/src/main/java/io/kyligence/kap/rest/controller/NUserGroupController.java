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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.rest.request.UpdateGroupRequest;
import io.kyligence.kap.rest.service.AclTCRService;
import lombok.val;

@Controller
@RequestMapping(value = "/user_group")
public class NUserGroupController extends NBasicController {

    @Autowired
    @Qualifier("userGroupService")
    private IUserGroupService userGroupService;

    @Autowired
    @Qualifier("userService")
    private UserService userService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    private static final Message msg = MsgPicker.getMsg();

    @RequestMapping(value = "/groupMembers/{groupName:.+}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getUsersByGroup(@PathVariable(value = "groupName") String groupName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> members = userGroupService.getGroupMembersByName(groupName);
        val subList = PagingUtil.cutPage(members, pageOffset, pageSize);
        //LDAP users dose not have authorities
        for (ManagedUser user : subList) {
            userService.completeUserInfo(user);
        }
        Map<String, Object> result = new HashMap<>();
        result.put("groupMembers", subList);
        result.put("size", members.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "get groups members");
    }

    @RequestMapping(value = "/groups", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<List<String>> listUserAuthorities(@RequestParam(value = "project") String project)
            throws IOException {
        List<String> groups = userGroupService.listAllAuthorities(project);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, groups, "get groups");
    }

    @RequestMapping(value = "/usersWithGroup", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getUsersWithGroup(
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize,
            @RequestParam(value = "userGroupName", required = false, defaultValue = "") String userGroupName)
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
        Map<String, Object> result = new HashMap<>();
        result.put("usersWithGroup", usersWithGroup);
        result.put("size", groups.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, result, "get users with group");
    }

    @RequestMapping(value = "/users_and_groups", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getUsersAndGroups() throws IOException {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, userGroupService.getUserAndUserGroup(), "");
    }

    @RequestMapping(value = "/{groupName:.+}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addUserGroup(@PathVariable(value = "groupName") String groupName)
            throws IOException {
        checkGroupName(groupName);
        userGroupService.addGroup(groupName);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "add user group");
    }

    @RequestMapping(value = "/{groupName:.+}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> delUserGroup(@PathVariable(value = "groupName") String groupName)
            throws IOException {
        userGroupService.deleteGroup(groupName);
        aclTCRService.revokeAclTCR(groupName, false);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "del user group");
    }

    //move users in/out from groups
    @RequestMapping(value = "/users", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addOrDelUsers(@RequestBody UpdateGroupRequest updateGroupRequest)
            throws IOException {
        userGroupService.modifyGroupUsers(updateGroupRequest.getGroup(), updateGroupRequest.getUsers());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "modify users in user group");
    }

    private void checkGroupName(String groupName) {
        if (StringUtils.isEmpty(groupName)) {
            throw new BadRequestException(msg.getEMPTY_GROUP_NAME());
        }
    }
}