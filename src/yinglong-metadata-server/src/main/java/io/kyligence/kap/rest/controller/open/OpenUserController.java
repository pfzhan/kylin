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

import io.kyligence.kap.rest.request.UserRequest;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.UserInfoResponse;
import org.apache.kylin.rest.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
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
import io.kyligence.kap.rest.controller.NUserController;
import io.kyligence.kap.rest.request.PasswordChangeRequest;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/user", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class OpenUserController extends NBasicController {
    @Autowired
    private NUserController userController;

    @Autowired
    private UserService userService;

    @ApiOperation(value = "listAllUsers", tags = { "MID" })
    @GetMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DataResult<List<UserInfoResponse>>> listAllUsers(
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "is_case_sensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        List<ManagedUser> usersByFuzzyMatching = userService.getManagedUsersByFuzzMatching(name, isCaseSensitive);
        //LDAP users dose not have authorities
        for (ManagedUser u : usersByFuzzyMatching) {
            userService.completeUserInfo(u);
        }
        List<UserInfoResponse> userInfoResponses = usersByFuzzyMatching.stream().map(UserInfoResponse::new)
                .collect(Collectors.toList());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.get(userInfoResponses, pageOffset, pageSize), "");
    }

    @ApiOperation(value = "createUser", tags = { "MID" })
    @PostMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> createUser(@RequestBody ManagedUser user) throws IOException {
        return userController.createUser(user);
    }

    @ApiOperation(value = "updateUser", tags = { "MID" })
    @PutMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> updateUser(@RequestBody UserRequest user) throws IOException {
        return userController.updateUser(user);
    }

    @ApiOperation(value = "delete", tags = { "MID" })
    @DeleteMapping(value = "/{username:.+}")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> delete(@PathVariable("username") String username) {
        return userController.delete(username);
    }

    @ApiOperation(value = "updateUserPassword", tags = { "MID" })
    @PutMapping(value = "/password")
    @ResponseBody
    public EnvelopeResponse<String> updateUserPassword(@RequestBody PasswordChangeRequest user) {
        return userController.updateUserPassword(user);
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> deleteUser(@RequestBody ManagedUser request) {
        return userController.delete(request.getUsername());
    }

    @ApiOperation(value = "deleteUser", tags = { "MID" })
    @DeleteMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchDelete(@RequestBody List<String> usernames) throws IOException {
        return userController.batchDelete(usernames);
    }

    @ApiOperation(value = "createUser", tags = {
            "MID" }, notes = "Update Body: default_password, locked_time, wrong_time, first_login_failed_time")
    @PostMapping(value = "/batch")
    @ResponseBody
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<String> batchCreate(@RequestBody List<ManagedUser> users) throws IOException {
        return userController.batchCreate(users);
    }
}
