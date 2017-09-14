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

package io.kyligence.kap.rest.controller2;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.TableACLService;
import org.apache.kylin.rest.util.ValidateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/acl")
public class TableAclControllerV2 extends BasicController {

    @Autowired
    @Qualifier("TableAclService")
    private TableACLService tableACLService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @RequestMapping(value = "/table/{project}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<List<String>> getUsersCanQueryTheTbl(@PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Set<String> allUsers = validateUtil.getAllUsers(project);
        List<String> whiteList = tableACLService.getUsersCanQueryTheTbl(project, table, allUsers);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, whiteList, "get table acl");
    }

    @RequestMapping(value = "/table/{project}/black/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<List<String>> getUsersCannotQueryTheTbl(@PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        List<String> blackList = tableACLService.getUsersCannotQueryTheTbl(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, blackList, "get table acl");
    }

    // because the frontend passes user can not visit, so that means put it to the table black list
    @RequestMapping(value = "/table/{project}/{table}/{username}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> putUserToTableBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        tableACLService.addToTableBlackList(project, username, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "revoke user table query permission and add user to table black list.");
    }

    // because the frontend passes user can visit, so that means remove the user from the table black list
    @RequestMapping(value = "/table/{project}/{table}/{username}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> deleteUserFromTableBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        tableACLService.deleteFromTableBlackList(project, username, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "grant user table query permission and remove user from table black list.");
    }
}
