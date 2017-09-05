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
import java.util.List;

import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.util.ValidateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.ColumnACLService;

@Controller
@RequestMapping(value = "/acl")
public class ColumnAclController extends BasicController {

    @Autowired
    @Qualifier("ColumnAclService")
    private ColumnACLService columnACLService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @RequestMapping(value = "/column/{project}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getUserColumnBlackListByTable(@PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.vaildateArgs(project, table);
        project = project.toUpperCase();
        validateUtil.validateTable(project, table);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, columnACLService.getColumnBlackListByTable(project, table), "get table acl");
    }

    @RequestMapping(value = "/column/white/{project}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse getUserColumnWhiteListByTable(@PathVariable String project, @PathVariable String table) throws IOException {
        project = project.toUpperCase();
        validateUtil.validateTable(project, table);
        List<String> allUsers = validateUtil.getAllUsers();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, columnACLService.getColumnUserWhiteListByTable(project, table, allUsers), "get available user");
    }

    @RequestMapping(value = "/column/{project}/{table}/{username}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse putUserToColumnBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username,
            @RequestBody List<String> columns) throws IOException {
        validateUtil.vaildateArgs(project, table, username);
        project = project.toUpperCase();
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, columns);
        columnACLService.addToColumnBlackList(project, username, table, columns);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "add user to column balck list.");
    }

    @RequestMapping(value = "/column/{project}/{table}/{username}", method = {RequestMethod.PUT}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse updateUserColumnBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username,
            @RequestBody List<String> columns) throws IOException {
        validateUtil.vaildateArgs(project, table, username);
        project = project.toUpperCase();
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, columns);
        columnACLService.updateColumnBlackList(project, username, table, columns);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "update user's black column list");
    }

    @RequestMapping(value = "/column/{project}/{table}/{username}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse deleteUserFromColumnBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.vaildateArgs(project, table, username);
        project = project.toUpperCase();
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        columnACLService.deleteFromTableBlackList(project, username, table);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "",
                "delete user from " + table + "'s column black list");
    }
}
