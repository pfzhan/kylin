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
import java.util.Map;

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

import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.rest.service.RowACLService;

@Controller
@RequestMapping(value = "/acl")
public class RowAclController extends BasicController {

    @Autowired
    @Qualifier("RowAclService")
    private RowACLService rowACLService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @RequestMapping(value = "/row/{project}/{table:.+}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<Map<String, Map<String, List<RowACL.Cond>>>> getUserColumnBlackListByTable(
            @PathVariable String project, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, rowACLService.getRowCondsByTable(project, table),
                "get column cond list in table");
    }

    @RequestMapping(value = "/row/available_user/{project}/{table:.+}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<List<String>> getUsersCanAddRowACL(@PathVariable String project, @PathVariable String table)
            throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS,
                rowACLService.getUsersCanAddRowACL(project, table, validateUtil.getAllUsers(project)),
                "get row cond list in table");
    }

    @RequestMapping(value = "/row/{project}/{table}/{username}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addRowConds(@PathVariable String project, @PathVariable String table,
            @PathVariable String username, @RequestBody Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        rowACLService.addToRowCondList(project, username, table, condsWithColumn);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "add user row cond list.");
    }

    @RequestMapping(value = "/row/preview/{project}/{table}/{username}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> preview(@PathVariable String project, @PathVariable String table,
            @PathVariable String username, @RequestBody Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, rowACLService.preview(project, table, condsWithColumn),
                "add user row cond list.");
    }

    @RequestMapping(value = "/row/{project}/{table}/{username}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> updateUserColumnBlackList(@PathVariable String project, @PathVariable String table,
            @PathVariable String username, @RequestBody Map<String, List<RowACL.Cond>> condsWithColumn)
            throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        rowACLService.updateToRowCondList(project, username, table, condsWithColumn);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "update user's row cond list");
    }

    @RequestMapping(value = "/row/{project}/{table}/{username}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> deleteUserFromColumnBlackList(@PathVariable String project,
            @PathVariable String username, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateUser(username);
        validateUtil.validateTable(project, table);
        rowACLService.deleteFromRowCondList(project, username, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "delete user's row cond list");
    }
}
