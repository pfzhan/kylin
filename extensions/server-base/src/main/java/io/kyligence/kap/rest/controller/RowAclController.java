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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.rest.PagingUtil;
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

    @RequestMapping(value = "/row/{project}/{table:.+}", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<Map<Pair<String, String>, RowACL.ColumnToConds>> getUserColumnBlackListByTable(
            @PathVariable String project,
            @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Map<String, RowACL.ColumnToConds> userRowConds = rowACLService.getColumnToCondsByTable(project, table, MetadataConstants.TYPE_USER);
        Map<String, RowACL.ColumnToConds> groupRowConds = rowACLService.getColumnToCondsByTable(project, table, MetadataConstants.TYPE_GROUP);
        Map<Pair<String, String>, RowACL.ColumnToConds> results = new TreeMap<>(new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                return o1.getFirst().compareToIgnoreCase(o2.getFirst());
            }
        });
        for (String u : userRowConds.keySet()) {
            results.put(Pair.newPair(u, "u"), userRowConds.get(u));
        }
        for (String g : groupRowConds.keySet()) {
            results.put(Pair.newPair(g, "g"), groupRowConds.get(g));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, results, "get column cond list in table");
    }

    @RequestMapping(value = "/row/{project}/{type}/{table:.+}", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<Map<String, RowACL.ColumnToConds>> getUserColumnBlackListByTable(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, rowACLService.getColumnToCondsByTable(project, table, type),
                "get column cond list in table");
    }

    @RequestMapping(value = "/row/white/{project}/{type}/{table:.+}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<HashMap<String, Object>> getUsersCanAddRowACL(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Set<String> allIdentifiers = validateUtil.getAllIdentifiers(project, type);
        //for name fuzzy matching
        List<String> usersByFuzzyMatching = PagingUtil.getUsersByFuzzyMatching(name, isCaseSensitive, rowACLService.getIdentifiersCanAddRowACL(project, table, allIdentifiers, type));

        HashMap<String, Object> data = new HashMap<>();
        data.put("users", PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize));
        data.put("size", usersByFuzzyMatching.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "get users can add row ACL");
    }

    @RequestMapping(value = "/row/{project}/{type}/{table}/{name}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addRowConds(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String name,
            @PathVariable String type,
            @RequestBody RowACL.ColumnToConds condsWithColumn) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(name, type);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        rowACLService.addToRowACL(project, name, table, condsWithColumn, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "add user row cond list.");
    }

    @RequestMapping(value = "/row/preview/{project}/{table:.+}", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> preview(
            @PathVariable String project,
            @PathVariable String table,
            @RequestBody RowACL.ColumnToConds condsWithColumn) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, rowACLService.preview(project, table, condsWithColumn),
                "add user row cond list.");
    }

    @RequestMapping(value = "/row/{project}/{type}/{table}/{name}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> updateUserColumnBlackList(
            @PathVariable String project,
            @PathVariable String table,
            @PathVariable String name,
            @PathVariable String type,
            @RequestBody RowACL.ColumnToConds condsWithColumn) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(name, type);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, condsWithColumn.keySet());
        rowACLService.updateRowACL(project, name, table, condsWithColumn, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "update user's row cond list");
    }

    @RequestMapping(value = "/row/{project}/{type}/{table}/{name}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> deleteUserFromColumnBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String name,
            @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(name, type);
        validateUtil.validateTable(project, table);
        rowACLService.deleteFromRowACL(project, name, table, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "delete user's row cond list");
    }
}
