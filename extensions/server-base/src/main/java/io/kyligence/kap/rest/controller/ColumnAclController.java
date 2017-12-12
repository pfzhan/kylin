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

import io.kyligence.kap.rest.PagingUtil;
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
    public EnvelopeResponse<Map<Pair<String, String>, Set<String>>> getAllColumnBlackListByTable(
            @PathVariable String project,
            @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Map<String, Set<String>> userBlackList = columnACLService.getColumnBlackListByTable(project, table, MetadataConstants.TYPE_USER);
        Map<String, Set<String>> groupBlackList = columnACLService.getColumnBlackListByTable(project, table, MetadataConstants.TYPE_GROUP);
        Map<Pair<String, String>, Set<String>> results = new TreeMap<>(new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                return o1.getFirst().compareToIgnoreCase(o2.getFirst());
            }
        });
        for (String u : userBlackList.keySet()) {
            results.put(Pair.newPair(u, "u"), userBlackList.get(u));
        }
        for (String g : groupBlackList.keySet()) {
            results.put(Pair.newPair(g, "g"), groupBlackList.get(g));
        }
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, results, "get column acl");
    }

    @RequestMapping(value = "/column/{project}/{type}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<java.util.Map<String, Set<String>>> getColumnBlackListByTable(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, columnACLService.getColumnBlackListByTable(project, table, type), "get column acl");
    }

    @RequestMapping(value = "/column/white/{project}/{type}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<HashMap<String, Object>> getIdentifiersCanAddColumnACL(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @RequestParam(value = "name", required = false) String name,
            @RequestParam(value = "isCaseSensitive", required = false) boolean isCaseSensitive,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize) throws IOException {
        validateUtil.validateTable(project, table);
        Set<String> allIdentifiers = validateUtil.getAllIdentifiers(project, type);
        //for name fuzzy matching
        List<String> usersByFuzzyMatching = PagingUtil.getIdentifierAfterFuzzyMatching(name, isCaseSensitive, columnACLService.getCanAccessList(project, table, allIdentifiers, type));

        HashMap<String, Object> data = new HashMap<>();
        data.put("users", PagingUtil.cutPage(usersByFuzzyMatching, pageOffset, pageSize));
        data.put("size", usersByFuzzyMatching.size());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, data, "get users can add column ACL");
    }

    @RequestMapping(value = "/column/{project}/{type}/{table}/{username}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> putIdentifierToColumnBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String username,
            @RequestBody Set<String> columns) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateIdentifiers(username, type);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, columns);
        columnACLService.addToColumnACL(project, username, table, columns, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "add user to column balck list.");
    }

    @RequestMapping(value = "/column/{project}/{type}/{table}/{username}", method = {RequestMethod.PUT}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> updateIdentifierColumnBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String username,
            @RequestBody Set<String> columns) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateIdentifiers(username, type);
        validateUtil.validateTable(project, table);
        validateUtil.validateColumn(project, table, columns);
        columnACLService.updateColumnACL(project, username, table, columns, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "update user's black column list");
    }

    @RequestMapping(value = "/column/{project}/{type}/{table}/{username}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> deleteIdentifierFromColumnBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String username) throws IOException {
        validateUtil.validateArgs(project, table, username);
        validateUtil.validateIdentifiers(username, type);
        validateUtil.validateTable(project, table);
        columnACLService.deleteFromColumnACL(project, username, table, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "",
                "delete user from " + table + "'s column black list");
    }
}
