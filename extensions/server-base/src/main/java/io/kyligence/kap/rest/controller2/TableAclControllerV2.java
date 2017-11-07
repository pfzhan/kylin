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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.MetadataConstants;
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
    public EnvelopeResponse<List<Pair<String, String>>> getAllIdentifiersCanQueryTheTbl(
            @PathVariable String project,
            @PathVariable String table
    ) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Set<String> allUsers = validateUtil.getAllIdentifiers(project, MetadataConstants.TYPE_USER);
        Set<String> allGroups = validateUtil.getAllIdentifiers(project, MetadataConstants.TYPE_GROUP);

        List<String> users = tableACLService.getCanAccessList(project, table, allUsers, MetadataConstants.TYPE_USER);
        List<String> groups = tableACLService.getCanAccessList(project, table, allGroups, MetadataConstants.TYPE_GROUP);

        List<Pair<String, String>> results = new ArrayList<>();
        for (String user : users) {
            results.add(Pair.newPair(user, "u"));
        }
        for (String group : groups) {
            results.add(Pair.newPair(group, "g"));
        }

        Collections.sort(results, new Comparator<Pair<String, String>>() {
            @Override
            public int compare(Pair<String, String> o1, Pair<String, String> o2) {
                return o1.getFirst().compareToIgnoreCase(o2.getFirst());
            }
        });
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, results, "get table acl");
    }

    @RequestMapping(value = "/table/{project}/{type}/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<List<String>> getIdentifiersCanQueryTheTbl(@PathVariable String project, @PathVariable String type, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        Set<String> allIdentifiers = validateUtil.getAllIdentifiers(project, type);
        List<String> whiteList = tableACLService.getCanAccessList(project, table, allIdentifiers, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, whiteList, "get table acl");
    }

    @RequestMapping(value = "/table/{project}/{type}/black/{table:.+}", method = {RequestMethod.GET}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<List<String>> getIdentifiersCannotQueryTheTbl(@PathVariable String project, @PathVariable String type, @PathVariable String table) throws IOException {
        validateUtil.validateArgs(project, table);
        validateUtil.validateTable(project, table);
        List<String> blackList = tableACLService.getNoAccessList(project, table, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, blackList, "get table acl");
    }

    // because the frontend passes user can not visit, so that means put it to the table black list
    @RequestMapping(value = "/table/{project}/{type}/{table}/{name}", method = {RequestMethod.DELETE}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> putIdentifiersToTableBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String name) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(name, type);
        validateUtil.validateTable(project, table);
        tableACLService.addToTableACL(project, name, table, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "revoke user table query permission and add user to table black list.");
    }

    // because the frontend passes user can visit, so that means remove the user from the table black list
    @RequestMapping(value = "/table/{project}/{type}/{table}/{name}", method = {RequestMethod.POST}, produces = {"application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse<String> deleteIdentifiersFromTableBlackList(
            @PathVariable String project,
            @PathVariable String type,
            @PathVariable String table,
            @PathVariable String name) throws IOException {
        validateUtil.validateArgs(project, table, name);
        validateUtil.validateIdentifiers(name, type);
        validateUtil.validateTable(project, table);
        tableACLService.deleteFromTableACL(project, name, table, type);
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "grant user table query permission and remove user from table black list.");
    }
}
