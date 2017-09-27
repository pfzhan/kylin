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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.metadata.acl.RowACLManager;

@Component("RowAclService")
public class RowACLService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(RowACLService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    RowACL getRowACL(String project) throws IOException {
        return RowACLManager.getInstance(getConfig()).getRowACLByCache(project);
    }

    //get user's row cond list.Like {user1:{col1[a,b,c], col2[d]}.
    public Map<String, Map<String, List<RowACL.Cond>>> getRowCondsByTable(String project, String table) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return RowACLManager.getInstance(getConfig()).getRowCondListByTable(project, table);
    }

    //get available users only for frontend to select.
    public List<String> getUsersCanAddRowACL(String project, String table, Set<String> allUsers)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> users = getRowCondsByTable(project, table).keySet();
        List<String> availableUsers = new ArrayList<>();

        for (String u : allUsers) {
            if (!users.contains(u)) {
                availableUsers.add(u);
            }
        }
        return availableUsers;
    }

    public boolean exists(String project, String username) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return RowACLManager.getInstance(getConfig()).getRowACLByCache(project).getTableRowCondsWithUser().containsKey(username);
    }

    public String preview(String project, String table, Map<String, List<RowACL.Cond>> condsWithColumn) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (condsWithColumn == null || condsWithColumn.isEmpty()) {
            return "";
        }
        checkInputConds(condsWithColumn);
        return RowACLManager.getInstance(getConfig()).preview(project, table, condsWithColumn);
    }

    public void addToRowCondList(String project, String username, String table,
            Map<String, List<RowACL.Cond>> condsWithColumn) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (condsWithColumn == null || condsWithColumn.isEmpty()) {
            return;
        }
        checkInputConds(condsWithColumn);
        RowACLManager.getInstance(getConfig()).addRowACL(project, username, table, condsWithColumn);
    }

    public void updateToRowCondList(String project, String username, String table,
            Map<String, List<RowACL.Cond>> condsWithColumn) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (condsWithColumn == null || condsWithColumn.isEmpty()) {
            return;
        }
        checkInputConds(condsWithColumn);
        RowACLManager.getInstance(getConfig()).updateRowACL(project, username, table, condsWithColumn);
    }

    public void deleteFromRowCondList(String project, String username, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACL(project, username, table);
    }

    public void deleteFromRowCondList(String project, String username) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACL(project, username);
    }

    public void deleteFromRowCondListByTbl(String project, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACLByTbl(project, table);
    }

    private void checkInputConds(Map<String, List<RowACL.Cond>> condsWithColumn) {
        for (String c : condsWithColumn.keySet()) {
            List<RowACL.Cond> conds = condsWithColumn.get(c);
            if (conds == null || conds.isEmpty()) {
                throw new RuntimeException("Operation fail, input condition list is empty");
            }
        }
    }
}