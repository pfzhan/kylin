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

import io.kyligence.kap.metadata.acl.ColumnACL;
import io.kyligence.kap.metadata.acl.ColumnACLManager;

@Component("ColumnAclService")
public class ColumnACLService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ColumnACLService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    //get user's black column list by table. Like {user:[col1, col2]}.
    public Map<String, Set<String>> getColumnBlackListByTable(String project, String table) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getColumnBlackListByProject(project).getColumnBlackListByTable(table);
    }

    //get available users only for frontend to select to add column ACL.
    public List<String> getUsersCanAddColumnACL(String project, String table, Set<String> allUsers)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> blockedUsers = getColumnBlackListByTable(project, table).keySet();
        List<String> whiteUsers = new ArrayList<>();
        for (String u : allUsers) {
            if (!blockedUsers.contains(u)) {
                whiteUsers.add(u);
            }
        }
        return whiteUsers;
    }

    ColumnACL getColumnBlackListByProject(String project) throws IOException {
        return ColumnACLManager.getInstance(getConfig()).getColumnACLByCache(project);
    }

    public boolean exists(String project, String username) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return ColumnACLManager.getInstance(getConfig()).getColumnACLByCache(project).getUserColumnBlackList().containsKey(username);
    }

    public void addToColumnBlackList(String project, String username, String table, Set<String> columns)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).addColumnACL(project, username, table, columns);
    }

    public void updateColumnBlackList(String project, String username, String table, Set<String> columns)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).updateColumnACL(project, username, table, columns);
    }

    public void deleteFromTableBlackList(String project, String username, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).deleteColumnACL(project, username, table);
    }

    public void deleteFromTableBlackList(String project, String username) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).deleteColumnACL(project, username);
    }
}
