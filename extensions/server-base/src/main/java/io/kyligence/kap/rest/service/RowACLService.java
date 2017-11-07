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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
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

    //get user's row cond list.Like {user1/group1:{col1[a,b,c], col2[d]}.
    public Map<String, RowACL.ColumnToConds> getColumnToCondsByTable(String project, String table, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return RowACLManager.getInstance(getConfig()).getRowACLByCache(project).getColumnToCondsByTable(table, type);
    }

    //get available users only for frontend to select.
    public List<String> getIdentifiersCanAddRowACL(String project, String table, Set<String> allIdentifiers, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> users = getColumnToCondsByTable(project, table, type).keySet();
        List<String> list = Lists.newArrayList(allIdentifiers);
        list.removeAll(users);
        return list;
    }

    public boolean exists(String project, String name, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return RowACLManager.getInstance(getConfig()).getRowACLByCache(project).contains(name, type);
    }

    public String preview(String project, String table, RowACL.ColumnToConds columnToConds) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (columnToConds == null || columnToConds.isEmpty()) {
            return "";
        }
        checkInputConds(columnToConds);
        return RowACLManager.getInstance(getConfig()).preview(project, table, columnToConds);
    }

    public void addToRowACL(String project, String name, String table, RowACL.ColumnToConds columnToConds, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (columnToConds == null || columnToConds.isEmpty()) {
            return;
        }
        checkInputConds(columnToConds);
        RowACLManager.getInstance(getConfig()).addRowACL(project, name, table, columnToConds, type);
    }

    public void updateRowACL(String project, String name, String table, RowACL.ColumnToConds columnToConds, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        checkInputConds(columnToConds);
        RowACLManager.getInstance(getConfig()).updateRowACL(project, name, table, columnToConds, type);
    }

    public void deleteFromRowACL(String project, String name, String table, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACL(project, name, table, type);
    }

    public void deleteFromRowACL(String project, String name, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACL(project, name, type);
    }

    public void deleteFromRowACLByTbl(String project, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        RowACLManager.getInstance(getConfig()).deleteRowACLByTbl(project, table);
    }

    private void checkInputConds(RowACL.ColumnToConds columnToConds) {
        if (columnToConds == null || columnToConds.isEmpty()) {
            throw new RuntimeException("Operation fail, columnToConds list is empty");
        }
        for (String c : columnToConds.keySet()) {
            if (columnToConds.getCondsByColumn(c).isEmpty()) {
                throw new RuntimeException("Operation fail, input condition list is empty");
            }
        }
    }
}