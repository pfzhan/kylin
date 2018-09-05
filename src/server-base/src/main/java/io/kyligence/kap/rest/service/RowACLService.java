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

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.acl.ColumnToConds;
import io.kyligence.kap.metadata.acl.RowACL;
import io.kyligence.kap.metadata.acl.RowACLManager;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("RowAclService")
public class RowACLService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(RowACLService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    //get user/group's row cond list.Like {user1/group1:{col1[a,b,c], col2[d]}.
    public Map<String, ColumnToConds> getRowACLByTable(String project, String table, String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getRowACLManager().getRowACLByTable(project, type, table);
    }

    //get available users only for frontend to select.
    public List<String> getIdentifiersCanAddRowACL(String project, String table, Set<String> allIdentifiers,
            String type) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        Collection<String> exists = getRowACLIdentifiersByTable(project, type, table);
        List<String> list = Lists.newArrayList(allIdentifiers);
        list.removeAll(exists);
        return list;
    }

    public Collection<String> getRowACLIdentifiersByTable(String project, String type, String table) {
        return getRowACLManager().getRowACLEntitesByTable(project, type, table);
    }

    public RowACL getRowACL(String project, String type, String name, String table) {
        return getRowACLManager().getRowACL(project, type, name, table);
    }

    public String preview(String project, String table, ColumnToConds columnToConds) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (columnToConds == null || columnToConds.isEmpty()) {
            return "";
        }
        checkInputConds(columnToConds);
        return ColumnToConds.preview(project, table, columnToConds);
    }

    public void addToRowACL(String project, String name, String table, ColumnToConds columnToConds, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        if (columnToConds == null || columnToConds.isEmpty()) {
            return;
        }
        checkInputConds(columnToConds);
        getRowACLManager().addRowACL(project, type, name, table, columnToConds);
    }

    public List<String> batchOverwriteRowACL(String project, Map<String, ColumnToConds> userWithConds, String table,
                                             String type) throws IOException {
        List<String> failUsers = new ArrayList<String>();
        for (String name : userWithConds.keySet()) {
            ColumnToConds columnToConds = userWithConds.get(name);
            try {
                if (getRowACL(project, type, name, table) == null) {
                    addToRowACL(project, name, table, columnToConds, type);
                } else {
                    updateRowACL(project, name, table, columnToConds, type);
                }
            } catch (Exception e) {
                logger.error("user : " + name + " add or update row acl for tabel: " + table + "  fail!", e);
                failUsers.add(name);
            }
        }
        return failUsers;
    }

    public void updateRowACL(String project, String name, String table, ColumnToConds columnToConds, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        checkInputConds(columnToConds);
        getRowACLManager().updateRowACL(project, type, name, table, columnToConds);
    }

    public void deleteFromRowACL(String project, String name, String table, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        getRowACLManager().deleteRowACL(project, type, name, table);
    }

    private void checkInputConds(ColumnToConds columnToConds) {
        if (columnToConds == null || columnToConds.isEmpty()) {
            throw new RuntimeException("Operation fail, columnToConds list is empty");
        }
        for (String c : columnToConds.keySet()) {
            if (columnToConds.getCondsByColumn(c).isEmpty()) {
                throw new RuntimeException("Operation fail, input condition list is empty");
            }
        }
    }

    private RowACLManager getRowACLManager() {
        return RowACLManager.getInstance(getConfig());
    }
}