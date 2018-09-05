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

import com.google.common.collect.ImmutableSet;
import io.kyligence.kap.metadata.acl.ColumnACL;
import io.kyligence.kap.metadata.acl.ColumnACLManager;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("ColumnAclService")
public class ColumnACLService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(ColumnACLService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    public Map<String, Set<String>> getColumnBlackListByTable(String project, String table, String type)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        return getColumnACL(project).getColumnBlackListByTable(table, type);
    }

    public List<String> getCanAccessList(String project, String table, Set<String> allIdentifiers, String type) {
        aclEvaluate.checkProjectWritePermission(project);
        return getColumnACL(project).getCanAccessList(table, allIdentifiers, type);
    }

    public boolean exists(String project, String name, String type) {
        aclEvaluate.checkProjectWritePermission(project);
        return getColumnACL(project).contains(name, type);
    }

    public void addToColumnACL(String project, String name, String table, Set<String> columns, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).addColumnACL(project, name, table, columns, type);
    }

    public List<String> batchOverwirteToColumnACL(String project, Map<String, List<String>> userWithColumns, String table,
            String type) throws IOException {
        List<String> failUsers = new ArrayList<String>();
        for (String name : userWithColumns.keySet()) {
            List<String> columnList = userWithColumns.get(name);
            Set<String> columns = ImmutableSet.copyOf(columnList);
            try {
                if (exists(project, name, type)) {
                    updateColumnACL(project, name, table, columns, type);
                } else {
                    addToColumnACL(project, name, table, columns, type);
                }
            } catch (Exception e) {
                logger.error("user : " + name + " add or update column acl for tabel: " + table + "  fail!", e);
                failUsers.add(name);
            }
        }
        return failUsers;
    }

    public void updateColumnACL(String project, String name, String table, Set<String> columns, String type)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).updateColumnACL(project, name, table, columns, type);
    }

    public void deleteFromColumnACL(String project, String name, String table, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).deleteColumnACL(project, name, table, type);
    }

    public void deleteFromColumnACL(String project, String name, String type) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).deleteColumnACL(project, name, type);
    }

    public void deleteFromColumnACLByTbl(String project, String table) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        ColumnACLManager.getInstance(getConfig()).deleteColumnACLByTbl(project, table);
    }

    private ColumnACL getColumnACL(String project) {
        return ColumnACLManager.getInstance(getConfig()).getColumnACLByCache(project);
    }
}
