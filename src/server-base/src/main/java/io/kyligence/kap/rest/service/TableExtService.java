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

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.persistence.AclEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.acl.TableACL;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.security.AclEntityType;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.ACLOperationUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.Sid;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.List;
import java.util.Set;

@Component("tableExtService")
public class TableExtService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(TableExtService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("tableAclService")
    private TableACLService tableACLService;

    @Autowired
    @Qualifier("ColumnAclService")
    private ColumnACLService columnAclService;

    @Autowired
    @Qualifier("RowAclService")
    private RowACLService rowAclService;

    /**
     * Load a group of  tables
     * 
     * @return an array of table name sets:
     *         [0] : tables that loaded successfully
     *         [1] : tables that didn't load due to running sample job todo
     *         [2] : tables that didn't load due to other error
     *
     * @throws Exception if reading hive metadata error
     */
    public Set<String>[] loadTables(String[] tables, String project, Integer sourceType) throws Exception {
        aclEvaluate.checkProjectAdminPermission(project);
        List<Pair<TableDesc, TableExtDesc>> extractTableMeta = tableService.extractTableMeta(tables, project,
                sourceType);
        Set<String> loaded = Sets.newLinkedHashSet();
        Set<String> running = Sets.newLinkedHashSet();//for table sample
        Set<String> failed = Sets.newLinkedHashSet();
        for (Pair<TableDesc, TableExtDesc> pair : extractTableMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();
            String tableName = tableDesc.getIdentity();
            boolean ok;
            try {
                loadTable(tableDesc, extDesc, project);
                ok = true;
            } catch (Exception ex) {
                logger.error("Failed to load table '" + tableName + "'\"", ex);
                ok = false;
            }
            (ok ? loaded : failed).add(tableName);
        }
        return new Set[] { loaded, running, failed };
    }

    /**
     * Load given table to project
     * 
     * @throws IOException on error
     */
    public void loadTable(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {

        String[] loaded = tableService.loadTableToProject(tableDesc, extDesc, project);
        // sanity check when loaded is empty or loaded table is not the table
        String tableName = tableDesc.getIdentity();
        if (loaded.length == 0 || !loaded[0].equals(tableName))
            throw new IllegalStateException();
        ProjectInstance projectInstance = NProjectManager.getInstance(getConfig()).getProject(project);
        // add the new table to blacklist if the project don't grant internal permission by default
        if (!KapConfig.wrap(projectInstance.getConfig()).isProjectInternalDefaultPermissionGranted()) {
            try {
                blockAllUsersAccessToTable(project, tableName);
            } catch (Exception ex) {
                try {
                    unloadTable(tableName, project);
                } catch (Exception ex2) {
                    logger.error(
                            "Failed to unload table '" + tableName + "' after failing to blockAllUsersAccessToTable",
                            ex2);
                }
            }
        }
    }

    public void blockAllUsersAccessToTable(String project, String table) throws IOException {
        String prjUuid = projectService.getProjectManager().getProject(project).getUuid();

        AclEntity ae = accessService.getAclEntity(AclEntityType.PROJECT_INSTANCE, prjUuid);
        Acl acl = accessService.getAcl(ae);

        // NOTE the table ACL is a blacklist
        for (AccessControlEntry ace : acl.getEntries()) {
            Sid sid = ace.getSid();
            String name;
            String type;

            if (sid instanceof PrincipalSid) {
                name = ((PrincipalSid) sid).getPrincipal();
                type = "user";
            } else {
                name = ((GrantedAuthoritySid) sid).getGrantedAuthority();
                type = "group";
            }

            blockUserAccessToTable(project, name, table, type);
        }
    }

    private void blockUserAccessToTable(String project, String name, String table, String type) throws IOException {
        TableACL tableACL = getTableACLManager().getTableACLByCache(project);
        if (!tableACL.getNoAccessList(table, type).contains(name)) {
            tableACLService.addToTableACL(project, name, table, type);
        }

        // delete column acl
        if (columnAclService.exists(project, name, type)) {
            columnAclService.deleteFromColumnACL(project, name, table, type);
        }

        // delete row acl
        if (rowAclService.getRowACLIdentifiersByTable(project, type, table).contains(name)) {
            rowAclService.deleteFromRowACL(project, name, table, type);
        }
    }

    private void unloadTable(String tableName, String project) throws IOException {

        // drop the TableDesc FIRST!
        boolean ok = tableService.unloadTable(tableName, project);
        if (!ok)
            throw new IOException(
                    "Unload table '" + tableName + "' in project '" + project + "' falied, cannot find the objects?");

        // drop the TableExtDesc
        getTableManager(project).removeTableExt(tableName);

        // clean up ACL
        ACLOperationUtil.delLowLevelACLByTbl(project, tableName);
    }

}
