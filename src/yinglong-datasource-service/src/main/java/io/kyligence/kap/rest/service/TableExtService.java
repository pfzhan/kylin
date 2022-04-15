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
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import io.kyligence.kap.rest.aspect.Transaction;

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;

@Component("tableExtService")
public class TableExtService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(TableExtService.class);

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private AclEvaluate aclEvaluate;

    /**
     * Load a group of  tables
     *
     * @return an array of table name sets:
     * [0] : tables that loaded successfully
     * [1] : tables that didn't load due to running sample job todo
     * [2] : tables that didn't load due to other error
     * @throws Exception if reading hive metadata error
     */
    @Transaction(project = 1, retry = 1)
    public LoadTableResponse loadTables(String[] tables, String project) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);

        return ugi.doAs(new PrivilegedExceptionAction<LoadTableResponse>() {
            @Override
            public LoadTableResponse run() throws Exception {
                ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);

                List<Pair<TableDesc, TableExtDesc>> extractTableMeta = tableService.extractTableMeta(tables, project);
                LoadTableResponse tableResponse = new LoadTableResponse();
                Set<String> loaded = Sets.newLinkedHashSet();
                Set<String> failed = Sets.newLinkedHashSet();
                for (Pair<TableDesc, TableExtDesc> pair : extractTableMeta) {
                    TableDesc tableDesc = pair.getFirst();
                    TableExtDesc extDesc = pair.getSecond();
                    String tableName = tableDesc.getIdentity();
                    boolean ok;
                    if (projectInstance.isProjectKerberosEnabled()) {
                        ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance)
                                .getSourceMetadataExplorer();
                        if (!explr.checkTablesAccess(Sets.newHashSet(pair.getFirst().getIdentity()))) {
                            failed.add(tableName);
                            continue;
                        }
                    }
                    try {
                        loadTable(tableDesc, extDesc, project);
                        ok = true;
                    } catch (Exception ex) {
                        logger.error("Failed to load table '" + tableName + "'\"", ex);
                        ok = false;
                    }
                    (ok ? loaded : failed).add(tableName);
                }
                tableResponse.setLoaded(loaded);
                tableResponse.setFailed(failed);
                return tableResponse;
            }
        });
    }

    /**
     * Load given table to project
     *
     * @throws IOException on error
     */
    @Transaction(project = 2)
    public void loadTable(TableDesc tableDesc, TableExtDesc extDesc, String project) {
        checkBeforeLoadTable(tableDesc, project);
        String[] loaded = tableService.loadTableToProject(tableDesc, extDesc, project);
        // sanity check when loaded is empty or loaded table is not the table
        String tableName = tableDesc.getIdentity();
        if (loaded.length == 0 || !loaded[0].equals(tableName))
            throw new IllegalStateException();

    }

    @Transaction(project = 1)
    public void removeJobIdFromTableExt(String jobId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
        for (TableDesc desc : tableMetadataManager.listAllTables()) {
            TableExtDesc extDesc = tableMetadataManager.getTableExtIfExists(desc);
            if (extDesc == null) {
                continue;
            }
            extDesc = tableMetadataManager.copyForWrite(extDesc);
            if (extDesc.getJodID() != null && jobId.equals(extDesc.getJodID())) {
                extDesc.setJodID(null);
                tableMetadataManager.saveTableExt(extDesc);
            }
        }

    }

    @Transaction(project = 0, retry = 1)
    public LoadTableResponse loadTablesByDatabase(String project, final String[] databases) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        LoadTableResponse loadTableByDatabaseResponse = new LoadTableResponse();
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        for (final String database : databases) {
            List<String> tables = tableService.getSourceTableNames(project, database, "");
            List<String> identities = tables.stream().map(s -> database + "." + s)
                    .filter(t -> Objects.isNull(tableManager.getTableDesc(t))).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(identities)) {
                String[] tableToLoad = new String[identities.size()];
                LoadTableResponse loadTableResponse = loadTables(identities.toArray(tableToLoad), project);
                loadTableByDatabaseResponse.getLoaded().addAll(loadTableResponse.getLoaded());
                loadTableByDatabaseResponse.getFailed().addAll(loadTableResponse.getFailed());
            }
        }
        return loadTableByDatabaseResponse;
    }

    @Transaction(project = 0, retry = 1)
    public void checkAndLoadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        loadTable(tableDesc, extDesc, project);
    }

    private void checkBeforeLoadTable(TableDesc tableDesc, String project) {
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
        TableDesc originTableDesc = tableMetadataManager.getTableDesc(tableDesc.getIdentity());
        if (originTableDesc != null && (originTableDesc.getSourceType() == ISourceAware.ID_STREAMING
                || tableDesc.getSourceType() == ISourceAware.ID_STREAMING)) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSAME_TABLE_NAME_EXIST(), tableDesc.getIdentity()));
        }
    }
}
