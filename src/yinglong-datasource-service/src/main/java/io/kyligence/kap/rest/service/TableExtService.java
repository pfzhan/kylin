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

import static org.apache.kylin.common.exception.ServerErrorCode.PROJECT_NOT_EXIST;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.delegate.TableMetadataContract;
import io.kyligence.kap.rest.request.S3TableExtInfo;
import io.kyligence.kap.rest.request.UpdateAWSTableExtDescRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.UpdateAWSTableExtDescResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import lombok.experimental.Delegate;

@Component("tableExtService")
public class TableExtService extends BasicService implements TableMetadataContract {
    private static final Logger logger = LoggerFactory.getLogger(TableExtService.class);

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Delegate
    private final TableMetadataBaseService tableMetadataBaseServer = new TableMetadataBaseService();

    @Autowired
    private ProjectService projectService;

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
    public LoadTableResponse loadDbTables(String[] dbTables, String project, boolean isDb) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        Map<String, Set<String>> dbTableMap = classifyDbTables(dbTables, isDb);
        Set<String> existDbs = Sets.newHashSet(tableService.getSourceDbNames(project));
        LoadTableResponse tableResponse = new LoadTableResponse();
        List<Pair<TableDesc, TableExtDesc>> loadTables = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : dbTableMap.entrySet()) {
            String db = entry.getKey();
            Set<String> tableSet = entry.getValue();
            if (!existDbs.contains(db)) {
                if (isDb) {
                    tableResponse.getFailed().add(db);
                } else {
                    List<String> tables = tableSet.stream().map(table -> db + "." + table).collect(Collectors.toList());
                    tableResponse.getFailed().addAll(tables);
                }
                continue;
            }
            Set<String> existTables = Sets.newHashSet(tableService.getSourceTableNames(project, db, ""));
            Set<String> failTables = Sets.newHashSet(tableSet);
            if (!isDb) {
                existTables.retainAll(tableSet);
                failTables.removeAll(existTables);
                List<String> tables = failTables.stream().map(table -> db + "." + table).collect(Collectors.toList());
                tableResponse.getFailed().addAll(tables);
            }

            String[] tables = existTables.stream().map(table -> db + "." + table).toArray(String[]::new);
            if (tables.length > 0)
                loadTables.addAll(extractTableMeta(tables, project, tableResponse));
        }
        if (!loadTables.isEmpty()) {
            return innerLoadTables(project, tableResponse, loadTables);
        }

        return tableResponse;
    }

    public LoadTableResponse loadAWSTablesCompatibleCrossAccount(List<S3TableExtInfo> s3TableExtInfoList, String project) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> dbTableList = new ArrayList<>();
        Map<String, S3TableExtInfo> map = new HashMap<>();
        for (S3TableExtInfo s3TableExtInfo : s3TableExtInfoList) {
            dbTableList.add(s3TableExtInfo.getName());
            map.put(s3TableExtInfo.getName(), s3TableExtInfo);
        }
        String[] dbTables = dbTableList.toArray(new String[0]);
        Map<String, Set<String>> dbTableMap = classifyDbTables(dbTables, false);
        Set<String> existDbs = Sets.newHashSet(tableService.getSourceDbNames(project));
        LoadTableResponse tableResponse = new LoadTableResponse();
        List<Pair<TableDesc, TableExtDesc>> loadTables = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : dbTableMap.entrySet()) {
            String db = entry.getKey();
            Set<String> tableSet = entry.getValue();
            if (!existDbs.contains(db)) {
                List<String> tables = tableSet.stream().map(table -> db + "." + table).collect(Collectors.toList());
                tableResponse.getFailed().addAll(tables);
                continue;
            }
            Set<String> existTables = Sets.newHashSet(tableService.getSourceTableNames(project, db, ""));
            Set<String> failTables = Sets.newHashSet(tableSet);
            existTables.retainAll(tableSet);
            failTables.removeAll(existTables);
            List<String> tmpTables = failTables.stream().map(table -> db + "." + table).collect(Collectors.toList());
            tableResponse.getFailed().addAll(tmpTables);

            String[] tables = existTables.stream().map(table -> db + "." + table).toArray(String[]::new);
            if (tables.length > 0) {
                List<Pair<TableDesc, TableExtDesc>> tableDescs = extractTableMeta(tables, project, tableResponse);
                for (Pair<TableDesc, TableExtDesc> tableExtDescPair : tableDescs) {
                    TableDesc tableDesc = tableExtDescPair.getFirst();
                    TableExtDesc tableExtDesc = tableExtDescPair.getSecond();
                    String tableKey = tableDesc.getDatabase() + "." + tableDesc.getName();
                    S3TableExtInfo target = map.get(tableKey);
                    if (null != target) {
                        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, target.getRoleArn());
                        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, target.getEndpoint());
                        loadTables.add(tableExtDescPair);
                    } else {
                        tableResponse.getFailed().add(tableKey);
                    }
                }
            }
        }
        if (!loadTables.isEmpty()) {
            return innerLoadTables(project, tableResponse, loadTables);
        }

        return tableResponse;
    }

    public UpdateAWSTableExtDescResponse updateAWSLoadedTableExtProp(UpdateAWSTableExtDescRequest request) {
        aclEvaluate.checkProjectOperationPermission(request.getProject());
        UpdateAWSTableExtDescResponse response = new UpdateAWSTableExtDescResponse();
        Map<String, S3TableExtInfo> map = new HashMap<>();
        for (S3TableExtInfo s3TableExtInfo : request.getTables()) {
            map.put(s3TableExtInfo.getName(), s3TableExtInfo);
        }
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, request.getProject());
        List<TableDesc> projectTableDescList = tableMetadataManager.listAllTables();
        List<String> identityList = projectTableDescList.stream()
                .map(TableDesc::getIdentity)
                .filter(identity -> map.get(identity) != null)
                .collect(Collectors.toList());

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager innerTableMetadataManager = getManager(NTableMetadataManager.class, request.getProject());
            for (String identity : identityList) {
                TableDesc tableDesc = innerTableMetadataManager.getTableDesc(identity);
                TableExtDesc extDesc = innerTableMetadataManager.getTableExtIfExists(tableDesc);
                if (null == extDesc) {
                    response.getFailed().add(identity);
                } else {
                    TableExtDesc copyExt = innerTableMetadataManager.copyForWrite(extDesc);
                    S3TableExtInfo s3TableExtInfo = map.get(identity);
                    copyExt.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, s3TableExtInfo.getRoleArn());
                    copyExt.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, s3TableExtInfo.getEndpoint());
                    innerTableMetadataManager.saveTableExt(copyExt);
                    tableService.refreshSparkSessionIfNecessary(copyExt);
                    response.getSucceed().add(identity);
                }
            }
            return response;
        }, request.getProject());
    }

    private LoadTableResponse innerLoadTables(String project, LoadTableResponse tableResponse,
            List<Pair<TableDesc, TableExtDesc>> loadTables) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> { //
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.readSystemKylinConfig(),
                    project);
            loadTables.forEach(pair -> {
                String tableName = pair.getFirst().getIdentity();
                boolean success = true;
                if (tableManager.getTableDesc(tableName) == null) {
                    try {
                        loadTable(pair.getFirst(), pair.getSecond(), project);
                    } catch (Exception ex) {
                        logger.error("Failed to load table ({}/{})", project, tableName, ex);
                        success = false;
                    }
                }
                Set<String> targetSet = success ? tableResponse.getLoaded() : tableResponse.getFailed();
                targetSet.add(tableName);
            });
            return tableResponse;
        }, project, 1);
    }

    private List<Pair<TableDesc, TableExtDesc>> extractTableMeta(String[] tables, String project,
            LoadTableResponse tableResponse) throws IOException, InterruptedException {
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);
        return ugi.doAs((PrivilegedExceptionAction<List<Pair<TableDesc, TableExtDesc>>>) () -> {
            ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);
            List<Pair<TableDesc, TableExtDesc>> extractTableMetas = tableService.extractTableMeta(tables, project);
            if (projectInstance.isProjectKerberosEnabled()) {
                return extractTableMetas.stream().map(pair -> {
                    TableDesc tableDesc = pair.getFirst();
                    String tableName = tableDesc.getIdentity();
                    ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance)
                            .getSourceMetadataExplorer();
                    if (!explr.checkTablesAccess(Sets.newHashSet(tableName))) {
                        tableResponse.getFailed().add(tableName);
                        return null;
                    }
                    return pair;
                }).filter(Objects::nonNull).collect(Collectors.toList());
            }
            return extractTableMetas;
        });
    }

    private Map<String, Set<String>> classifyDbTables(String[] dbTables, boolean isDb) {
        Map<String, Set<String>> dbTableMap = Maps.newHashMap();
        for (String str : dbTables) {
            String db;
            String table = null;
            if (isDb) {
                db = str.toUpperCase(Locale.ROOT);
            } else {
                String[] dbTableName = HadoopUtil.parseHiveTableName(str);
                db = dbTableName[0].toUpperCase(Locale.ROOT);
                table = dbTableName[1].toUpperCase(Locale.ROOT);
            }
            Set<String> tables = dbTableMap.getOrDefault(db, Sets.newHashSet());
            if (table != null) {
                tables.add(table);
            }
            dbTableMap.put(db, tables);
        }
        return dbTableMap;
    }

    /**
     * Load given table to project
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
    public void checkAndLoadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        loadTable(tableDesc, extDesc, project);
    }

    private void checkBeforeLoadTable(TableDesc tableDesc, String project) {
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
        TableDesc originTableDesc = tableMetadataManager.getTableDesc(tableDesc.getIdentity());
        if (originTableDesc != null && (originTableDesc.getSourceType() == ISourceAware.ID_STREAMING
                || tableDesc.getSourceType() == ISourceAware.ID_STREAMING)) {
            throw new KylinException(PROJECT_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSameTableNameExist(), tableDesc.getIdentity()));
        }
    }

    @Override
    public List<String> getTableNamesByFuzzyKey(String project, String fuzzyKey) {
        if (StringUtils.isNotEmpty(project)) {
            NTableMetadataManager nTableMetadataManager = getManager(NTableMetadataManager.class, project);
            return nTableMetadataManager.getTableNamesByFuzzyKey(fuzzyKey);
        }
        List<String> tableNames = new ArrayList<>();
        // query from all projects
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(null, false);
        for (ProjectInstance projectInstance : projectInstances) {
            NTableMetadataManager nTableMetadataManager = getManager(NTableMetadataManager.class, projectInstance.getName());
            tableNames.addAll(nTableMetadataManager.getTableNamesByFuzzyKey(fuzzyKey));
        }
        return tableNames;
    }
}
