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

import static org.apache.kylin.common.exception.ServerErrorCode.DATABASE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_MANAGEMENT_NOT_ENABLED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NSparkSnapshotJob;
import io.kyligence.kap.metadata.acl.AclTCRDigest;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.SnapshotCheckResponse;
import io.kyligence.kap.rest.response.SnapshotResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("snapshotService")
public class SnapshotService extends BasicService {

    private static final String ONLINE = "ONLINE";
    private static final String REFRESHING = "REFRESHING";
    private static final String LOADING = "LOADING";
    private static final String OFFLINE = "OFFLINE";

    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    private TableService tableService;

    public JobInfoResponse buildSnapshots(String project, Set<String> buildDatabases,
            Set<String> needBuildSnapshotTables, boolean isRefresh, int priority) {
        if (buildDatabases.isEmpty()) {
            return buildSnapshots(project, needBuildSnapshotTables, isRefresh, priority);
        }

        NTableMetadataManager tableManager = getTableManager(project);
        val databases = buildDatabases.stream().map(String::toUpperCase).collect(Collectors.toSet());
        val databasesNotExist = databases.stream().filter(database -> !tableManager.listDatabases().contains(database))
                .collect(Collectors.toSet());
        if (!databasesNotExist.isEmpty()) {
            throw new KylinException(DATABASE_NOT_EXIST, String.format(MsgPicker.getMsg().getDATABASE_NOT_EXIST(),
                    StringUtils.join(databasesNotExist, ", ")));
        }
        Set<String> tablesOfDatabases = tableManager.listAllTables().stream()
                .filter(tableDesc -> databases.contains(tableDesc.getDatabase())).map(TableDesc::getIdentity)
                .collect(Collectors.toSet());
        needBuildSnapshotTables.addAll(tablesOfDatabases);

        return buildSnapshots(project, needBuildSnapshotTables, isRefresh, priority);
    }

    public JobInfoResponse buildSnapshots(String project, Set<String> needBuildSnapshotTables, boolean isRefresh, int priority) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = getTableDescs(project, needBuildSnapshotTables);
        checkTablePermission(tables);
        if (isRefresh) {
            checkTableSnapshotExist(project, getTableDescs(project, needBuildSnapshotTables));
        }

        List<String> jobIds = new ArrayList<>();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            checkRunningSnapshotTask(project, needBuildSnapshotTables);
            getSourceUsageManager().licenseCheckWrap(project, () -> {
                for (TableDesc tableDesc : tables) {
                    NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
                    JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);
                    jobStatisticsManager.updateStatistics(TimeUtil.getDayStart(System.currentTimeMillis()), 0, 0, 1);
                    NSparkSnapshotJob job = NSparkSnapshotJob.create(tableDesc, project, getUsername(), isRefresh);
                    ExecutablePO po = NExecutableManager.toPO(job, project);
                    po.setPriority(priority);
                    execMgr.addJob(po);
                    jobIds.add(job.getId());
                }
                return null;
            });
            return null;
        }, project);
        String jobName = isRefresh ? SNAPSHOT_REFRESH.toString() : SNAPSHOT_BUILD.toString();
        return JobInfoResponse.of(jobIds, jobName);
    }

    private void checkTablePermission(Set<TableDesc> tables) {
        List<TableDesc> nonPermittedTables = tables.stream().filter(tableDesc -> !isAuthorizedTableAndColumn(tableDesc))
                .collect(Collectors.toList());
        if (!nonPermittedTables.isEmpty()) {
            List<String> tableIdentities = nonPermittedTables.stream().map(TableDesc::getIdentity)
                    .collect(Collectors.toList());
            throw new KylinException(PERMISSION_DENIED,
                    String.format(MsgPicker.getMsg().getSNAPSHOT_OPERATION_PERMISSION_DENIED(),
                            StringUtils.join(tableIdentities, "', '")));
        }

    }

    @Transaction(project = 0)
    public SnapshotCheckResponse deleteSnapshots(String project, Set<String> tableNames) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = getTableDescs(project, tableNames);
        checkTablePermission(tables);
        checkTableSnapshotExist(project, tables);

        List<String> needDeleteTables = tables.stream().map(TableDesc::getIdentity).collect(Collectors.toList());

        NTableMetadataManager tableManager = getTableManager(project);
        NExecutableManager execManager = getExecutableManager(project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);

        List<AbstractExecutable> conflictJobs = executables.stream()
                .filter(exec -> needDeleteTables.contains(exec.getParam(NBatchConstants.P_TABLE_NAME)))
                .collect(Collectors.toList());

        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> {
            execManager.discardJob(job.getId());
            updateSnapcheckResponse(job, response);
        });
        tableNames.forEach(tableName -> {
            TableDesc src = tableManager.getTableDesc(tableName);
            TableDesc copy = tableManager.copyForWrite(src);
            copy.setLastSnapshotPath(null);

            TableExtDesc ext = tableManager.getOrCreateTableExt(src);
            TableExtDesc extCopy = tableManager.copyForWrite(ext);
            extCopy.setOriginalSize(-1);

            tableManager.mergeAndUpdateTableExt(ext, extCopy);
            tableManager.updateTableDesc(copy);
        });
        return response;
    }

    public SnapshotCheckResponse checkBeforeDeleteSnapshots(String project, Set<String> tableNames) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = getTableDescs(project, tableNames);
        checkTablePermission(tables);
        checkTableSnapshotExist(project, tables);

        List<String> needDeleteTables = tables.stream().map(TableDesc::getIdentity).collect(Collectors.toList());

        NExecutableManager execManager = getExecutableManager(project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);

        List<AbstractExecutable> conflictJobs = executables.stream()
                .filter(exec -> needDeleteTables.contains(exec.getParam(NBatchConstants.P_TABLE_NAME)))
                .collect(Collectors.toList());

        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> updateSnapcheckResponse(job, response));
        return response;
    }

    private void updateSnapcheckResponse(AbstractExecutable job, SnapshotCheckResponse response) {
        String tableIdentity = job.getTargetSubject();
        String[] tableSplit = tableIdentity.split("\\.");
        String database = "";
        String table = tableIdentity;
        if (tableSplit.length >= 2) {
            database = tableSplit[0];
            table = tableSplit[1];
        }
        response.addAffectedJobs(job.getId(), database, table);
    }

    private void checkTableSnapshotExist(String project, Set<TableDesc> tables) {
        NExecutableManager execManager = getExecutableManager(project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);
        List<String> tablesWithEmptySnapshot = tables.stream()
                .filter(tableDesc -> !hasSnapshotOrRunningJob(tableDesc, executables)).map(TableDesc::getIdentity)
                .collect(Collectors.toList());
        if (!tablesWithEmptySnapshot.isEmpty()) {
            throw new KylinException(SNAPSHOT_NOT_EXIST, String.format(MsgPicker.getMsg().getSNAPSHOT_NOT_FOUND(),
                    StringUtils.join(tablesWithEmptySnapshot, "', '")));
        }
    }

    private void checkSnapshotManualManagement(String project) {
        if (!getProjectManager().getProject(project).getConfig().isSnapshotManualManagementEnabled()) {
            throw new KylinException(SNAPSHOT_MANAGEMENT_NOT_ENABLED,
                    MsgPicker.getMsg().getSNAPSHOT_MANAGEMENT_NOT_ENABLED());
        }
    }

    private void checkRunningSnapshotTask(String project, Set<String> needBuildSnapshotTables) {
        //check whether snapshot task is running on current project
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        Set<String> runningTables = new HashSet<>();
        for (AbstractExecutable executable : executables) {
            if (needBuildSnapshotTables.contains(executable.getParam(NBatchConstants.P_TABLE_NAME))) {
                runningTables.add(executable.getParam(NBatchConstants.P_TABLE_NAME));
            }
        }

        if (!runningTables.isEmpty()) {
            JobSubmissionException jobSubmissionException = new JobSubmissionException(
                    MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
            runningTables.forEach(tableName -> jobSubmissionException.addJobFailInfo(tableName,
                    new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_FAIL())));
            throw jobSubmissionException;
        }

    }

    private Set<TableDesc> getTableDescs(String project, Set<String> needBuildSnapshotTables) {
        Preconditions.checkNotNull(needBuildSnapshotTables);
        Preconditions.checkArgument(!needBuildSnapshotTables.isEmpty());
        NTableMetadataManager tableManager = getTableManager(project);
        Set<TableDesc> tables = new HashSet<>();
        Set<String> notFoundTables = new HashSet<>();
        for (String tableName : needBuildSnapshotTables) {
            TableDesc tableDesc = tableManager.getTableDesc(tableName);
            if (tableDesc != null) {
                tables.add(tableDesc);
            } else {
                notFoundTables.add(tableName);
            }
        }
        if (!notFoundTables.isEmpty()) {
            throw new KylinException(TABLE_NOT_EXIST,
                    String.format(MsgPicker.getMsg().getTABLE_NOT_FOUND(), StringUtils.join(notFoundTables, "', '")));
        }
        return tables;
    }

    public List<SnapshotResponse> getProjectSnapshots(String project, String table, Set<String> statusFilter,
            String sortBy, boolean isReversed) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
        if (table == null)
            table = "";

        String database = null;
        if (table.contains(".")) {
            database = table.split("\\.", 2)[0].trim();
            table = table.split("\\.", 2)[1].trim();
        }

        final String finalTable = table;
        final String finalDatabase = database;
        List<TableDesc> tables = nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
            if (StringUtils.isEmpty(finalDatabase)) {
                return true;
            }
            return tableDesc.getDatabase().equalsIgnoreCase(finalDatabase);
        }).filter(tableDesc -> {
            if (StringUtils.isEmpty(finalTable)) {
                return true;
            }
            if (finalDatabase == null && tableDesc.getDatabase().toLowerCase().contains(finalTable.toLowerCase())) {
                return true;
            }
            return tableDesc.getName().toLowerCase().contains(finalTable.toLowerCase());
        }).filter(this::isAuthorizedTable).filter(tableDesc -> hasSnapshotOrRunningJob(tableDesc, executables))
                .collect(Collectors.toList());

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<SnapshotResponse> response = new ArrayList<>();
        tables.stream().forEach(tableDesc -> {
            Pair<Integer, Integer> countPair = getModelCount(tableDesc);
            response.add(new SnapshotResponse(tableDesc,
                    tableService.getSnapshotSize(project, tableDesc.getIdentity(), fs), countPair.getFirst(),
                    countPair.getSecond(), tableService.getSnapshotModificationTime(tableDesc),
                    getSnapshotJobStatus(tableDesc, executables), getForbiddenColumns(tableDesc)));
        });

        if (!statusFilter.isEmpty()) {
            val upperCaseFilter = statusFilter.stream().map(String::toUpperCase).collect(Collectors.toSet());
            response.removeIf(res -> !upperCaseFilter.contains(res.getStatus()));
        }

        sortBy = StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy;
        if ("last_modified_time".equalsIgnoreCase(sortBy) && isReversed) {
            response.sort(SnapshotResponse::compareTo);
        } else {
            Comparator<SnapshotResponse> comparator = propertyComparator(sortBy, !isReversed);
            response.sort(comparator);
        }

        return response;
    }

    private Pair<Integer, Integer> getModelCount(TableDesc tableDesc) {
        int factCount = 0;
        int lookupCount = 0;
        val manager = NDataModelManager.getInstance(getConfig(), tableDesc.getProject());
        for (val model : manager.listAllModels()) {
            if (model.isBroken()) {
                continue;
            }
            if (model.isRootFactTable(tableDesc)) {
                factCount++;
            } else if (model.isLookupTable(tableDesc)) {
                lookupCount++;
            }
        }
        return new Pair<>(factCount, lookupCount);
    }

    private Set<String> getForbiddenColumns(TableDesc tableDesc) {
        String project = tableDesc.getProject();
        Set<String> forbiddenColumns = Sets.newHashSet();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, false)) {
            return forbiddenColumns;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, username, true);
        Set<String> allColumns = userAuth.getColumns();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allColumns.addAll(groupAuth.getColumns());
        }

        forbiddenColumns = Sets.newHashSet(tableDesc.getColumns()).stream()
                .map(columnDesc -> columnDesc.getTable().getIdentity() + "." + columnDesc.getName())
                .collect(Collectors.toSet());

        forbiddenColumns.removeAll(allColumns);
        return forbiddenColumns;
    }

    private boolean hasSnapshotOrRunningJob(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath()) || hasRunningJob(tableDesc, executables);
    }

    private String getSnapshotJobStatus(TableDesc tableDesc, List<AbstractExecutable> executables) {
        boolean hasSnapshot = StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath());
        boolean hasJob = hasRunningJob(tableDesc, executables);
        if (hasSnapshot) {
            if (hasJob) {
                return REFRESHING;
            } else {
                return ONLINE;
            }
        } else {
            if (hasJob) {
                return LOADING;
            } else {
                return OFFLINE;
            }
        }
    }

    private boolean hasRunningJob(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return executables.stream().map(executable -> executable.getParam(NBatchConstants.P_TABLE_NAME))
                .collect(Collectors.toList()).contains(tableDesc.getIdentity());
    }

    private boolean isAuthorizedTableAndColumn(TableDesc originTable) {
        String project = originTable.getProject();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, false)) {
            return true;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, username, true);
        Set<String> allTables = userAuth.getTables();
        Set<String> allColumns = userAuth.getColumns();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allTables.addAll(groupAuth.getTables());
            allColumns.addAll(groupAuth.getColumns());
        }

        if (!allTables.contains(originTable.getIdentity())) {
            return false;
        }

        return allColumns.containsAll(Lists.newArrayList(originTable.getColumns()).stream()
                .map(columnDesc -> columnDesc.getTable().getIdentity() + "." + columnDesc.getName())
                .collect(Collectors.toList()));
    }

    private boolean isAuthorizedTable(TableDesc originTable) {
        String project = originTable.getProject();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, false)) {
            return true;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, username, true);
        Set<String> allTables = userAuth.getTables();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getAclTCRManager(project).getAuthTablesAndColumns(project, group, false);
            allTables.addAll(groupAuth.getTables());
        }

        return allTables.contains(originTable.getIdentity());
    }

    private boolean matchTablePattern(TableDesc tableDesc, String tablePattern, String databasePattern,
            String databaseTarget) {
        if (StringUtils.isEmpty(tablePattern)) {
            return true;
        }

        if (StringUtils.isEmpty(databasePattern) && databaseTarget.toLowerCase().contains(tablePattern.toLowerCase())) {
            return true;
        }

        return tableDesc.getName().toLowerCase().contains(tablePattern.toLowerCase());
    }

    public NInitTablesResponse getTables(String project, String tablePattern, int offset, int limit) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        NInitTablesResponse response = new NInitTablesResponse();
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
        Set<String> databases = nTableMetadataManager.listDatabases();

        String expectedDatabase = null;
        if (tablePattern.contains(".")) {
            expectedDatabase = tablePattern.split("\\.", 2)[0].trim();
            tablePattern = tablePattern.split("\\.", 2)[1].trim();
        }

        final String finalTable = tablePattern;
        final String finalDatabase = expectedDatabase;
        for (String database : databases) {
            if (expectedDatabase != null && !expectedDatabase.equalsIgnoreCase(database)) {
                continue;
            }
            List<TableDesc> tables = nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
                if (StringUtils.isEmpty(database)) {
                    return true;
                }
                return tableDesc.getDatabase().equalsIgnoreCase(database);
            }).filter(tableDesc -> matchTablePattern(tableDesc, finalTable, finalDatabase, database))
                    .filter(this::isAuthorizedTableAndColumn).sorted(tableService::compareTableDesc)
                    .collect(Collectors.toList());

            int tableSize = tables.size();
            List<TableDesc> tablePage = PagingUtil.cutPage(tables, offset, limit);

            if (!tablePage.isEmpty()) {
                List<TableNameResponse> tableResponse = tablePage.stream().map(tableDesc -> {
                    val resp = new TableNameResponse();
                    resp.setTableName(tableDesc.getName());
                    resp.setLoaded(hasSnapshotOrRunningJob(tableDesc, executables));
                    return resp;
                }).collect(Collectors.toList());

                response.putDatabase(database, tableSize, tableResponse);
            }
        }

        return response;
    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, String tablePattern) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager tableManager = getTableManager(project);
        if (tablePattern == null) {
            tablePattern = "";
        }

        List<TableNameResponse> tableNameResponses = new ArrayList<>();
        if (tablePattern.contains(".")) {
            String databasePattern = tablePattern.split("\\.", 2)[0].trim();
            if (!databasePattern.equalsIgnoreCase(database)) {
                return tableNameResponses;
            }
            tablePattern = tablePattern.split("\\.", 2)[1].trim();
        }

        final String finalTable = tablePattern;
        List<TableDesc> tables = tableManager.listAllTables().stream()
                .filter(tableDesc -> tableDesc.getDatabase().equalsIgnoreCase(database)).filter(tableDesc -> {
                    if (StringUtils.isEmpty(finalTable)) {
                        return true;
                    }
                    return tableDesc.getName().toLowerCase().contains(finalTable.toLowerCase());
                }).filter(this::isAuthorizedTableAndColumn).sorted(tableService::compareTableDesc)
                .collect(Collectors.toList());
        for (TableDesc tableDesc : tables) {
            TableNameResponse tableNameResponse = new TableNameResponse();
            tableNameResponse.setTableName(tableDesc.getName());
            tableNameResponse.setLoaded(hasSnapshotOrRunningJob(tableDesc, executables));
            tableNameResponses.add(tableNameResponse);
        }
        return tableNameResponses;
    }
}
