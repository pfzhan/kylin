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

import static io.kyligence.kap.rest.service.SnapshotService.SnapshotStatus.BROKEN;
import static org.apache.kylin.common.exception.ServerErrorCode.COLUMN_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.DATABASE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_MANAGEMENT_NOT_ENABLED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NSparkSnapshotJob;
import io.kyligence.kap.metadata.acl.AclTCRDigest;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.request.SnapshotRequest;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.SnapshotCheckResponse;
import io.kyligence.kap.rest.response.SnapshotColResponse;
import io.kyligence.kap.rest.response.SnapshotInfoResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import lombok.val;

@Component("snapshotService")
public class SnapshotService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotService.class);

    public enum SnapshotStatus {
        ONLINE, REFRESHING, LOADING, OFFLINE, BROKEN
    }

    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    private TableService tableService;

    public JobInfoResponse buildSnapshots(String project, Set<String> buildDatabases,
            Set<String> needBuildSnapshotTables, Map<String, SnapshotRequest.TableOption> options, boolean isRefresh,
            int priority, String yarnQueue) {
        if (buildDatabases.isEmpty()) {
            return buildSnapshots(project, needBuildSnapshotTables, options, isRefresh, priority, yarnQueue);
        }

        NTableMetadataManager tableManager = getTableManager(project);
        val databases = buildDatabases.stream().map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        val databasesNotExist = databases.stream().filter(database -> !tableManager.listDatabases().contains(database))
                .collect(Collectors.toSet());
        if (!databasesNotExist.isEmpty()) {
            throw new KylinException(DATABASE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getDATABASE_NOT_EXIST(), StringUtils.join(databasesNotExist, ", ")));
        }
        Set<TableDesc> tablesOfDatabases = tableManager.listAllTables().stream()
                .filter(tableDesc -> databases.contains(tableDesc.getDatabase())).collect(Collectors.toSet());

        tablesOfDatabases = skipLoadedTable(tablesOfDatabases, project);
        tablesOfDatabases = tablesOfDatabases.stream().filter(this::isAuthorizedTableAndColumn)
                .collect(Collectors.toSet());

        needBuildSnapshotTables
                .addAll(tablesOfDatabases.stream().map(TableDesc::getIdentity).collect(Collectors.toSet()));

        return buildSnapshots(project, needBuildSnapshotTables, options, isRefresh, priority, yarnQueue);
    }

    private Set<TableDesc> skipLoadedTable(Set<TableDesc> tablesOfDatabases, String project) {
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        return tablesOfDatabases.stream().filter(table -> !hasLoadedSnapshot(table, executables))
                .collect(Collectors.toSet());
    }

    public JobInfoResponse buildSnapshots(String project, Set<String> needBuildSnapshotTables,
            Map<String, SnapshotRequest.TableOption> options, boolean isRefresh, int priority, String yarnQueue) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = checkAndGetTable(project, needBuildSnapshotTables);
        checkTablePermission(tables);
        if (isRefresh) {
            checkTableSnapshotExist(project, checkAndGetTable(project, needBuildSnapshotTables));
        }
        checkOptions(tables, options);
        Map<String, SnapshotRequest.TableOption> finalOptions = Maps.newHashMap();

        List<String> jobIds = new ArrayList<>();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            checkRunningSnapshotTask(project, needBuildSnapshotTables);
            JobManager.checkStorageQuota(project);
            getSourceUsageManager().licenseCheckWrap(project, () -> {
                for (TableDesc tableDesc : tables) {
                    NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
                    JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);
                    jobStatisticsManager.updateStatistics(TimeUtil.getDayStart(System.currentTimeMillis()), 0, 0, 1);

                    SnapshotRequest.TableOption option = decideBuildOption(tableDesc,
                            options.get(tableDesc.getIdentity()));
                    finalOptions.put(tableDesc.getIdentity(), option);

                    logger.info(
                            "create snapshot job with args, table: {}, selectedPartCol: {}, incrementBuild: {},isRefresh: {}",
                            tableDesc.getIdentity(), option.getPartitionCol(), option.isIncrementalBuild(), isRefresh);

                    NSparkSnapshotJob job = NSparkSnapshotJob.create(tableDesc, getUsername(), option.getPartitionCol(),
                            option.isIncrementalBuild(), isRefresh, yarnQueue);
                    ExecutablePO po = NExecutableManager.toPO(job, project);
                    po.setPriority(priority);
                    execMgr.addJob(po);

                    jobIds.add(job.getId());
                }
                return null;
            });

            NTableMetadataManager tableManager = getTableManager(project);
            for (TableDesc tableDesc : tables) {
                SnapshotRequest.TableOption option = finalOptions.get(tableDesc.getIdentity());
                if (tableDesc.isSnapshotHasBroken()
                        || !StringUtil.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                    TableDesc newTable = tableManager.copyForWrite(tableDesc);
                    newTable.setSnapshotHasBroken(false);
                    if (!StringUtil.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                        newTable.setSelectedSnapshotPartitionCol(option.getPartitionCol());
                    }
                    tableManager.updateTableDesc(newTable);
                }
            }
            return null;
        }, project);

        String jobName = isRefresh ? SNAPSHOT_REFRESH.toString() : SNAPSHOT_BUILD.toString();
        return JobInfoResponse.of(jobIds, jobName);
    }

    private void checkOptions(Set<TableDesc> tables, Map<String, SnapshotRequest.TableOption> options) {
        for (TableDesc table : tables) {
            SnapshotRequest.TableOption option = options.get(table.getIdentity());
            if (option != null) {
                String partCol = option.getPartitionCol();
                checkSupportBuildSnapShotByPartition(table);
                if (StringUtils.isNotEmpty(partCol) && table.findColumnByName(partCol) == null) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "table %s col %s not exist", table.getIdentity(), partCol));
                }
            }
        }
    }

    private SnapshotRequest.TableOption decideBuildOption(TableDesc tableDesc, SnapshotRequest.TableOption option) {

        boolean incrementalBuild = false;
        String selectedPartCol = null;

        if (option != null) {
            selectedPartCol = StringUtils.isEmpty(option.getPartitionCol()) ? null : option.getPartitionCol();
            incrementalBuild = option.isIncrementalBuild();
        } else {
            if (tableDesc.getLastSnapshotPath() != null) {
                selectedPartCol = tableDesc.getSelectedSnapshotPartitionCol();
                if (tableDesc.getSnapshotPartitionCol() != null) {
                    incrementalBuild = true;
                }
            }
        }
        if (!StringUtils.equals(selectedPartCol, tableDesc.getSnapshotPartitionCol())) {
            incrementalBuild = false;
        }
        return new SnapshotRequest.TableOption(selectedPartCol, incrementalBuild);
    }

    private void checkTablePermission(Set<TableDesc> tables) {
        List<TableDesc> nonPermittedTables = tables.stream().filter(tableDesc -> !isAuthorizedTableAndColumn(tableDesc))
                .collect(Collectors.toList());
        if (!nonPermittedTables.isEmpty()) {
            List<String> tableIdentities = nonPermittedTables.stream().map(TableDesc::getIdentity)
                    .collect(Collectors.toList());
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getSNAPSHOT_OPERATION_PERMISSION_DENIED());
        }

    }

    @Transaction(project = 0)
    public SnapshotCheckResponse deleteSnapshots(String project, Set<String> tableNames) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = checkAndGetTable(project, tableNames);
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
            copy.deleteSnapshot(false);

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
        Set<TableDesc> tables = checkAndGetTable(project, tableNames);
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
                .filter(tableDesc -> !hasLoadedSnapshot(tableDesc, executables)).map(TableDesc::getIdentity)
                .collect(Collectors.toList());
        if (!tablesWithEmptySnapshot.isEmpty()) {
            throw new KylinException(SNAPSHOT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSNAPSHOT_NOT_FOUND(), StringUtils.join(tablesWithEmptySnapshot, "', '")));
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

    private Set<TableDesc> checkAndGetTable(String project, Set<String> needBuildSnapshotTables) {
        Preconditions.checkNotNull(needBuildSnapshotTables);
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
            throw new KylinException(TABLE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getTABLE_NOT_FOUND(), StringUtils.join(notFoundTables, "', '")));
        }
        return tables;
    }

    public List<SnapshotInfoResponse> getProjectSnapshots(String project, String table,
            Set<SnapshotStatus> statusFilter, Set<Boolean> partitionFilter, String sortBy, boolean isReversed) {
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
            if (finalDatabase == null
                    && tableDesc.getDatabase().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT))) {
                return true;
            }
            return tableDesc.getName().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT));
        }).filter(this::isAuthorizedTable).filter(tableDesc -> hasLoadedSnapshot(tableDesc, executables))
                .collect(Collectors.toList());

        List<SnapshotInfoResponse> response = new ArrayList<>();
        tables.forEach(tableDesc -> {
            Pair<Integer, Integer> countPair = getModelCount(tableDesc);
            TableExtDesc tableExtDesc = nTableMetadataManager.getOrCreateTableExt(tableDesc);
            long totalRows = tableExtDesc.getTotalRows();
            response.add(new SnapshotInfoResponse(tableDesc, totalRows, countPair.getFirst(), countPair.getSecond(),
                    getSnapshotJobStatus(tableDesc, executables), getForbiddenColumns(tableDesc)));
        });

        if (!statusFilter.isEmpty()) {
            response.removeIf(res -> !statusFilter.contains(res.getStatus()));
        }
        if (partitionFilter.size() == 1) {
            boolean isPartition = partitionFilter.iterator().next();
            response.removeIf(res -> isPartition == (res.getSelectPartitionCol() == null));
        }

        sortBy = StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy;
        if ("last_modified_time".equalsIgnoreCase(sortBy) && isReversed) {
            response.sort(SnapshotInfoResponse::compareTo);
        } else {
            Comparator<SnapshotInfoResponse> comparator = propertyComparator(sortBy, !isReversed);
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
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
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

    private SnapshotStatus getSnapshotJobStatus(TableDesc tableDesc, List<AbstractExecutable> executables) {
        if (tableDesc.isSnapshotHasBroken()) {
            return BROKEN;
        }
        boolean hasSnapshot = StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath());
        boolean hasJob = hasRunningJob(tableDesc, executables);
        if (hasSnapshot) {
            if (hasJob) {
                return SnapshotStatus.REFRESHING;
            } else {
                return SnapshotStatus.ONLINE;
            }
        } else {
            if (hasJob) {
                return SnapshotStatus.LOADING;
            } else {
                return SnapshotStatus.OFFLINE;
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
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
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
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
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

        if (StringUtils.isEmpty(databasePattern)
                && databaseTarget.toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT))) {
            return true;
        }

        return tableDesc.getName().toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT));
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
                    resp.setLoaded(hasLoadedSnapshot(tableDesc, executables));
                    return resp;
                }).collect(Collectors.toList());

                response.putDatabase(database, tableSize, tableResponse);
            }
        }

        return response;
    }

    private boolean hasLoadedSnapshot(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return tableDesc.isSnapshotHasBroken() || hasSnapshotOrRunningJob(tableDesc, executables);
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
                    return tableDesc.getName().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT));
                }).filter(this::isAuthorizedTableAndColumn).sorted(tableService::compareTableDesc)
                .collect(Collectors.toList());
        for (TableDesc tableDesc : tables) {
            TableNameResponse tableNameResponse = new TableNameResponse();
            tableNameResponse.setTableName(tableDesc.getName());
            tableNameResponse.setLoaded(hasLoadedSnapshot(tableDesc, executables));
            tableNameResponses.add(tableNameResponse);
        }
        return tableNameResponses;
    }

    private void checkSupportBuildSnapShotByPartition(ISourceAware sourceAware) {
        ISource source = SourceFactory.getSource(sourceAware);
        if (!source.supportBuildSnapShotByPartition()) {
            throw new KylinException(INVALID_PARAMETER,
                    "not support build snapshot by partition column in current datasource");
        }
    }

    @Transaction(project = 0)
    public void configSnapshotPartitionCol(String project, Map<String, String> table2PartCol) {
        checkSnapshotManualManagement(project);
        checkSupportBuildSnapShotByPartition(getProjectManager().getProject(project));
        aclEvaluate.checkProjectOperationPermission(project);
        checkTableAndCol(project, table2PartCol);

        NTableMetadataManager tableManager = getTableManager(project);
        table2PartCol.forEach((tableName, colName) -> {
            TableDesc table = tableManager.copyForWrite(tableManager.getTableDesc(tableName));
            if (StringUtils.isEmpty(colName)) {
                colName = null;
            }
            colName = colName == null ? null : colName.toUpperCase(Locale.ROOT);
            table.setSelectedSnapshotPartitionCol(colName);
            tableManager.updateTableDesc(table);
        });

    }

    private void checkTableAndCol(String project, Map<String, String> table2PartCol) {
        if (table2PartCol.isEmpty()) {
            return;
        }
        Set<TableDesc> tables = checkAndGetTable(project, table2PartCol.keySet());
        checkTablePermission(tables);

        NTableMetadataManager tableManager = getTableManager(project);
        List<String> notFoundCols = Lists.newArrayList();
        table2PartCol.forEach((tableName, colName) -> {
            TableDesc table = tableManager.getTableDesc(tableName);
            if (StringUtils.isNotEmpty(colName) && table.findColumnByName(colName) == null) {
                notFoundCols.add(tableName + "." + colName);
            }
        });
        if (!notFoundCols.isEmpty()) {
            throw new KylinException(COLUMN_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getCOLUMN_NOT_EXIST(), StringUtils.join(notFoundCols, "', '")));
        }
    }

    public List<SnapshotColResponse> getSnapshotCol(String project, Set<String> tables, Set<String> databases,
            String tablePattern, boolean includeExistSnapshot) {
        return getSnapshotCol(project, tables, databases, tablePattern, includeExistSnapshot, true);
    }

    public List<SnapshotColResponse> getSnapshotCol(String project, Set<String> tables, Set<String> databases,
            String tablePattern, boolean includeExistSnapshot, boolean excludeBroken) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);

        Set<String> finalTables = Optional.ofNullable(tables).orElse(Sets.newHashSet());
        Set<String> finalDatabase = Optional.ofNullable(databases).orElse(Sets.newHashSet());
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        return getTableManager(project).listAllTables().stream().filter(table -> {
            if (finalDatabase.isEmpty() && finalTables.isEmpty()) {
                return true;
            }
            return finalTables.contains(table.getIdentity()) || finalDatabase.contains(table.getDatabase());
        }).filter(table -> {
            if (StringUtils.isEmpty(tablePattern)) {
                return true;
            }
            return table.getIdentity().toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT));
        }).filter(table -> includeExistSnapshot || !hasLoadedSnapshot(table, executables)
                || (!excludeBroken && table.isSnapshotHasBroken())).filter(this::isAuthorizedTableAndColumn)
                .map(SnapshotColResponse::from).collect(Collectors.toList());
    }

    public SnapshotColResponse reloadPartitionCol(String project, String table) throws Exception {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        TableDesc newTableDesc = tableService.extractTableMeta(Arrays.asList(table).toArray(new String[0]), project)
                .get(0).getFirst();
        newTableDesc.init(project);
        return SnapshotColResponse.from(newTableDesc);
    }
}
