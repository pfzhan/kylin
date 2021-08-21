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

import static org.apache.kylin.common.exception.ServerErrorCode.COLUMN_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.DUPLICATED_COLUMN_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_IMPORT_SSB_DATA;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_REFRESH_CATALOG_CACHE;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_COMPUTED_COLUMN_EXPRESSION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARTITION_COLUMN;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.ON_GOING_JOB_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.RELOAD_TABLE_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;
import static org.apache.kylin.query.exception.QueryErrorCode.EMPTY_TABLE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.function.LikeMatchers;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.exception.QueryErrorCode;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.TableRefresh;
import org.apache.kylin.rest.response.TableRefreshAll;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.Graphs;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NSegmentConfigHelper;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.metadata.model.exception.IllegalCCExpressionException;
import io.kyligence.kap.metadata.model.schema.AffectedModelContext;
import io.kyligence.kap.metadata.model.schema.ReloadTableContext;
import io.kyligence.kap.metadata.model.schema.SchemaNode;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.model.schema.SchemaUtil;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.rest.aspect.Transaction;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.constant.JobInfoEnum;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.BatchLoadTableResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.OpenPreReloadTableResponse;
import io.kyligence.kap.rest.response.PreReloadTableResponse;
import io.kyligence.kap.rest.response.PreUnloadTableResponse;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import io.kyligence.kap.rest.source.NHiveTableName;
import lombok.val;
import lombok.var;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);
    private static final String REFRESH_SINGLE_CATALOG_PATH = "/kylin/api/tables/single_catalog_cache";

    @Autowired
    private ModelService modelService;

    @Autowired
    private FusionModelService fusionModelService;

    @Autowired
    private ModelSemanticHelper semanticHelper;

    @Autowired
    private TableSamplingService tableSamplingService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("kafkaService")
    private KafkaService kafkaService;

    @Autowired
    @Qualifier("aclTCRService")
    private AclTCRService aclTCRService;

    @Autowired
    private ClusterManager clusterManager;

    @Autowired
    private LicenseInfoService licenseInfoService;

    public List<TableDesc> getTableDescByType(String project, boolean withExt, final String tableName,
            final String database, boolean isFuzzy, int sourceType) throws IOException {
        return getTableDesc(project, withExt, tableName, database, isFuzzy).stream()
                .filter(tableDesc -> sourceType == tableDesc.getSourceType()).collect(Collectors.toList());
    }

    public List<TableDesc> getTableDesc(String project, boolean withExt, final String tableName, final String database,
            boolean isFuzzy) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        List<TableDesc> tables = Lists.newArrayList();
        //get table not fuzzy,can use getTableDesc(tableName)
        if (StringUtils.isNotEmpty(tableName) && !isFuzzy) {
            val tableDesc = nTableMetadataManager.getTableDesc(database + "." + tableName);
            if (tableDesc != null)
                tables.add(tableDesc);
        } else {
            tables.addAll(nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
                if (StringUtils.isEmpty(database)) {
                    return true;
                }
                return tableDesc.getDatabase().equalsIgnoreCase(database);
            }).filter(tableDesc -> {
                if (StringUtils.isEmpty(tableName)) {
                    return true;
                }
                return tableDesc.getName().toLowerCase(Locale.ROOT).contains(tableName.toLowerCase(Locale.ROOT));
            }).sorted(this::compareTableDesc).collect(Collectors.toList()));
        }
        return getTablesResponse(tables, project, withExt);
    }

    public int compareTableDesc(TableDesc table1, TableDesc table2) {
        if (table1.isTop() == table2.isTop()) {
            if (table1.isIncrementLoading() == table2.isIncrementLoading()) {
                return table1.getName().compareToIgnoreCase(table2.getName());
            } else {
                return table1.isIncrementLoading() && !table2.isIncrementLoading() ? -1 : 1;
            }
        } else {
            return table1.isTop() && !table2.isTop() ? -1 : 1;
        }
    }

    @Transaction(project = 2)
    public String[] loadTableToProject(TableDesc tableDesc, TableExtDesc extDesc, String project) {
        return loadTablesToProject(Lists.newArrayList(Pair.newPair(tableDesc, extDesc)), project);
    }

    private String[] loadTablesToProject(List<Pair<TableDesc, TableExtDesc>> allMeta, String project) {
        final NTableMetadataManager tableMetaMgr = getTableManager(project);
        // save table meta
        List<String> saved = Lists.newArrayList();
        List<TableDesc> savedTables = Lists.newArrayList();
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();
            TableDesc origTable = tableMetaMgr.getTableDesc(tableDesc.getIdentity());
            val nTableDesc = new TableDesc(tableDesc);
            if (origTable == null || origTable.getProject() == null) {
                nTableDesc.setUuid(UUID.randomUUID().toString());
                nTableDesc.setLastModified(0);
            } else {
                nTableDesc.setUuid(origTable.getUuid());
                nTableDesc.setLastModified(origTable.getLastModified());
                nTableDesc.setIncrementLoading(origTable.isIncrementLoading());
            }

            tableMetaMgr.saveSourceTable(nTableDesc);
            if (extDesc != null) {
                TableExtDesc origExt = tableMetaMgr.getTableExtIfExists(tableDesc);
                TableExtDesc nTableExtDesc = new TableExtDesc(extDesc);
                if (origExt == null || origExt.getProject() == null) {
                    nTableExtDesc.setUuid(UUID.randomUUID().toString());
                    nTableExtDesc.setLastModified(0);
                } else {
                    nTableExtDesc.setUuid(origExt.getUuid());
                    nTableExtDesc.setLastModified(origExt.getLastModified());
                    nTableExtDesc.setMvcc(origExt.getMvcc());
                    nTableExtDesc.setOriginalSize(origExt.getOriginalSize());
                }
                val colNameMap = Stream.of(nTableDesc.getColumns())
                        .collect(Collectors.toMap(ColumnDesc::getName, col -> {
                            try {
                                return Integer.parseInt(col.getId());
                            } catch (NumberFormatException e) {
                                return Integer.MAX_VALUE;
                            }
                        }));
                nTableExtDesc.getAllColumnStats()
                        .sort(Comparator.comparing(stat -> colNameMap.getOrDefault(stat.getColumnName(), -1)));
                nTableExtDesc.init(project);

                tableMetaMgr.saveTableExt(nTableExtDesc);
            }

            saved.add(tableDesc.getIdentity());
            savedTables.add(tableDesc);
        }
        String[] result = saved.toArray(new String[saved.size()]);
        return result;
    }

    public List<Pair<TableDesc, TableExtDesc>> extractTableMeta(String[] tables, String project) {
        // de-dup
        SetMultimap<String, String> databaseTables = LinkedHashMultimap.create();
        for (String fullTableName : tables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName.toUpperCase(Locale.ROOT));
            Preconditions.checkArgument(!parts[1].isEmpty() && !parts[0].isEmpty(),
                    MsgPicker.getMsg().getTABLE_PARAM_EMPTY());
            databaseTables.put(parts[0], parts[1]);
        }
        // load all tables first Pair<TableDesc, TableExtDesc>
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance).getSourceMetadataExplorer();
        List<Pair<Map.Entry<String, String>, Object>> results = databaseTables.entries().parallelStream().map(entry -> {
            try {
                Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue(), project);
                TableDesc tableDesc = pair.getFirst();
                Preconditions.checkState(tableDesc.getDatabase().equalsIgnoreCase(entry.getKey()));
                Preconditions.checkState(tableDesc.getName().equalsIgnoreCase(entry.getValue()));
                Preconditions.checkState(tableDesc.getIdentity().equals(
                        entry.getKey().toUpperCase(Locale.ROOT) + "." + entry.getValue().toUpperCase(Locale.ROOT)));
                TableExtDesc extDesc = pair.getSecond();
                Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getIdentity()));
                return new Pair<Map.Entry<String, String>, Object>(entry, pair);
            } catch (Exception e) {
                return new Pair<Map.Entry<String, String>, Object>(entry, e);
            }
        }).collect(Collectors.toList());
        List<Pair<Map.Entry<String, String>, Object>> errorList = results.stream()
                .filter(pair -> pair.getSecond() instanceof Throwable).collect(Collectors.toList());
        if (!errorList.isEmpty()) {
            errorList.forEach(e -> logger.error(e.getFirst().getKey() + "." + e.getFirst().getValue(),
                    (Throwable) (e.getSecond())));
            String errorTables = StringUtils
                    .join(errorList.stream().map(error -> error.getFirst().getKey() + "." + error.getFirst().getValue())
                            .collect(Collectors.toList()), ",");
            String errorMessage = String.format(Locale.ROOT, MsgPicker.getMsg().getHIVETABLE_NOT_FOUND(), errorTables);
            throw new KylinException(TABLE_NOT_EXIST, errorMessage);
        }
        return results.stream().map(pair -> (Pair<TableDesc, TableExtDesc>) pair.getSecond())
                .collect(Collectors.toList());
    }

    public List<String> getSourceDbNames(String project) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listDatabases().stream().map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toList());
    }

    public List<String> getSourceTableNames(String project, String database, final String table) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listTables(database).stream().filter(s -> {
            if (StringUtils.isEmpty(table)) {
                return true;
            } else {
                return s.toLowerCase(Locale.ROOT).contains(table.toLowerCase(Locale.ROOT));
            }
        }).map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toList());

    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, final String table)
            throws Exception {
        aclEvaluate.checkProjectReadPermission(project);
        List<TableNameResponse> tableNameResponses = new ArrayList<>();
        NTableMetadataManager tableManager = getTableManager(project);
        List<String> tables = getSourceTableNames(project, database, table);
        for (String tableName : tables) {
            TableNameResponse tableNameResponse = new TableNameResponse();
            tableNameResponse.setTableName(tableName);
            String tableNameWithDB = String.format(Locale.ROOT, "%s.%s", database, tableName);
            checkTableExistOrLoad(tableNameResponse, tableManager.getTableDesc(tableNameWithDB));
            tableNameResponses.add(tableNameResponse);
        }
        return tableNameResponses;
    }

    private TableDescResponse getTableResponse(TableDesc table, String project) {
        TableDescResponse tableDescResponse = new TableDescResponse(table);
        TableExtDesc tableExtDesc = getTableManager(project).getTableExtIfExists(table);
        if (table.getKafkaConfig() != null) {
            tableDescResponse.setKafkaBootstrapServers(table.getKafkaConfig().getKafkaBootstrapServers());
            tableDescResponse.setSubscribe(table.getKafkaConfig().getSubscribe());
            tableDescResponse.setBatchTable(table.getKafkaConfig().getBatchTable());
        }

        if (tableExtDesc == null) {
            return tableDescResponse;
        }

        for (TableDescResponse.ColumnDescResponse colDescRes : tableDescResponse.getExtColumns()) {
            final TableExtDesc.ColumnStats columnStats = tableExtDesc.getColumnStatsByName(colDescRes.getName());
            if (columnStats != null) {
                colDescRes.setCardinality(columnStats.getCardinality());
                colDescRes.setMaxValue(columnStats.getMaxValue());
                colDescRes.setMinValue(columnStats.getMinValue());
                colDescRes.setNullCount(columnStats.getNullCount());
            }
        }
        tableDescResponse.setDescExd(tableExtDesc.getDataSourceProps());
        tableDescResponse.setCreateTime(tableExtDesc.getCreateTime());
        return tableDescResponse;
    }

    private List<TableDesc> getTablesResponse(List<TableDesc> tables, String project, boolean withExt)
            throws IOException {
        List<TableDesc> descs = new ArrayList<>();
        val projectManager = getProjectManager();
        val groups = getCurrentUserGroups();
        final List<AclTCR> aclTCRS = getAclTCRManager(project).getAclTCRs(AclPermissionUtil.getCurrentUsername(),
                groups);
        final boolean isAclGreen = AclPermissionUtil.canUseACLGreenChannel(project, groups, true);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<NDataModel> healthyModels = projectManager.listHealthyModels(project);
        for (val originTable : tables) {
            TableDesc table = getAuthorizedTableDesc(project, isAclGreen, originTable, aclTCRS);
            if (Objects.isNull(table)) {
                continue;
            }
            TableDescResponse tableDescResponse;
            val modelsUsingRootTable = Lists.<NDataModel> newArrayList();
            val modelsUsingTable = Lists.<NDataModel> newArrayList();
            for (NDataModel model : healthyModels) {
                if (model.containsTable(table)) {
                    modelsUsingTable.add(model);
                }

                if (model.isRootFactTable(table)) {
                    modelsUsingRootTable.add(model);
                }
            }

            if (withExt) {
                tableDescResponse = getTableResponse(table, project);
            } else {
                tableDescResponse = new TableDescResponse(table);
            }

            TableExtDesc tableExtDesc = getTableManager(project).getTableExtIfExists(table);
            if (tableExtDesc != null) {
                tableDescResponse.setTotalRecords(tableExtDesc.getTotalRows());
                tableDescResponse.setSamplingRows(tableExtDesc.getSampleRows());
                tableDescResponse.setJodID(tableExtDesc.getJodID());
                filterSamplingRows(project, tableDescResponse, isAclGreen, aclTCRS);
            }

            if (CollectionUtils.isNotEmpty(modelsUsingRootTable)) {
                tableDescResponse.setRootFact(true);
                tableDescResponse.setStorageSize(getStorageSize(project, modelsUsingRootTable, fs));
            } else if (CollectionUtils.isNotEmpty(modelsUsingTable)) {
                tableDescResponse.setLookup(true);
                tableDescResponse.setStorageSize(getSnapshotSize(project, table.getIdentity(), fs));
            }
            Pair<Set<String>, Set<String>> tableColumnType = getTableColumnType(project, table, modelsUsingTable);
            NDataLoadingRange dataLoadingRange = getDataLoadingRangeManager(project)
                    .getDataLoadingRange(table.getIdentity());
            if (null != dataLoadingRange) {
                tableDescResponse.setPartitionedColumn(dataLoadingRange.getColumnName());
                tableDescResponse.setPartitionedColumnFormat(dataLoadingRange.getPartitionDateFormat());
                tableDescResponse.setSegmentRange(dataLoadingRange.getCoveredRange());
            }
            tableDescResponse.setForeignKey(tableColumnType.getSecond());
            tableDescResponse.setPrimaryKey(tableColumnType.getFirst());
            descs.add(tableDescResponse);
        }

        return descs;
    }

    @VisibleForTesting
    void filterSamplingRows(String project, TableDescResponse rtableDesc, boolean isAclGreen, List<AclTCR> aclTCRS) {
        if (isAclGreen) {
            return;
        }
        List<String[]> result = Lists.newArrayList();
        final String dbTblName = rtableDesc.getIdentity();
        Map columnRows = Arrays.stream(rtableDesc.getExtColumns()).map(cdr -> {
            int id = Integer.parseInt(cdr.getId());
            val columnRealRows = getAclTCRManager(project).getAuthorizedRows(dbTblName, cdr.getName(), aclTCRS);
            return new AbstractMap.SimpleEntry<>(id, columnRealRows);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        for (String[] row : rtableDesc.getSamplingRows()) {
            if (Objects.isNull(row)) {
                continue;
            }
            int i = 0;
            boolean jumpThisSample = false;
            List<String> nlist = Lists.newArrayList();
            while (i++ < row.length) {
                if (!columnRows.containsKey(i)) {
                    continue;
                }
                val columnRealRows = (AclTCR.ColumnRealRows) columnRows.get(i);
                if (Objects.isNull(columnRealRows)) {
                    jumpThisSample = true;
                    break;
                }
                Set<String> equalRows = columnRealRows.getRealRow();
                Set<String> likeRows = columnRealRows.getRealLikeRow();
                if ((CollectionUtils.isNotEmpty(equalRows) && !equalRows.contains(row[i - 1]))
                        || (CollectionUtils.isNotEmpty(likeRows) && noMatchedLikeCondition(row[i - 1], likeRows))) {
                    jumpThisSample = true;
                    break;
                }
                nlist.add(row[i - 1]);
            }
            if (jumpThisSample) {
                continue;
            }
            if (CollectionUtils.isNotEmpty(nlist)) {
                result.add(nlist.toArray(new String[0]));
            }
        }
        rtableDesc.setSamplingRows(result);
    }

    private static boolean noMatchedLikeCondition(String target, Set<String> likePatterns) {
        for (String likePattern : likePatterns) {
            val matcher = new LikeMatchers.DefaultLikeMatcher(likePattern, "\\");
            if (matcher.matches(target)) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    TableDesc getAuthorizedTableDesc(String project, boolean isAclGreen, TableDesc originTable, List<AclTCR> aclTCRS) {
        if (isAclGreen) {
            return originTable;
        }
        return getAclTCRManager(project).getAuthorizedTableDesc(originTable, aclTCRS);
    }

    public long getSnapshotSize(String project, String table, FileSystem fs) {
        val tableDesc = getTableManager(project).getTableDesc(table);
        if (tableDesc != null && tableDesc.getLastSnapshotPath() != null) {
            try {
                val path = new Path(KapConfig.wrap(KylinConfig.getInstanceFromEnv()).getMetadataWorkingDirectory(),
                        tableDesc.getLastSnapshotPath());
                return HadoopUtil.getContentSummary(fs, path).getLength();
            } catch (Exception e) {
                logger.warn("cannot get snapshot path {}", tableDesc.getLastSnapshotPath(), e);
            }
        }
        return 0;
    }

    public long getSnapshotModificationTime(TableDesc tableDesc) {
        if (tableDesc != null && tableDesc.getLastSnapshotPath() != null) {
            try {
                val path = new Path(KapConfig.wrap(KylinConfig.getInstanceFromEnv()).getMetadataWorkingDirectory(),
                        tableDesc.getLastSnapshotPath());
                return HDFSUtils.getFileStatus(path).getModificationTime();
            } catch (Exception e) {
                logger.warn("cannot get snapshot path {}", tableDesc.getLastSnapshotPath(), e);
            }
        }
        return 0;
    }

    private long getStorageSize(String project, List<NDataModel> models, FileSystem fs) {
        val dfManger = getDataflowManager(project);
        long size = 0;
        for (val model : models) {
            val df = dfManger.getDataflow(model.getUuid());
            val readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
            if (CollectionUtils.isNotEmpty(readySegs)) {
                for (NDataSegment segment : readySegs) {
                    size += segment.getStorageBytesSize();
                }
            }
        }
        return size;
    }

    //get table's primaryKeys(pair first) and foreignKeys(pair second)
    private Pair<Set<String>, Set<String>> getTableColumnType(String project, TableDesc table,
            List<NDataModel> modelsUsingTable) {
        val dataModelManager = getDataModelManager(project);
        Set<String> primaryKey = new HashSet<>();
        Set<String> foreignKey = new HashSet<>();
        for (val model : modelsUsingTable) {
            val joinTables = dataModelManager.getDataModelDesc(model.getUuid()).getJoinTables();
            for (JoinTableDesc joinTable : joinTables) {
                if (joinTable.getTable().equals(table.getIdentity())) {
                    foreignKey.addAll(Arrays.asList(joinTable.getJoin().getForeignKey()));
                    primaryKey.addAll(Arrays.asList(joinTable.getJoin().getPrimaryKey()));
                    break;
                }
            }
        }
        Pair<Set<String>, Set<String>> result = new Pair<>();
        result.setFirst(primaryKey);
        result.setSecond(foreignKey);
        return result;
    }

    public String normalizeHiveTableName(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase(Locale.ROOT);
    }

    @Transaction(project = 1)
    public void setPartitionKey(String table, String project, String column, String columnFormat) {
        aclEvaluate.checkProjectWritePermission(project);
        if (StringUtils.isNotEmpty(column)) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(columnFormat),
                    "Partition column format can not be empty!");
        }

        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        String tableName = table.substring(table.lastIndexOf('.') + 1);
        String columnIdentity = tableName + "." + column;
        if ((dataLoadingRange == null && StringUtils.isEmpty(column)) || (dataLoadingRange != null
                && StringUtils.equalsIgnoreCase(columnIdentity, dataLoadingRange.getColumnName())
                && StringUtils.equalsIgnoreCase(columnFormat, dataLoadingRange.getPartitionDateFormat()))) {
            logger.info("Partition column {} does not change", column);
            return;
        }
        handlePartitionColumnChanged(dataLoadingRange, columnIdentity, column, columnFormat, project, table);
    }

    private void purgeRelatedModel(String modelId, String table, String project) {
        val dfManager = getDataflowManager(project);
        // toggle table type, remove all segments in related models
        //follow semanticVersion,#8196
        modelService.purgeModel(modelId, project);
        val dataflow = dfManager.getDataflow(modelId);
        if (RealizationStatusEnum.LAG_BEHIND == dataflow.getStatus()) {
            dfManager.updateDataflowStatus(dataflow.getId(), RealizationStatusEnum.ONLINE);
        }
    }

    private void handlePartitionColumnChanged(NDataLoadingRange dataLoadingRange, String columnIdentity, String column,
            String columnFormat, String project, String table) {
        val dataLoadingRangeManager = getDataLoadingRangeManager(project);
        val tableManager = getTableManager(project);
        val tableDesc = tableManager.getTableDesc(table);
        val copy = tableManager.copyForWrite(tableDesc);
        if (StringUtils.isEmpty(column)) {
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
            copy.setIncrementLoading(false);
            tableManager.updateTableDesc(copy);
        } else {
            modelService.checkSingleIncrementingLoadingTable(project, table);
            if (dataLoadingRange != null) {
                val loadingRangeCopy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
                loadingRangeCopy.setColumnName(columnIdentity);
                loadingRangeCopy.setPartitionDateFormat(columnFormat);
                loadingRangeCopy.setCoveredRange(null);
                dataLoadingRangeManager.updateDataLoadingRange(loadingRangeCopy);
            } else {
                dataLoadingRange = new NDataLoadingRange(table, columnIdentity);
                dataLoadingRange.setPartitionDateFormat(columnFormat);
                logger.info("Create DataLoadingRange {}", dataLoadingRange.getTableName());
                dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
            }
            copy.setIncrementLoading(true);
            tableManager.updateTableDesc(copy);
        }
        val dfManager = getDataflowManager(project);
        val models = dfManager.getTableOrientedModelsUsingRootTable(tableDesc);
        for (val model : models) {
            purgeRelatedModel(model.getUuid(), table, project);
            modelService.syncPartitionDesc(model.getUuid(), project);
            if (StringUtils.isEmpty(column)) {
                buildFullSegment(model.getUuid(), project);
            }
        }
    }

    private void buildFullSegment(String model, String project) {
        val jobManager = getJobManager(project);
        val dataflowManager = getDataflowManager(project);
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(model);
        val dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        val newSegment = dataflowManager.appendSegment(dataflow,
                new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));

        getSourceUsageManager().licenseCheckWrap(project,
                () -> jobManager.addSegmentJob(new JobParam(newSegment, model, getUsername())));
    }

    public void setDataRange(String project, DateRangeRequest dateRangeRequest) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
        String table = dateRangeRequest.getTable();
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        Preconditions.checkNotNull(dataLoadingRange, "table " + table + " is not incremental, ");
        SegmentRange allRange = dataLoadingRange.getCoveredRange();

        var start = dateRangeRequest.getStart();
        var end = dateRangeRequest.getEnd();

        if (PushDownUtil.needPushdown(start, end)) {
            val pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);
            start = PushDownUtil.calcStart(pushdownResult.getFirst(), allRange);
            end = pushdownResult.getSecond();
        }

        if (allRange != null && allRange.getEnd().toString().equals(end))
            throw new IllegalStateException("There is no more new data to load");

        String finalStart = start;
        String finalEnd = end;
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            saveDataRange(project, table, finalStart, finalEnd);
            return null;
        }, project);
    }

    private void saveDataRange(String project, String table, String start, String end) throws Exception {
        proposeAndSaveDateFormatIfNotExist(project, table);
        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        SegmentRange newSegmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(start, end);
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        rangeManager.appendSegmentRange(dataLoadingRange, newSegmentRange);
        handleLoadingRangeUpdate(project, table, newSegmentRange);
    }

    public ExistedDataRangeResponse getLatestDataRange(String project, String table) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);
        val start = PushDownUtil.calcStart(pushdownResult.getFirst(), dataLoadingRange.getCoveredRange());
        return new ExistedDataRangeResponse(start, pushdownResult.getSecond());
    }

    public String getPartitionColumnFormat(String project, String table, String partitionColumn) throws Exception {
        aclEvaluate.checkProjectOperationPermission(project);

        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        Preconditions.checkNotNull(tableDesc,
                String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), table));
        Set<String> columnSet = Stream.of(tableDesc.getColumns()).map(ColumnDesc::getName)
                .map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        if (!columnSet.contains(partitionColumn.toUpperCase(Locale.ROOT))) {
            throw new KylinException(COLUMN_NOT_EXIST, String.format(Locale.ROOT,
                    "Can not find the column:%s in table:%s, project:%s", partitionColumn, table, project));
        }
        try {
            if (tableDesc.getKafkaConfig() != null) {
                List<ByteBuffer> messages = kafkaService.getMessages(tableDesc.getKafkaConfig(), project, 0);
                if (messages == null || messages.isEmpty()) {
                    throw new KylinException(EMPTY_TABLE,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getNO_DATA_IN_TABLE(), table));
                }
                Map<String, Object> resp = kafkaService.getMessageTypeAndDecodedMessages(messages);
                String json = ((List<String>) resp.get("message")).get(0);
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object> mapping = mapper.readValue(json, Map.class);
                Map<String, Object> mappingAllCaps = new HashMap<>();
                mapping.forEach((key, value) -> mappingAllCaps.put(key.toUpperCase(Locale.ROOT), value));
                String cell = (String) mappingAllCaps.get(partitionColumn);
                return DateFormat.proposeDateFormat(cell);
            } else {
                String cell = PushDownUtil.getFormatIfNotExist(table, partitionColumn, project);
                return DateFormat.proposeDateFormat(cell);
            }

        } catch (Exception e) {
            logger.error("Failed to get date format.", e);
            throw new KylinException(INVALID_PARTITION_COLUMN, MsgPicker.getMsg().getPUSHDOWN_PARTITIONFORMAT_ERROR());
        }
    }

    public Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String table)
            throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        String partitionColumn = dataLoadingRange.getColumnName();

        val maxAndMinTime = PushDownUtil.getMaxAndMinTimeWithTimeOut(partitionColumn, table, project);
        String dateFormat;
        if (StringUtils.isEmpty(dataLoadingRange.getPartitionDateFormat()))
            dateFormat = setPartitionColumnFormat(maxAndMinTime.getFirst(), project, table);
        else
            dateFormat = dataLoadingRange.getPartitionDateFormat();

        return new Pair<>(DateFormat.getFormattedDate(maxAndMinTime.getFirst(), dateFormat),
                DateFormat.getFormattedDate(maxAndMinTime.getSecond(), dateFormat));
    }

    private String setPartitionColumnFormat(String time, String project, String table) {
        String format = DateFormat.proposeDateFormat(time);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
            NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);

            val copy = rangeManager.copyForWrite(dataLoadingRange);
            copy.setPartitionDateFormat(format);
            rangeManager.updateDataLoadingRange(copy);

            // sync to all related models
            val dataflowManager = getDataflowManager(project);
            TableDesc tableDesc = getTableManager(project).getTableDesc(table);
            val models = dataflowManager.getTableOrientedModelsUsingRootTable(tableDesc);
            for (val model : models) {
                modelService.syncPartitionDesc(model.getUuid(), project);
            }

            return 0;
        }, project);
        return format;
    }

    private void proposeAndSaveDateFormatIfNotExist(String project, String table) throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        if (StringUtils.isNotEmpty(dataLoadingRange.getPartitionDateFormat()))
            return;

        String partitionColumn = dataLoadingRange.getColumnName();

        val format = PushDownUtil.getFormatIfNotExist(table, partitionColumn, project);

        setPartitionColumnFormat(format, project, table);
    }

    private void handleLoadingRangeUpdate(String project, String tableName, SegmentRange segmentRange)
            throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }
        List<NDataModel> models = NDataflowManager.getInstance(kylinConfig, project)
                .getTableOrientedModelsUsingRootTable(tableDesc);
        if (CollectionUtils.isNotEmpty(models)) {
            val jobManager = getJobManager(project);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
            for (var model : models) {
                val modelId = model.getUuid();
                IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project).getIndexPlan(modelId);
                NDataflow df = dataflowManager.getDataflow(indexPlan.getUuid());
                NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);

                getSourceUsageManager().licenseCheckWrap(project,
                        () -> jobManager.addSegmentJob(new JobParam(dataSegment, modelId, getUsername())));

                logger.info(
                        "LoadingRangeUpdateHandler produce AddSegmentEvent project : {}, model : {}, segmentRange : {}",
                        project, modelId, segmentRange);
            }
        }
    }

    @VisibleForTesting
    public SegmentRange getSegmentRangeByTable(DateRangeRequest dateRangeRequest) {
        String project = dateRangeRequest.getProject();
        String table = dateRangeRequest.getTable();
        NTableMetadataManager nProjectManager = getTableManager(project);
        TableDesc tableDesc = nProjectManager.getTableDesc(table);
        return SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(),
                dateRangeRequest.getEnd());

    }

    public List<BatchLoadTableResponse> getBatchLoadTables(String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        final List<TableDesc> incrementalLoadTables = getTableManager(project).getAllIncrementalLoadTables();
        final List<BatchLoadTableResponse> result = Lists.newArrayList();

        for (TableDesc table : incrementalLoadTables) {
            String tableIdentity = table.getIdentity();
            int relatedIndexNum = getRelatedIndexNumOfATable(table, project);
            result.add(new BatchLoadTableResponse(tableIdentity, relatedIndexNum));
        }

        return result;
    }

    private int getRelatedIndexNumOfATable(TableDesc tableDesc, String project) {
        int result = 0;
        val dataflowManager = getDataflowManager(project);
        for (val model : dataflowManager.getTableOrientedModelsUsingRootTable(tableDesc)) {
            IndexPlan indexPlan = getIndexPlanManager(project).getIndexPlan(model.getUuid());
            result += indexPlan.getAllIndexes().size();
        }

        return result;
    }

    public void batchLoadDataRange(String project, List<DateRangeRequest> requests) throws Exception {
        NProjectManager projectManager = getProjectManager();
        ProjectInstance projectInstance = projectManager.getProject(project);
        if (projectInstance != null) {
            project = projectInstance.getName();
        }
        aclEvaluate.checkProjectOperationPermission(project);
        for (DateRangeRequest request : requests) {
            setDataRange(project, request);
        }
    }

    private List<AbstractExecutable> stopAndGetSnapshotJobs(String project, String table) {
        val execManager = getExecutableManager(project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);

        List<AbstractExecutable> conflictJobs = executables.stream()
                .filter(exec -> table.equalsIgnoreCase(exec.getParam(NBatchConstants.P_TABLE_NAME)))
                .collect(Collectors.toList());

        conflictJobs.forEach(job -> {
            execManager.discardJob(job.getId());
        });
        return conflictJobs;
    }

    @Transaction(project = 0)
    public void unloadTable(String project, String table, Boolean cascade) {
        aclEvaluate.checkProjectWritePermission(project);
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        val tableDesc = tableMetadataManager.getTableDesc(table);
        if (tableDesc == null) {
            val msg = MsgPicker.getMsg();
            throw new KylinException(INVALID_TABLE_NAME, String.format(Locale.ROOT, msg.getTABLE_NOT_FOUND(), table));
        }

        stopAndGetSnapshotJobs(project, table);

        val dataflowManager = getDataflowManager(project);
        if (cascade) {
            for (NDataModel tableRelatedModel : dataflowManager.getModelsUsingTable(tableDesc)) {
                fusionModelService.dropModel(tableRelatedModel.getId(), project, true);
            }
            unloadKafkaTableUsingTable(project, tableDesc);
        } else {
            stopStreamingJobByTable(project, tableDesc);
            jobService.stopBatchJob(project, tableDesc);
        }

        unloadTable(project, table);

        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (dataLoadingRange != null) {
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
        }

        aclTCRService.unloadTable(project, table);

        NProjectManager npr = getProjectManager();
        final ProjectInstance projectInstance = npr.getProject(project);
        Set<String> databases = getLoadedDatabases(project).stream().map(str -> str.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());
        if (tableDesc.getDatabase().equals(projectInstance.getDefaultDatabase())
                && !databases.contains(projectInstance.getDefaultDatabase())) {
            projectInstance.setDefaultDatabase(ProjectInstance.DEFAULT_DATABASE);
            npr.updateProject(projectInstance);
        }
    }

    private void unloadKafkaTableUsingTable(String project, TableDesc tableDesc) {
        if (tableDesc.getSourceType() != ISourceAware.ID_SPARK)
            return;

        val kafkaConfigManger = getKafkaConfigManager(project);
        for (KafkaConfig kafkaConfig : kafkaConfigManger.getKafkaTablesUsingTable(tableDesc.getIdentity())) {
            unloadTable(project, kafkaConfig.getIdentity());
        }
    }

    private void stopStreamingJobByTable(String project, TableDesc tableDesc) {
        for (NDataModel tableRelatedModel : getDataflowManager(project).getModelsUsingTable(tableDesc)) {
            fusionModelService.stopStreamingJob(tableRelatedModel.getId(), project);
        }
    }

    public void unloadTable(String project, String tableIdentity) {
        getTableManager(project).removeTableExt(tableIdentity);
        getTableManager(project).removeSourceTable(tableIdentity);
        getKafkaConfigManager(project).removeKafkaConfig(tableIdentity);
    }

    @Transaction(project = 0, readonly = true)
    public PreUnloadTableResponse preUnloadTable(String project, String tableIdentity) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        val response = new PreUnloadTableResponse();
        val dataflowManager = getDataflowManager(project);
        val tableMetadataManager = getTableManager(project);
        val execManager = getExecutableManager(project);

        val tableDesc = tableMetadataManager.getTableDesc(tableIdentity);
        val models = dataflowManager.getModelsUsingTable(tableDesc);
        response.setHasModel(!models.isEmpty());
        response.setModels(models.stream().map(NDataModel::getAlias).collect(Collectors.toList()));

        val rootTableModels = dataflowManager.getModelsUsingRootTable(tableDesc);
        val fs = HadoopUtil.getWorkingFileSystem();
        long storageSize = 0;
        if (CollectionUtils.isNotEmpty(rootTableModels)) {
            storageSize += getStorageSize(project, rootTableModels, fs);
        }
        storageSize += getSnapshotSize(project, tableIdentity, fs);
        response.setStorageSize(storageSize);

        response.setHasJob(
                execManager.countByModelAndStatus(tableIdentity, state -> state == ExecutableState.RUNNING) > 0);
        response.setHasSnapshot(tableDesc.getLastSnapshotPath() != null);

        return response;
    }

    @Transaction(project = 1)
    public void setTop(String table, String project, boolean top) {
        aclEvaluate.checkProjectWritePermission(project);
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        TableDesc tableDesc = nTableMetadataManager.getTableDesc(table);
        tableDesc = nTableMetadataManager.copyForWrite(tableDesc);
        tableDesc.setTop(top);
        nTableMetadataManager.updateTableDesc(tableDesc);
    }

    public List<TablesAndColumnsResponse> getTableAndColumns(String project) {
        aclEvaluate.checkProjectReadPermission(project);
        List<TableDesc> tables = getTableManager(project).listAllTables();
        List<TablesAndColumnsResponse> result = new ArrayList<>();
        for (TableDesc table : tables) {
            TablesAndColumnsResponse response = new TablesAndColumnsResponse();
            response.setTable(table.getName());
            response.setDatabase(table.getDatabase());
            ColumnDesc[] columns = table.getColumns();
            List<String> columnNames = new ArrayList<>();
            for (ColumnDesc column : columns) {
                columnNames.add(column.getName());
            }
            response.setColumns(columnNames);
            result.add(response);
        }
        return result;
    }

    public void checkRefreshDataRangeReadiness(String project, String table, String start, String end) {
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        TableDesc tableDesc = tableMetadataManager.getTableDesc(table);
        if (!tableDesc.isIncrementLoading())
            return;

        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        SegmentRange readySegmentRange = dataLoadingRange.getCoveredRange();
        if (readySegmentRange == null) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT());
        }
        SegmentRange segmentRangeRefresh = SourceFactory.getSource(tableDesc).getSegmentRange(start, end);

        if (!readySegmentRange.contains(segmentRangeRefresh)) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_REFRESH_SEGMENT_BY_NOT_READY());
        }
    }

    private NDataLoadingRange getDataLoadingRange(String project, String table) {
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (dataLoadingRange == null) {
            throw new IllegalStateException(
                    "this table can not set date range, please check table " + table + " is fact or not");
        }
        return dataLoadingRange;
    }

    @Transaction(project = 0)
    public void setPushDownMode(String project, String table, boolean pushdownRangeLimited) {
        aclEvaluate.checkProjectWritePermission(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRangeUpdate = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        dataLoadingRangeUpdate.setPushdownRangeLimited(pushdownRangeLimited);
        dataLoadingRangeManager.updateDataLoadingRange(dataLoadingRangeUpdate);
    }

    public AutoMergeConfigResponse getAutoMergeConfigByModel(String project, String modelId) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataModelManager dataModelManager = getDataModelManager(project);
        AutoMergeConfigResponse mergeConfig = new AutoMergeConfigResponse();

        NDataModel model = dataModelManager.getDataModelDesc(modelId);
        if (model == null) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        Preconditions.checkState(segmentConfig != null);
        mergeConfig.setAutoMergeEnabled(segmentConfig.getAutoMergeEnabled());
        mergeConfig.setAutoMergeTimeRanges(segmentConfig.getAutoMergeTimeRanges());
        mergeConfig.setVolatileRange(segmentConfig.getVolatileRange());
        return mergeConfig;
    }

    public AutoMergeConfigResponse getAutoMergeConfigByTable(String project, String tableName) {
        aclEvaluate.checkProjectOperationPermission(project);
        AutoMergeConfigResponse mergeConfig = new AutoMergeConfigResponse();
        val segmentConfig = NSegmentConfigHelper.getTableSegmentConfig(project, tableName);
        Preconditions.checkState(segmentConfig != null);
        mergeConfig.setAutoMergeEnabled(segmentConfig.getAutoMergeEnabled());
        mergeConfig.setAutoMergeTimeRanges(segmentConfig.getAutoMergeTimeRanges());
        mergeConfig.setVolatileRange(segmentConfig.getVolatileRange());
        return mergeConfig;
    }

    @Transaction(project = 0)
    public void setAutoMergeConfigByModel(String project, AutoMergeRequest autoMergeRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        String modelId = autoMergeRequest.getModel();
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<AutoMergeTimeEnum> autoMergeRanges = new ArrayList<>();
        for (String range : autoMergeRequest.getAutoMergeTimeRanges()) {
            autoMergeRanges.add(AutoMergeTimeEnum.valueOf(range));
        }
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.valueOf(autoMergeRequest.getVolatileRangeType()));
        volatileRange.setVolatileRangeEnabled(autoMergeRequest.isVolatileRangeEnabled());
        volatileRange.setVolatileRangeNumber(autoMergeRequest.getVolatileRangeNumber());

        NDataModel model = dataModelManager.getDataModelDesc(modelId);
        if (model == null) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelId));
        }
        if (ManagementType.MODEL_BASED == model.getManagementType()) {
            NDataModel modelUpdate = dataModelManager.copyForWrite(model);
            var segmentConfig = modelUpdate.getSegmentConfig();
            segmentConfig.setVolatileRange(volatileRange);
            segmentConfig.setAutoMergeTimeRanges(autoMergeRanges);
            segmentConfig.setAutoMergeEnabled(autoMergeRequest.isAutoMergeEnabled());
            dataModelManager.updateDataModelDesc(modelUpdate);

        } else {
            autoMergeRequest.setTable(model.getRootFactTable().getTableIdentity());
            setAutoMergeConfigByTable(project, autoMergeRequest);

        }
    }

    public boolean getPushDownMode(String project, String table) {
        aclEvaluate.checkProjectOperationPermission(project);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        return dataLoadingRange.isPushdownRangeLimited();
    }

    @Transaction(project = 0)
    public void setAutoMergeConfigByTable(String project, AutoMergeRequest autoMergeRequest) {
        aclEvaluate.checkProjectWritePermission(project);
        String tableName = autoMergeRequest.getTable();
        List<AutoMergeTimeEnum> autoMergeRanges = new ArrayList<>();
        for (String range : autoMergeRequest.getAutoMergeTimeRanges()) {
            autoMergeRanges.add(AutoMergeTimeEnum.valueOf(range));
        }
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.valueOf(autoMergeRequest.getVolatileRangeType()));
        volatileRange.setVolatileRangeEnabled(autoMergeRequest.isVolatileRangeEnabled());
        volatileRange.setVolatileRangeNumber(autoMergeRequest.getVolatileRangeNumber());
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, tableName);
        NDataLoadingRange dataLoadingRangeUpdate = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        var segmentConfig = dataLoadingRangeUpdate.getSegmentConfig();
        segmentConfig.setAutoMergeEnabled(autoMergeRequest.isAutoMergeEnabled());
        segmentConfig.setAutoMergeTimeRanges(autoMergeRanges);
        segmentConfig.setVolatileRange(volatileRange);
        dataLoadingRangeManager.updateDataLoadingRange(dataLoadingRangeUpdate);
    }

    @Transaction(project = 0, readonly = true)
    public OpenPreReloadTableResponse preProcessBeforeReloadWithoutFailFast(String project, String tableIdentity)
            throws Exception {
        aclEvaluate.checkProjectWritePermission(project);

        val context = calcReloadContext(project, tableIdentity, false);
        PreReloadTableResponse preReloadTableResponse = preProcessBeforeReloadWithContext(project, context);

        OpenPreReloadTableResponse openPreReloadTableResponse = new OpenPreReloadTableResponse(preReloadTableResponse);
        openPreReloadTableResponse.setDuplicatedColumns(Lists.newArrayList(context.getDuplicatedColumns()));
        openPreReloadTableResponse.setEffectedJobs(Lists.newArrayList(context.getEffectedJobs()));
        boolean hasDatasourceChanged = (preReloadTableResponse.getAddColumnCount()
                + preReloadTableResponse.getRemoveColumnCount()
                + preReloadTableResponse.getDataTypeChangeColumnCount()) > 0;
        openPreReloadTableResponse.setHasDatasourceChanged(hasDatasourceChanged);
        openPreReloadTableResponse.setHasEffectedJobs(!context.getEffectedJobs().isEmpty());
        openPreReloadTableResponse.setHasDuplicatedColumns(!context.getDuplicatedColumns().isEmpty());

        return openPreReloadTableResponse;
    }

    @Transaction(project = 0, readonly = true)
    public PreReloadTableResponse preProcessBeforeReloadWithFailFast(String project, String tableIdentity)
            throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        val context = calcReloadContext(project, tableIdentity, true);
        return preProcessBeforeReloadWithContext(project, context);
    }

    private PreReloadTableResponse preProcessBeforeReloadWithContext(String project, ReloadTableContext context) {
        val result = new PreReloadTableResponse();
        result.setAddColumnCount(context.getAddColumns().size());
        result.setRemoveColumnCount(context.getRemoveColumns().size());
        result.setRemoveDimCount(context.getRemoveAffectedModels().values().stream()
                .map(AffectedModelContext::getDimensions).mapToLong(Set::size).sum());
        result.setDataTypeChangeColumnCount(context.getChangeTypeColumns().size());
        val schemaChanged = result.getAddColumnCount() > 0 || result.getRemoveColumnCount() > 0
                || result.getDataTypeChangeColumnCount() > 0;
        result.setSnapshotDeleted(schemaChanged);
        val projectInstance = getProjectManager().getProject(project);
        if (projectInstance.getMaintainModelType() == MaintainModelType.MANUAL_MAINTAIN) {
            val affectedModels = Maps.newHashMap(context.getChangeTypeAffectedModels());
            affectedModels.putAll(context.getRemoveAffectedModels());
            result.setBrokenModelCount(affectedModels.values().stream().filter(AffectedModelContext::isBroken).count());
        }
        // change type column also will remove measure when column type change
        long removeColumnAffectMeasureSum = context.getRemoveAffectedModels().values().stream()
                .map(AffectedModelContext::getMeasures).mapToLong(Set::size).sum();

        long changeColumnTypeAffectMeasureSum = context.getChangeTypeAffectedModels().values().stream()
                .map(AffectedModelContext::getMeasures).mapToLong(Set::size).sum();

        result.setRemoveMeasureCount(removeColumnAffectMeasureSum + changeColumnTypeAffectMeasureSum);
        result.setRemoveLayoutsCount(
                context.getRemoveAffectedModels().values().stream().mapToLong(m -> m.getUpdatedLayouts().size()).sum());
        result.setAddLayoutsCount(
                context.getRemoveAffectedModels().values().stream().mapToLong(m -> m.getAddLayouts().size()).sum());
        result.setRefreshLayoutsCount(context.getChangeTypeAffectedModels().values().stream()
                .mapToLong(m -> Sets
                        .difference(m.getUpdatedLayouts(),
                                context.getRemoveAffectedModel(m.getProject(), m.getModelId()).getUpdatedLayouts())
                        .size())
                .sum());

        result.setUpdateBaseIndexCount(context.getChangeTypeAffectedModels().values().stream().mapToInt(m -> {
            IndexPlan indexPlan = NIndexPlanManager.getInstance(getConfig(), m.getProject())
                    .getIndexPlan(m.getModelId());
            if (!indexPlan.getConfig().isBaseIndexAutoUpdate()) {
                return 0;
            }
            Set<Long> updateLayouts = Sets.newHashSet();
            updateLayouts.addAll(m.getUpdatedLayouts());
            updateLayouts.addAll(context.getRemoveAffectedModel(m.getProject(), m.getModelId()).getUpdatedLayouts());
            int updateBaseIndexCount = 0;
            if (updateLayouts.contains(indexPlan.getBaseAggLayoutId())) {
                updateBaseIndexCount++;
            }
            if (updateLayouts.contains(indexPlan.getBaseTableLayoutId())) {
                updateBaseIndexCount++;
            }
            return updateBaseIndexCount;
        }).sum());

        return result;
    }

    public Pair<String, List<String>> reloadTable(String projectName, String tableIdentity, boolean needSample,
            int maxRows, boolean needBuild) {
        return reloadTable(projectName, tableIdentity, needSample, maxRows, needBuild, ExecutablePO.DEFAULT_PRIORITY, null);
    }

    public Pair<String, List<String>> reloadTable(String projectName, String tableIdentity, boolean needSample,
            int maxRows, boolean needBuild, int priority, String yarnQueue) {
        aclEvaluate.checkProjectWritePermission(projectName);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            Pair<String, List<String>> pair = new Pair<>();
            List<String> buildingJobs = innerReloadTable(projectName, tableIdentity, needBuild);
            pair.setSecond(buildingJobs);
            if (needSample && maxRows > 0) {
                List<String> jobIds = tableSamplingService.sampling(Sets.newHashSet(tableIdentity), projectName,
                        maxRows, priority, yarnQueue);
                if (CollectionUtils.isNotEmpty(jobIds)) {
                    pair.setFirst(jobIds.get(0));
                }
            }
            return pair;
        }, projectName);
    }

    @Transaction(project = 0)
    List<String> innerReloadTable(String projectName, String tableIdentity, boolean needBuild) throws Exception {
        val tableManager = getTableManager(projectName);
        val originTable = tableManager.getTableDesc(tableIdentity);
        Preconditions.checkNotNull(originTable,
                String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableIdentity));

        val project = getProjectManager().getProject(projectName);
        val context = calcReloadContext(projectName, tableIdentity, true);
        Set<NDataModel> affectedModels = getAffectedModels(projectName, context);
        List<String> jobs = Lists.newArrayList();
        for (val model : affectedModels) {
            val jobId = updateBrokenModel(project, model, context, needBuild);
            if (StringUtils.isNotEmpty(jobId)) {
                jobs.add(jobId);
            }
        }
        mergeTable(projectName, context, true);
        for (val model : affectedModels) {
            val jobId = updateModelByReloadTable(project, model, context, needBuild);
            if (StringUtils.isNotEmpty(jobId)) {
                jobs.add(jobId);
            }
        }

        val loadingManager = getDataLoadingRangeManager(projectName);
        val removeCols = context.getRemoveColumnFullnames();
        loadingManager.getDataLoadingRanges().forEach(loadingRange -> {
            if (removeCols.contains(loadingRange.getColumnName())) {
                setPartitionKey(tableIdentity, projectName, null, null);
            }
        });

        mergeTable(projectName, context, false);
        return jobs;
    }

    private Set<NDataModel> getAffectedModels(String projectName, ReloadTableContext context) {
        if ((context.getAddColumns().isEmpty() && context.getRemoveColumns().isEmpty()
                && context.getChangeTypeColumns().isEmpty())) {
            return Sets.newHashSet();
        }
        if (Objects.isNull(context.getTableDesc())) {
            return Sets.newHashSet();
        }
        String tableIdentity = context.getTableDesc().getIdentity();
        List<NDataModel> allHealthModels = getDataflowManager(projectName).listUnderliningDataModels();
        return allHealthModels.stream().filter(modelDesc -> modelDesc.getAllTables().stream()
                .map(TableRef::getTableIdentity).anyMatch(tableIdentity::equalsIgnoreCase)).collect(Collectors.toSet());
    }

    private String updateBrokenModel(ProjectInstance project, NDataModel model, ReloadTableContext context,
            boolean needBuild) throws Exception {
        val removeAffectedModel = context.getRemoveAffectedModel(project.getName(), model.getId());
        val changeTypeAffectedModel = context.getChangeTypeAffectedModel(project.getName(), model.getId());

        if (!removeAffectedModel.isBroken()) {
            return null;
        }
        val projectName = project.getName();
        if (project.getMaintainModelType() == MaintainModelType.AUTO_MAINTAIN) {
            return null;
        }

        cleanIndexPlan(projectName, model, Lists.newArrayList(removeAffectedModel, changeTypeAffectedModel));

        getOptRecManagerV2(projectName).discardAll(model.getId());
        val request = new ModelRequest(JsonUtil.deepCopy(model, NDataModel.class));
        setRequest(request, model, removeAffectedModel, changeTypeAffectedModel, projectName);
        modelService.updateBrokenModel(projectName, request, removeAffectedModel.getColumns());

        val jobManager = getJobManager(projectName);
        val updatedModel = getDataModelManager(projectName).getDataModelDesc(model.getId());
        if (needBuild && !updatedModel.isBroken()) {
            return getSourceUsageManager().licenseCheckWrap(projectName,
                    () -> jobManager.addIndexJob(new JobParam(model.getId(), getUsername())));
        }
        return null;
    }

    private String updateModelByReloadTable(ProjectInstance project, NDataModel model, ReloadTableContext context,
            boolean needBuild) throws Exception {
        val projectName = project.getName();
        val baseIndexUpdater = new BaseIndexUpdateHelper(model, false);
        val removeAffected = context.getRemoveAffectedModel(project.getName(), model.getId());
        val changeTypeAffected = context.getChangeTypeAffectedModel(project.getName(), model.getId());
        if (removeAffected.isBroken()) {
            return null;
        }

        if (!(context.getRemoveColumns().isEmpty() && context.getChangeTypeColumns().isEmpty())) {
            cleanIndexPlan(projectName, model, Lists.newArrayList(removeAffected, changeTypeAffected));
        }

        getOptRecManagerV2(projectName).discardAll(model.getId());

        val request = new ModelRequest(JsonUtil.deepCopy(model, NDataModel.class));
        setRequest(request, model, removeAffected, changeTypeAffected, projectName);
        request.setColumnsFetcher((tableRef, isFilterCC) -> TableRef.filterColumns(
                tableRef.getIdentity().equals(context.getTableDesc().getIdentity()) ? context.getTableDesc() : tableRef,
                isFilterCC));
        try {
            modelService.updateDataModelSemantic(projectName, request);
        } catch (TransactionException e) {
            Throwable root = ExceptionUtils.getRootCause(e) == null ? e : ExceptionUtils.getRootCause(e);
            String tableName = context.getTableDesc().getName();
            String columnNames = String.join(MsgPicker.getMsg().getCOMMA(), context.getChangeTypeColumns());
            if (root instanceof IllegalCCExpressionException) {
                throw new KylinException(INVALID_COMPUTED_COLUMN_EXPRESSION, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getRELOAD_TABLE_CC_RETRY(), root.getMessage(), tableName, columnNames));
            } else if (root instanceof KylinException
                    && ((KylinException) root).getErrorCode() == QueryErrorCode.SCD2_COMMON_ERROR.toErrorCode()) {
                throw new KylinException(RELOAD_TABLE_FAILED, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getRELOAD_TABLE_MODEL_RETRY(), tableName, columnNames, model.getAlias()));
            }
            throw e;
        }

        if (CollectionUtils.isNotEmpty(changeTypeAffected.getUpdatedLayouts())) {
            indexPlanService.reloadLayouts(projectName, changeTypeAffected.getModelId(),
                    changeTypeAffected.getUpdatedLayouts());
        }
        baseIndexUpdater.update(indexPlanService);
        if (CollectionUtils.isNotEmpty(removeAffected.getUpdatedLayouts())
                || CollectionUtils.isNotEmpty(changeTypeAffected.getUpdatedLayouts())) {
            val jobManager = getJobManager(projectName);
            if (needBuild) {
                return getSourceUsageManager().licenseCheckWrap(projectName,
                        () -> jobManager.addIndexJob(new JobParam(model.getId(), getUsername())));
            }
        }
        return null;
    }

    private void setRequest(ModelRequest request, NDataModel model, AffectedModelContext removeAffectedModel,
            AffectedModelContext changeTypeAffectedModel, String projectName) {
        request.setSimplifiedMeasures(model.getEffectiveMeasures().values().stream()
                .filter(m -> !removeAffectedModel.getMeasures().contains(m.getId())
                        && !changeTypeAffectedModel.getMeasures().contains(m.getId()))
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));

        request.setSimplifiedMeasures(request.getSimplifiedMeasures().stream().map(simplifiedMeasure -> {
            int measureId = simplifiedMeasure.getId();
            NDataModel.Measure updateMeasure = changeTypeAffectedModel.getUpdateMeasureMap().get(measureId);
            if (updateMeasure != null) {
                simplifiedMeasure = SimplifiedMeasure.fromMeasure(updateMeasure);
            }
            return simplifiedMeasure;
        }).collect(Collectors.toList()));

        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(col -> !removeAffectedModel.getDimensions().contains(col.getId()) && col.isDimension())
                .collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs().stream()
                .filter(cc -> !removeAffectedModel.getComputedColumns().contains(cc.getColumnName()))
                .collect(Collectors.toList()));

        // clean
        request.getAllNamedColumns().stream() //
                .filter(col -> removeAffectedModel.getColumns().contains(col.getId())) //
                .forEach(col -> col.setStatus(NDataModel.ColumnStatus.TOMB));
        String factTableAlias = model.getRootFactTable().getAlias();
        Set<String> ccFullNameSet = removeAffectedModel.getComputedColumns().stream() //
                .map(ccName -> factTableAlias + "." + ccName) //
                .collect(Collectors.toSet());
        request.getAllNamedColumns().stream() //
                .filter(col -> ccFullNameSet.contains(col.getAliasDotColumn())) //
                .forEach(col -> col.setStatus(NDataModel.ColumnStatus.TOMB));

        request.setProject(projectName);
    }

    void cleanIndexPlan(String projectName, NDataModel model, List<AffectedModelContext> affectedModels) {
        val indexManager = getIndexPlanManager(projectName);
        for (AffectedModelContext affectedContext : affectedModels) {
            if (!affectedContext.getUpdateMeasureMap().isEmpty()) {
                getDataModelManager(projectName).updateDataModel(model.getId(), modelDesc -> {
                    affectedContext.getUpdateMeasureMap().forEach((originalMeasureId, newMeasure) -> {
                        int maxMeasureId = modelDesc.getAllMeasures().stream().map(NDataModel.Measure::getId)
                                .mapToInt(i -> i).max().orElse(NDataModel.MEASURE_ID_BASE - 1);
                        Optional<NDataModel.Measure> originalMeasureOptional = modelDesc.getAllMeasures().stream()
                                .filter(measure -> measure.getId() == originalMeasureId).findAny();
                        if (originalMeasureOptional.isPresent()) {
                            NDataModel.Measure originalMeasure = originalMeasureOptional.get();
                            originalMeasure.setTomb(true);
                            maxMeasureId++;
                            newMeasure.setId(maxMeasureId);
                            modelDesc.getAllMeasures().add(newMeasure);
                        }
                    });
                });
            }
            indexManager.updateIndexPlan(model.getId(), affectedContext::shrinkIndexPlan);
        }
    }

    void mergeTable(String projectName, ReloadTableContext context, boolean keepTomb) {
        val tableManager = getTableManager(projectName);
        val originTable = tableManager.getTableDesc(context.getTableDesc().getIdentity());
        val originTableExt = tableManager.getTableExtIfExists(originTable);
        context.getTableDesc().setMvcc(originTable.getMvcc());
        if (originTableExt != null && keepTomb) {
            val validStats = originTableExt.getAllColumnStats().stream()
                    .filter(stats -> !context.getRemoveColumns().contains(stats.getColumnName()))
                    .collect(Collectors.toList());
            context.getTableExtDesc().setColumnStats(validStats);
            context.getTableExtDesc().setOriginalSize(originTableExt.getOriginalSize());

            val originCols = originTableExt.getAllColumnStats().stream().map(TableExtDesc.ColumnStats::getColumnName)
                    .collect(Collectors.toList());
            val indexMapping = Maps.<Integer, Integer> newHashMap();
            int index = 0;
            for (ColumnDesc column : context.getTableDesc().getColumns()) {
                int oldIndex = originCols.indexOf(column.getName());
                indexMapping.put(index, oldIndex);
                index++;
            }
            context.getTableExtDesc().setSampleRows(originTableExt.getSampleRows().stream().map(row -> {
                val result = new String[indexMapping.size()];
                indexMapping.forEach((key, value) -> {
                    if (value != -1) {
                        result[key] = row[value];
                    } else {
                        result[key] = "";
                    }
                });
                return result;
            }).collect(Collectors.toList()));
            context.getTableExtDesc().setMvcc(originTable.getMvcc());
        }

        TableDesc loadDesc = context.getTableDesc();
        if (keepTomb) {
            val copy = tableManager.copyForWrite(originTable);
            val originColMap = Stream.of(copy.getColumns())
                    .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
            val newColMap = Stream.of(context.getTableDesc().getColumns())
                    .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
            for (String changedTypeColumn : context.getChangeTypeColumns()) {
                originColMap.put(changedTypeColumn, newColMap.get(changedTypeColumn));
            }
            for (String addColumn : context.getAddColumns()) {
                originColMap.put(addColumn, newColMap.get(addColumn));
            }
            copy.setColumns(originColMap.values().stream()
                    .sorted(Comparator.comparing(col -> Integer.parseInt(col.getId()))).toArray(ColumnDesc[]::new));
            loadDesc = copy;
        }
        int idx = 1;
        for (ColumnDesc column : loadDesc.getColumns()) {
            column.setId(idx + "");
            idx++;
        }
        cleanSnapshot(context, loadDesc, originTable, projectName);
        loadTableToProject(loadDesc, context.getTableExtDesc(), projectName);
    }

    void cleanSnapshot(ReloadTableContext context, TableDesc targetTable, TableDesc originTable, String projectName) {
        if (context.isChanged(originTable)) {
            val tableIdentity = targetTable.getIdentity();
            List<AbstractExecutable> stopJobs = stopAndGetSnapshotJobs(projectName, tableIdentity);
            if (!stopJobs.isEmpty() || targetTable.getLastSnapshotPath() != null) {
                targetTable.deleteSnapshot(true);
            } else {
                targetTable.copySnapshotFrom(originTable);
            }
        } else {
            targetTable.copySnapshotFrom(originTable);
        }
    }

    private void checkNewColumn(Graph<SchemaNode> graph, TableDesc newTableDesc, Set<String> addColumns) {
        Multimap<String, String> duplicatedColumns = getDuplicatedColumns(graph, newTableDesc, addColumns);
        if (Objects.nonNull(duplicatedColumns) && !duplicatedColumns.isEmpty()) {
            Map.Entry<String, String> entry = duplicatedColumns.entries().iterator().next();
            throw new KylinException(DUPLICATED_COLUMN_NAME,
                    MsgPicker.getMsg().getTABLE_RELOAD_ADD_COLUMN_EXIST(entry.getKey(), entry.getValue()));
        }
    }

    private Multimap<String, String> getDuplicatedColumns(Graph<SchemaNode> graph, TableDesc newTableDesc,
            Set<String> addColumns) {
        Multimap<String, String> duplicatedColumns = HashMultimap.create();
        List<SchemaNode> schemaNodes = graph.nodes().stream()
                .filter(schemaNode -> SchemaNodeType.MODEL_CC == schemaNode.getType())
                .filter(schemaNode -> addColumns.contains(schemaNode.getDetail().toUpperCase(Locale.ROOT)))
                .collect(Collectors.toList());
        for (SchemaNode schemaNode : schemaNodes) {
            NDataModel model = modelService.getModelByAlias(schemaNode.getSubject(), newTableDesc.getProject());
            if (newTableDesc.getIdentity().equals(model.getRootFactTableRef().getTableDesc().getIdentity())) {
                duplicatedColumns.put(newTableDesc.getIdentity(), schemaNode.getDetail());
            } else {
                for (JoinTableDesc joinTable : model.getJoinTables()) {
                    if (newTableDesc.getIdentity().equals(joinTable.getTableRef().getTableDesc().getIdentity())) {
                        duplicatedColumns.put(newTableDesc.getIdentity(), schemaNode.getDetail());
                        break;
                    }
                }
            }
        }
        return duplicatedColumns;
    }

    private void checkEffectedJobs(TableDesc newTableDesc) {
        List<String> targetSubjectList = getEffectedJobs(newTableDesc, JobInfoEnum.JOB_TARGET_SUBJECT);
        if (CollectionUtils.isNotEmpty(targetSubjectList)) {
            throw new KylinException(ON_GOING_JOB_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_RELOAD_HAVING_NOT_FINAL_JOB(),
                            StringUtils.join(targetSubjectList.iterator(), ",")));
        }
    }

    private Set<String> getEffectedJobIds(TableDesc newTableDesc) {
        return Sets.newHashSet(getEffectedJobs(newTableDesc, JobInfoEnum.JOB_ID));
    }

    private List<String> getEffectedJobs(TableDesc newTableDesc, JobInfoEnum jobInfoType) {
        val notFinalStateJobs = getExecutableManager(newTableDesc.getProject()).getAllExecutables().stream()
                .filter(job -> !job.getStatus().isFinalState()).collect(Collectors.toList());

        List<String> effectedJobs = Lists.newArrayList();
        for (AbstractExecutable job : notFinalStateJobs) {
            if (JobTypeEnum.TABLE_SAMPLING == job.getJobType()) {
                if (newTableDesc.getIdentity().equalsIgnoreCase(job.getTargetSubject())) {
                    effectedJobs.add(JobInfoEnum.JOB_ID == jobInfoType ? job.getId() : job.getTargetSubject());
                }
            } else {
                try {
                    NDataModel model = modelService.getModelById(job.getTargetSubject(), newTableDesc.getProject());
                    if (!model.isBroken() && model.getAllTables().stream().map(TableRef::getTableIdentity)
                            .anyMatch(s -> s.equalsIgnoreCase(newTableDesc.getIdentity()))) {
                        effectedJobs.add(JobInfoEnum.JOB_ID == jobInfoType ? job.getId() : model.getAlias());
                    }
                } catch (KylinException e) {
                    logger.warn("Get model by Job target subject failed!", e);
                }
            }
        }
        return effectedJobs;
    }

    private ReloadTableContext calcReloadContext(String project, String tableIdentity, boolean failFast)
            throws Exception {
        val context = new ReloadTableContext();
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);
        val tableMeta = ugi.doAs(new PrivilegedExceptionAction<Pair<TableDesc, TableExtDesc>>() {
            @Override
            public Pair<TableDesc, TableExtDesc> run() throws Exception {
                return extractTableMeta(new String[] { tableIdentity }, project).get(0);
            }
        });
        val newTableDesc = new TableDesc(tableMeta.getFirst());
        context.setTableDesc(newTableDesc);
        context.setTableExtDesc(tableMeta.getSecond());

        val originTableDesc = getTableManager(project).getTableDesc(tableIdentity);
        val collector = Collectors.toMap(ColumnDesc::getName, col -> Pair.newPair(col.getName(), col.getDatatype()));
        val originCols = Stream.of(originTableDesc.getColumns()).collect(collector);
        val newCols = Stream.of(newTableDesc.getColumns()).collect(collector);

        val diff = Maps.difference(newCols, originCols);
        context.setAddColumns(diff.entriesOnlyOnLeft().keySet());
        context.setRemoveColumns(diff.entriesOnlyOnRight().keySet());
        context.setChangeTypeColumns(diff.entriesDiffering().keySet());

        val dependencyGraph = SchemaUtil.dependencyGraph(project);

        if (failFast) {
            checkNewColumn(dependencyGraph, newTableDesc, Sets.newHashSet(context.getAddColumns()));
            checkEffectedJobs(newTableDesc);
        } else {
            Set<String> duplicatedColumnsSet = Sets.newHashSet();
            Multimap<String, String> duplicatedColumns = getDuplicatedColumns(dependencyGraph, newTableDesc,
                    Sets.newHashSet(context.getAddColumns()));
            for (Map.Entry<String, String> entry : duplicatedColumns.entries()) {
                duplicatedColumnsSet.add(entry.getKey() + "." + entry.getValue());
            }
            context.setDuplicatedColumns(duplicatedColumnsSet);
            context.setEffectedJobs(getEffectedJobIds(newTableDesc));
        }

        Map<String, Set<Pair<NDataModel.Measure, NDataModel.Measure>>> suitableColumnTypeChangedMeasuresMap = getSuitableColumnTypeChangedMeasures(
                dependencyGraph, project, originTableDesc, diff.entriesDiffering());

        BiFunction<Set<String>, Boolean, Map<String, AffectedModelContext>> toAffectedModels = (cols, isDelete) -> {
            Set<SchemaNode> affectedNodes = Sets.newHashSet();
            val columnMap = Arrays.stream(originTableDesc.getColumns())
                    .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
            cols.forEach(colName -> {
                if (columnMap.get(colName) != null) {
                    affectedNodes.addAll(
                            Graphs.reachableNodes(dependencyGraph, SchemaNode.ofTableColumn(columnMap.get(colName))));
                }
            });
            val nodesMap = affectedNodes.stream().filter(SchemaNode::isModelNode)
                    .collect(Collectors.groupingBy(SchemaNode::getSubject, Collectors.toSet()));
            Map<String, AffectedModelContext> modelContexts = Maps.newHashMap();
            nodesMap.forEach((key, nodes) -> {
                val indexPlan = getIndexPlanManager(project).getIndexPlanByModelAlias(key);
                Set<Pair<NDataModel.Measure, NDataModel.Measure>> updateMeasures = Sets.newHashSet();
                if (!isDelete) {
                    updateMeasures = suitableColumnTypeChangedMeasuresMap.getOrDefault(key, updateMeasures);
                }

                val modelContext = new AffectedModelContext(project, indexPlan, nodes, updateMeasures, isDelete);
                modelContexts.put(indexPlan.getUuid(), modelContext);
            });
            return modelContexts;
        };
        context.setRemoveAffectedModels(toAffectedModels.apply(context.getRemoveColumns(), true));
        context.setChangeTypeAffectedModels(toAffectedModels.apply(context.getChangeTypeColumns(), false));

        return context;
    }

    /**
     * get suitable measures when column type change
     * and remove old measure add new measure with suitable function return type
     * @param dependencyGraph
     * @param project
     * @param tableDesc
     * @param changeTypeDifference
     * @return
     */
    private Map<String, Set<Pair<NDataModel.Measure, NDataModel.Measure>>> getSuitableColumnTypeChangedMeasures(
            Graph<SchemaNode> dependencyGraph, String project, TableDesc tableDesc,
            Map<String, MapDifference.ValueDifference<Pair<String, String>>> changeTypeDifference) {
        Map<String, Set<Pair<NDataModel.Measure, NDataModel.Measure>>> result = Maps.newHashMap();

        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        val columnMap = Arrays.stream(tableDesc.getColumns())
                .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
        for (MapDifference.ValueDifference<Pair<String, String>> value : changeTypeDifference.values()) {
            Graphs.reachableNodes(dependencyGraph,
                    SchemaNode.ofTableColumn(columnMap.get(value.leftValue().getFirst()))).stream()
                    .filter(node -> node.getType() == SchemaNodeType.MODEL_MEASURE).forEach(node -> {
                        String modelAlias = node.getSubject();
                        String measureId = node.getDetail();

                        NDataModel modelDesc = dataModelManager.getDataModelDescByAlias(modelAlias);
                        if (modelDesc != null) {
                            NDataModel.Measure measure = modelDesc.getEffectiveMeasures()
                                    .get(Integer.parseInt(measureId));
                            if (measure != null) {
                                int originalMeasureId = measure.getId();

                                FunctionDesc originalFunction = measure.getFunction();

                                String newColumnType = value.leftValue().getSecond();

                                boolean datatypeSuitable = originalFunction
                                        .isDatatypeSuitable(DataType.getType(newColumnType));

                                if (datatypeSuitable) {
                                    // update measure function's return type
                                    FunctionDesc newFunction = FunctionDesc.newInstance(
                                            originalFunction.getExpression(),
                                            Lists.newArrayList(originalFunction.getParameters()),
                                            FunctionDesc.proposeReturnType(originalFunction.getExpression(),
                                                    newColumnType));

                                    NDataModel.Measure newMeasure = new NDataModel.Measure();

                                    newMeasure.setFunction(newFunction);
                                    newMeasure.setName(measure.getName());
                                    newMeasure.setColumn(measure.getColumn());
                                    newMeasure.setComment(measure.getComment());

                                    Set<Pair<NDataModel.Measure, NDataModel.Measure>> measureList = result
                                            .getOrDefault(modelAlias, new HashSet<>());

                                    measureList.add(Pair.newPair(measure, newMeasure));

                                    result.put(modelAlias, measureList);
                                }
                            }
                        }
                    });
        }
        return result;
    }

    boolean isSqlContainsColumns(String sql, String reloadTable, Set<String> cols) {
        if (sql == null) {
            sql = "";
        }
        sql = sql.toUpperCase(Locale.ROOT);
        if (reloadTable.contains(".")) {
            reloadTable = reloadTable.split("\\.")[1];
        }
        for (String col : cols) {
            col = col.toUpperCase(Locale.ROOT);
            String colWithTableName = reloadTable + "." + col;
            if (sql.contains(colWithTableName) || !sql.contains("." + col) && sql.contains(col)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> getLoadedDatabases(String project) {
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager tableManager = getTableManager(project);
        List<TableDesc> tables = tableManager.listAllTables();
        Set<String> loadedDatabases = new HashSet<>();
        for (TableDesc table : tables) {
            loadedDatabases.add(table.getDatabase());
        }
        return loadedDatabases;
    }

    public interface ProjectTablesFilter {
        List process(String database, String table) throws Exception;
    }

    public NInitTablesResponse getProjectTables(String project, String table, Integer offset, Integer limit,
            Boolean useHiveDatabase, ProjectTablesFilter projectTablesFilter) throws Exception {
        aclEvaluate.checkProjectReadPermission(project);
        NInitTablesResponse response = new NInitTablesResponse();
        if (table == null)
            table = "";
        String exceptDatabase = null;
        if (table.contains(".")) {
            exceptDatabase = table.split("\\.", 2)[0].trim();
            table = table.split("\\.", 2)[1].trim();
        }
        Collection<String> databases = useHiveDatabase ? getSourceDbNames(project) : getLoadedDatabases(project);
        for (String database : databases) {
            if (exceptDatabase != null && !exceptDatabase.equalsIgnoreCase(database)) {
                continue;
            }
            List<?> tables;
            if (exceptDatabase == null && database.toLowerCase(Locale.ROOT).contains(table.toLowerCase(Locale.ROOT))) {
                tables = projectTablesFilter.process(database, "");
            } else {
                tables = projectTablesFilter.process(database, table);
            }
            List<?> tablePage = PagingUtil.cutPage(tables, offset, limit);
            if (!tablePage.isEmpty()) {
                response.putDatabase(database, tables.size(), tablePage);
            }
        }
        return response;
    }

    public Pair<String[], Set<String>> classifyDbTables(String project, String[] tables) throws Exception {
        HashMap<String, Set<String>> map = new HashMap<>();
        Set<String> dbs = new HashSet<>(getSourceDbNames(project));
        List<String> existed = new ArrayList<>();
        Set<String> failed = new HashSet<>();
        for (String str : tables) {
            String db = null;
            String table = null;
            if (str.contains(".")) {
                db = str.split("\\.", 2)[0].trim().toUpperCase(Locale.ROOT);
                table = str.split("\\.", 2)[1].trim().toUpperCase(Locale.ROOT);
            } else {
                db = str.toUpperCase(Locale.ROOT);
            }
            if (!dbs.contains(db)) {
                failed.add(str);
                continue;
            }
            if (table != null) {
                Set<String> tbs = map.get(db);
                if (tbs == null) {
                    tbs = new HashSet<>(getSourceTableNames(project, db, null));
                    map.put(db, tbs);
                }
                if (!tbs.contains(table)) {
                    failed.add(str);
                    continue;
                }
            }
            existed.add(str);
        }
        return new Pair<>(existed.toArray(new String[0]), failed);
    }

    public List<TableNameResponse> getHiveTableNameResponses(String project, String database, final String table)
            throws Exception {
        if (Boolean.TRUE.equals(KylinConfig.getInstanceFromEnv().getLoadHiveTablenameEnabled())) {
            return getTableNameResponsesInCache(project, database, table);
        } else {
            return getTableNameResponses(project, database, table);
        }
    }

    public List<TableNameResponse> getTableNameResponsesInCache(String project, String database, final String table)
            throws Exception {
        aclEvaluate.checkProjectReadPermission(project);
        List<TableNameResponse> responses = new ArrayList<>();
        NTableMetadataManager tableManager = getTableManager(project);
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);
        List<String> tables = NHiveTableName.getInstance().getTables(ugi, project, database);
        for (String tableName : tables) {
            if (StringUtils.isEmpty(table)
                    || tableName.toUpperCase(Locale.ROOT).contains(table.toUpperCase(Locale.ROOT))) {
                TableNameResponse response = new TableNameResponse();
                String tableNameWithDB = String.format(Locale.ROOT, "%s.%s", database, tableName);
                checkTableExistOrLoad(response, tableManager.getTableDesc(tableNameWithDB));
                response.setTableName(tableName);
                responses.add(response);
            }
        }
        return responses;
    }

    public void checkTableExistOrLoad(TableNameResponse response, TableDesc tableDesc) {
        if (Objects.isNull(tableDesc)) {
            return;
        }
        // Table name already exists
        if (Objects.nonNull(tableDesc.getKafkaConfig())) {
            // The Kafka table has the same table name, A table with the same name already exists
            response.setExisted(true);
        } else {
            // The Hive table has the same table name, A table with the same name has been loaded
            response.setLoaded(true);
        }
    }

    public void loadHiveTableNameToCache() throws Exception {
        NHiveTableName.getInstance().loadHiveTableName();
    }

    public NHiveTableNameResponse loadProjectHiveTableNameToCacheImmediately(String project, boolean force)
            throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);
        return NHiveTableName.getInstance().fetchTablesImmediately(ugi, project, force);
    }

    private static final String SSB_ERROR_MSG = "import ssb data error.";

    public void importSSBDataBase() {
        aclEvaluate.checkIsGlobalAdmin();
        if (checkSSBDataBase()) {
            return;
        }
        synchronized (TableService.class) {
            if (checkSSBDataBase()) {
                return;
            }
            CliCommandExecutor exec = new CliCommandExecutor();
            val patternedLogger = new BufferedLogger(logger);
            val sampleSh = checkSSBEnv();
            try {
                exec.execute(sampleSh, patternedLogger);
            } catch (ShellException e) {
                logger.error(SSB_ERROR_MSG, e);
                throw new KylinException(FAILED_IMPORT_SSB_DATA, SSB_ERROR_MSG, e);
            }
            if (!checkSSBDataBase()) {
                throw new KylinException(FAILED_IMPORT_SSB_DATA, SSB_ERROR_MSG);
            }
        }
    }

    private String checkSSBEnv() {
        var home = KylinConfigBase.getKylinHome();
        if (!StringUtils.isEmpty(home) && !home.endsWith("/")) {
            home = home + "/";
        }
        val sampleSh = String.format(Locale.ROOT, "%sbin/sample.sh", home);
        checkFile(sampleSh);
        val ssbSh = String.format(Locale.ROOT, "%stool/ssb/create_sample_ssb_tables.sql", home);
        checkFile(ssbSh);
        val customer = String.format(Locale.ROOT, "%stool/ssb/data/SSB.CUSTOMER.csv", home);
        checkFile(customer);
        val dates = String.format(Locale.ROOT, "%stool/ssb/data/SSB.DATES.csv", home);
        checkFile(dates);
        val lineorder = String.format(Locale.ROOT, "%stool/ssb/data/SSB.LINEORDER.csv", home);
        checkFile(lineorder);
        val part = String.format(Locale.ROOT, "%stool/ssb/data/SSB.PART.csv", home);
        checkFile(part);
        val supplier = String.format(Locale.ROOT, "%stool/ssb/data/SSB.SUPPLIER.csv", home);
        checkFile(supplier);
        return sampleSh;
    }

    private void checkFile(String fileName) {
        File file = new File(fileName);
        if (!file.exists() || !file.isFile()) {
            throw new KylinException(FILE_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getFILE_NOT_EXIST(), fileName));
        }
    }

    public boolean checkSSBDataBase() {
        aclEvaluate.checkIsGlobalAdmin();
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return true;
        }
        ISourceMetadataExplorer explr = SourceFactory.getSparkSource().getSourceMetadataExplorer();
        try {
            val result = explr.listTables("SSB").stream().map(str -> str.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet());
            return result
                    .containsAll(Sets.newHashSet("CUSTOMER", "DATES", "LINEORDER", "P_LINEORDER", "PART", "SUPPLIER"));
        } catch (Exception e) {
            logger.warn("check ssb error", e);
            return false;
        }
    }

    public TableRefresh refreshSingleCatalogCache(Map refreshRequest) {
        TableRefresh result = new TableRefresh();
        val message = MsgPicker.getMsg();
        List<String> tables = (List<String>) refreshRequest.get("tables");
        List<String> refreshed = Lists.newArrayList();
        List<String> failed = Lists.newArrayList();
        tables.forEach(table -> refreshTable(table, refreshed, failed));

        if (failed.isEmpty()) {
            result.setCode(KylinException.CODE_SUCCESS);
        } else {
            result.setCode(KylinException.CODE_UNDEFINED);
            result.setMsg(message.getTABLE_REFRESH_NOTFOUND());
        }
        result.setRefreshed(refreshed);
        result.setFailed(failed);
        return result;
    }

    public void refreshTable(String table, List<String> refreshed, List<String> failed) {
        try {
            PushDownUtil.trySimplePushDownExecute("REFRESH TABLE " + table, null);
            refreshed.add(table);
        } catch (Exception e) {
            failed.add(table);
            logger.error("fail to refresh spark cached table", e);
        }
    }

    public TableRefreshAll refreshAllCatalogCache(final HttpServletRequest request) {

        val message = MsgPicker.getMsg();
        List<ServerInfoResponse> servers = clusterManager.getQueryServers();
        TableRefreshAll result = new TableRefreshAll();
        result.setCode(KylinException.CODE_SUCCESS);
        StringBuilder msg = new StringBuilder();
        List<TableRefresh> nodes = new ArrayList<>();

        servers.forEach(server -> {
            String host = server.getHost();
            String url = "http://" + host + REFRESH_SINGLE_CATALOG_PATH;
            try {
                EnvelopeResponse response = generateTaskForRemoteHost(request, url);
                if (StringUtils.isNotBlank(response.getMsg())) {
                    msg.append(host + ":" + response.getMsg() + ";");
                }
                if (response.getCode().equals(KylinException.CODE_UNDEFINED)) {
                    result.setCode(KylinException.CODE_UNDEFINED);
                }
                if (response.getData() != null) {
                    TableRefresh data = JsonUtil.convert(response.getData(), TableRefresh.class);
                    data.setServer(host);
                    nodes.add(data);
                }
            } catch (Exception e) {
                throw new KylinException(FAILED_REFRESH_CATALOG_CACHE, message.getTABLE_REFRESH_ERROR(), e);
            }
        });
        if (!nodes.isEmpty()) {
            result.setNodes(nodes);
        }
        result.setMsg(msg.toString());
        return result;
    }

    public List<TableDesc> getTablesOfModel(String project, String modelAlias) {
        aclEvaluate.checkProjectReadPermission(project);
        NDataModel model = getDataModelManager(project).getDataModelDescByAlias(modelAlias);
        if (Objects.isNull(model)) {
            throw new KylinException(MODEL_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getMODEL_NOT_FOUND(), modelAlias));
        }
        List<String> usedTableNames = Lists.newArrayList();
        usedTableNames.add(model.getRootFactTableName());
        usedTableNames.addAll(model.getJoinTables().stream().map(JoinTableDesc::getTable).collect(Collectors.toList()));
        return usedTableNames.stream().map(getTableManager(project)::getTableDesc).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public TableExtDesc getOrCreateTableExt(String project, TableDesc t) {
        return getTableManager(project).getOrCreateTableExt(t);
    }
}
