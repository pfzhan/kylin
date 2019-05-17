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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.NRuleBasedIndex;
import io.kyligence.kap.metadata.cube.model.NSegmentConfigHelper;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ReloadTableAffectedModelContext;
import io.kyligence.kap.rest.request.ReloadTableContext;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.BatchLoadTableResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.PreReloadTableResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import io.netty.util.internal.ThrowableUtil;
import lombok.val;
import lombok.var;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    private static Message msg = MsgPicker.getMsg();

    @Autowired
    private ModelService modelService;

    @Autowired
    private ModelSemanticHelper semanticHelper;

    @Autowired
    private FavoriteQueryService favoriteQueryService;

    public List<TableDesc> getTableDesc(String project, boolean withExt, final String tableName, final String database,
            boolean isFuzzy) throws IOException {
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        List<TableDesc> tables = new ArrayList<>();
        //get table not fuzzy,can use getTableDesc(tableName)
        if (StringUtils.isNotEmpty(tableName) && !isFuzzy) {
            tables.add(nTableMetadataManager.getTableDesc(database + "." + tableName));
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
                return tableDesc.getName().toLowerCase().contains(tableName.toLowerCase());
            }).sorted(this::compareTableDesc).collect(Collectors.toList()));
        }
        tables = getTablesResponse(tables, project, withExt);
        return tables;
    }

    private int compareTableDesc(TableDesc table1, TableDesc table2) {
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
                }
                nTableExtDesc.init(project);

                tableMetaMgr.saveTableExt(nTableExtDesc);
            }

            saved.add(tableDesc.getIdentity());
            savedTables.add(tableDesc);
        }
        String[] result = saved.toArray(new String[saved.size()]);
        return result;
    }

    public List<Pair<TableDesc, TableExtDesc>> extractTableMeta(String[] tables, String project) throws Exception {
        // de-dup
        SetMultimap<String, String> databaseTables = LinkedHashMultimap.create();
        for (String fullTableName : tables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName);
            databaseTables.put(parts[0], parts[1]);
        }
        // load all tables first  Pair<TableDesc, TableExtDesc>
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance).getSourceMetadataExplorer();
        List<Pair<Map.Entry<String, String>, Object>> results = databaseTables.entries().parallelStream().map(entry -> {
            try {
                Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue(), project);
                TableDesc tableDesc = pair.getFirst();
                Preconditions.checkState(tableDesc.getDatabase().equalsIgnoreCase(entry.getKey()));
                Preconditions.checkState(tableDesc.getName().equalsIgnoreCase(entry.getValue()));
                Preconditions.checkState(tableDesc.getIdentity()
                        .equals(entry.getKey().toUpperCase() + "." + entry.getValue().toUpperCase()));
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
            String errorMessage = StringUtils.join(errorList.stream()
                    .map(error -> "table : " + error.getFirst().getKey() + "." + error.getFirst().getValue()
                            + " load Metadata error for exception:"
                            + ThrowableUtil.stackTraceToString((Throwable) error.getSecond()))
                    .collect(Collectors.toList()), "\n");
            throw new RuntimeException(errorMessage);
        }
        return results.stream().map(pair -> (Pair<TableDesc, TableExtDesc>) pair.getSecond())
                .collect(Collectors.toList());
    }

    public List<String> getSourceDbNames(String project) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listDatabases().stream().map(s -> s.toUpperCase()).collect(Collectors.toList());
    }

    public List<String> getSourceTableNames(String project, String database, final String table) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listTables(database).stream().filter(s -> {
            if (StringUtils.isEmpty(table)) {
                return true;
            } else {
                return s.toLowerCase().contains(table.toLowerCase());
            }
        }).map(String::toUpperCase).collect(Collectors.toList());
    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, final String table)
            throws Exception {
        List<TableNameResponse> tableNameResponses = new ArrayList<>();
        NTableMetadataManager tableManager = getTableManager(project);
        List<String> tables = getSourceTableNames(project, database, table);
        for (String tableName : tables) {
            TableNameResponse tableNameResponse = new TableNameResponse();
            tableNameResponse.setTableName(tableName);
            tableNameResponse.setLoaded(tableManager.getTableDesc(database + "." + tableName) != null);
            tableNameResponses.add(tableNameResponse);
        }
        return tableNameResponses;
    }

    private TableDescResponse getTableResponse(TableDesc table, String project) {
        TableDescResponse tableDescResponse = new TableDescResponse(table);
        TableExtDesc tableExtDesc = getTableManager(project).getTableExtIfExists(table);
        if (tableExtDesc == null) {
            return tableDescResponse;
        }
        // get TableDescResponse
        Map<String, Long> cardinality = new HashMap<>();
        Map<String, String> dataSourceProp = new HashMap<>();
        String cardinalityString = tableExtDesc.getCardinality();
        if (!StringUtils.isEmpty(cardinalityString)) {
            String[] cardinalities = StringUtils.split(cardinalityString, ",");
            ColumnDesc[] columnDescs = tableDescResponse.getColumns();
            for (int i = 0; i < columnDescs.length; i++) {
                ColumnDesc columnDesc = columnDescs[i];
                long card = i < cardinalities.length ? Long.parseLong(cardinalities[i]) : 0L;
                cardinality.put(columnDesc.getName(), card);
            }
            tableDescResponse.setCardinality(cardinality);
        }
        dataSourceProp.putAll(tableExtDesc.getDataSourceProps());
        tableDescResponse.setDescExd(dataSourceProp);
        return tableDescResponse;
    }

    private List<TableDesc> getTablesResponse(List<TableDesc> tables, String project, boolean withExt)
            throws IOException {
        List<TableDesc> descs = new ArrayList<>();
        val dataflowManager = getDataflowManager(project);
        for (val table : tables) {
            TableDescResponse rtableDesc;
            val models = dataflowManager.getModelsUsingRootTable(table);
            val modelsUsingTable = dataflowManager.getModelsUsingTable(table);
            if (withExt) {
                rtableDesc = getTableResponse(table, project);
            } else {
                rtableDesc = new TableDescResponse(table);
            }

            TableExtDesc tableExtDesc = getTableManager(project).getTableExtIfExists(table);
            if (tableExtDesc != null) {
                rtableDesc.setTotalRecords(tableExtDesc.getTotalRows());
            }

            if (CollectionUtils.isNotEmpty(models)) {
                rtableDesc.setRootFact(true);
                rtableDesc.setStorageSize(getStorageSize(project, models));
            } else if (CollectionUtils.isNotEmpty(modelsUsingTable)) {
                rtableDesc.setLookup(true);
                rtableDesc.setStorageSize(getSnapshotSize(project, modelsUsingTable, table.getIdentity()));
            }
            Pair<Set<String>, Set<String>> tableColumnType = getTableColumnType(table, project);
            NDataLoadingRange dataLoadingRange = getDataLoadingRangeManager(project)
                    .getDataLoadingRange(table.getIdentity());
            if (null != dataLoadingRange) {
                rtableDesc.setPartitionedColumn(dataLoadingRange.getColumnName());
                rtableDesc.setSegmentRange(dataLoadingRange.getCoveredRange());
            }
            rtableDesc.setForeignKey(tableColumnType.getSecond());
            rtableDesc.setPrimaryKey(tableColumnType.getFirst());
            descs.add(rtableDesc);
        }

        return descs;
    }

    private long getSnapshotSize(String project, List<NDataModel> modelsUsingTable, String table) throws IOException {
        val dfManager = getDataflowManager(project);
        var hasReadySegs = false;
        var size = 0;
        val df = dfManager.getDataflow(modelsUsingTable.get(0).getUuid());
        val lastSeg = df.getLatestReadySegment();
        if (lastSeg != null) {
            hasReadySegs = true;
            val snapShots = lastSeg.getSnapshots();
            if (snapShots.containsKey(table)) {
                FileSystem fs = HadoopUtil.getReadFileSystem();
                val path = new Path(snapShots.get(table));
                if (fs.exists(path)) {
                    ContentSummary cs = fs.getContentSummary(path);
                    size += cs.getLength();
                }
            }
        }
        if (!hasReadySegs) {
            return -1;
        } else {
            return size;
        }
    }

    private long getStorageSize(String project, List<NDataModel> models) {
        val dfManger = getDataflowManager(project);
        boolean hasReadySegs = false;
        long size = 0;
        for (val model : models) {
            val df = dfManger.getDataflow(model.getUuid());
            val readySegs = df.getSegments(SegmentStatusEnum.READY);
            if (CollectionUtils.isNotEmpty(readySegs)) {
                hasReadySegs = true;
                size += dfManger.getDataflowByteSize(model.getUuid());
            }
        }
        if (!hasReadySegs) {
            return -1;
        } else {
            return size;
        }
    }

    //get table's primaryKeys(pair first) and foreignKeys(pair second)
    private Pair<Set<String>, Set<String>> getTableColumnType(TableDesc table, String project) {
        val dataModelManager = getDataModelManager(project);
        val models = getDataflowManager(project).getModelsUsingTable(table);
        Set<String> primaryKey = new HashSet<>();
        Set<String> foreignKey = new HashSet<>();
        for (val model : models) {
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
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase();
    }

    @Transaction(project = 1)
    public void setPartitionKey(String table, String project, String column) {
        NTableMetadataManager tableManager = getTableManager(project);

        val dataflowManager = getDataflowManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        tableDesc = tableManager.copyForWrite(tableDesc);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (StringUtils.isEmpty(column)) {
            if (dataLoadingRange == null) {
                return;
            }
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
            tableDesc.setIncrementLoading(false);
            tableManager.updateTableDesc(tableDesc);
        } else {
            String tableName = table.substring(table.lastIndexOf('.') + 1);
            String columnIdentity = tableName + "." + column;
            modelService.checkSingleIncrementingLoadingTable(project, table);
            if (dataLoadingRange != null && dataLoadingRange.getColumnName().equals(columnIdentity))
                return;

            if (dataLoadingRange == null) {
                dataLoadingRange = new NDataLoadingRange(table, columnIdentity);
                dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
            } else {
                val copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
                copy.setPartitionDateFormat(null);
                copy.setColumnName(columnIdentity);
                dataLoadingRangeManager.updateDataLoadingRange(copy);
            }

            tableDesc.setIncrementLoading(true);
            tableManager.updateTableDesc(tableDesc);
        }

        // toggle table type, remove all segments in related models
        val models = dataflowManager.getTableOrientedModelsUsingRootTable(tableDesc);
        for (val model : models) {
            //follow semanticVersion,#8196
            modelService.purgeModel(model.getUuid(), project);
            modelService.syncPartitionDesc(model.getUuid(), project);
            if (StringUtils.isEmpty(column)) {
                buildFullSegment(model.getUuid(), project);
            } else {
                //await table's range being set in next REST call
            }
        }
    }

    private void buildFullSegment(String model, String project) {
        val eventManager = getEventManager(project);
        val dataflowManager = getDataflowManager(project);
        val indexPlanManager = getIndexPlanManager(project);
        val indexPlan = indexPlanManager.getIndexPlan(model);
        val dataflow = dataflowManager.getDataflow(indexPlan.getUuid());
        val newSegment = dataflowManager.appendSegment(dataflow,
                new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));

        val addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setSegmentId(newSegment.getId());
        addSegmentEvent.setModelId(model);
        addSegmentEvent.setJobId(UUID.randomUUID().toString());
        addSegmentEvent.setOwner(getUsername());
        eventManager.post(addSegmentEvent);

        PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setSegmentId(newSegment.getId());
        postAddSegmentEvent.setModelId(model);
        postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
        postAddSegmentEvent.setOwner(getUsername());
        eventManager.post(postAddSegmentEvent);
    }

    public void setDataRange(String project, DateRangeRequest dateRangeRequest) throws Exception {
        String table = dateRangeRequest.getTable();
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        SegmentRange allRange = dataLoadingRange.getCoveredRange();

        var start = dateRangeRequest.getStart();
        var end = dateRangeRequest.getEnd();

        Pair<String, String> pushdownResult;
        if (PushDownUtil.needPushdown(start, end)) {
            pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);
            start = PushDownUtil.calcStart(pushdownResult.getFirst(), allRange);
            end = pushdownResult.getSecond();
        }

        if (allRange != null && allRange.getEnd().toString().equals(end))
            throw new IllegalStateException("There is no more new data to load");

        String finalStart = start;
        String finalEnd = end;
        UnitOfWork.doInTransactionWithRetry(() -> {
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
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);
        val start = PushDownUtil.calcStart(pushdownResult.getFirst(), dataLoadingRange.getCoveredRange());
        return new ExistedDataRangeResponse(start, pushdownResult.getSecond());

    }

    public Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String table)
            throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        String partitionColumn = dataLoadingRange.getColumnName();

        val maxAndMinTime = PushDownUtil.getMaxAndMinTime(partitionColumn, table, project);
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

        UnitOfWork.doInTransactionWithRetry(() -> {
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
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
            for (var model : models) {
                val modelId = model.getUuid();
                IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project).getIndexPlan(modelId);
                NDataflow df = dataflowManager.getDataflow(indexPlan.getUuid());
                NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
                AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
                addSegmentEvent.setModelId(modelId);
                addSegmentEvent.setSegmentId((dataSegment.getId()));
                addSegmentEvent.setJobId(UUID.randomUUID().toString());
                addSegmentEvent.setOwner(getUsername());
                eventManager.post(addSegmentEvent);

                PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
                postAddSegmentEvent.setModelId(modelId);
                postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
                postAddSegmentEvent.setSegmentId((dataSegment.getId()));
                postAddSegmentEvent.setOwner(getUsername());
                eventManager.post(postAddSegmentEvent);

                logger.info(
                        "LoadingRangeUpdateHandler produce AddSegmentEvent project : {}, model : {}, segmentRange : {}",
                        project, modelId, segmentRange);
            }
        }
    }

    public SegmentRange getSegmentRangeByTable(DateRangeRequest dateRangeRequest) {
        String project = dateRangeRequest.getProject();
        String table = dateRangeRequest.getTable();
        NTableMetadataManager nProjectManager = getTableManager(project);
        TableDesc tableDesc = nProjectManager.getTableDesc(table);
        return SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(),
                dateRangeRequest.getEnd());

    }

    public List<BatchLoadTableResponse> getBatchLoadTables(String project) {
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
        for (DateRangeRequest request : requests) {
            setDataRange(project, request);
        }
    }

    @Transaction(project = 0)
    public void unloadTable(String project, String table) {
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        tableMetadataManager.removeTableExt(table);
        tableMetadataManager.removeSourceTable(table);

        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (dataLoadingRange != null) {
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
        }
    }

    @Transaction(project = 1)
    public void setTop(String table, String project, boolean top) {
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        TableDesc tableDesc = nTableMetadataManager.getTableDesc(table);
        tableDesc = nTableMetadataManager.copyForWrite(tableDesc);
        tableDesc.setTop(top);
        nTableMetadataManager.updateTableDesc(tableDesc);
    }

    public List<TablesAndColumnsResponse> getTableAndColumns(String project) {
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
            throw new BadRequestException("There is no ready segment to refresh!");
        }
        SegmentRange segmentRangeRefresh = SourceFactory.getSource(tableDesc).getSegmentRange(start, end);

        if (!readySegmentRange.contains(segmentRangeRefresh)) {
            throw new BadRequestException("Data during refresh range must be ready!");
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
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRangeUpdate = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        dataLoadingRangeUpdate.setPushdownRangeLimited(pushdownRangeLimited);
        dataLoadingRangeManager.updateDataLoadingRange(dataLoadingRangeUpdate);
    }

    public AutoMergeConfigResponse getAutoMergeConfigByModel(String project, String modelId) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        AutoMergeConfigResponse mergeConfig = new AutoMergeConfigResponse();

        NDataModel model = dataModelManager.getDataModelDesc(modelId);
        if (model == null) {
            throw new BadRequestException("Model " + modelId + " does not exist in project " + project);
        }
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        Preconditions.checkState(segmentConfig != null);
        mergeConfig.setAutoMergeEnabled(segmentConfig.getAutoMergeEnabled());
        mergeConfig.setAutoMergeTimeRanges(segmentConfig.getAutoMergeTimeRanges());
        mergeConfig.setVolatileRange(segmentConfig.getVolatileRange());
        return mergeConfig;
    }

    public AutoMergeConfigResponse getAutoMergeConfigByTable(String project, String tableName) {
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
            throw new IllegalStateException("Model " + modelId + "does not exist in project " + project);
        }
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
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
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        return dataLoadingRange.isPushdownRangeLimited();
    }

    @Transaction(project = 0)
    public void setAutoMergeConfigByTable(String project, AutoMergeRequest autoMergeRequest) {
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
    public PreReloadTableResponse preProcessBeforeReload(String project, String tableIdentity) throws Exception {
        val context = calcReloadContext(project, tableIdentity);

        val result = new PreReloadTableResponse();
        result.setAddColumnCount(context.getAddColumns().size());
        result.setRemoveColumnCount(context.getRemoveColumns().size());
        result.setRemoveDimCount(context.getRemoveAffectedModels().values().stream()
                .map(ReloadTableAffectedModelContext::getDimensions).mapToLong(Set::size).sum());
        result.setRemoveDimModelCount(
                context.getRemoveAffectedModels().values().stream().filter(m -> !m.getDimensions().isEmpty()).count());
        result.setRemoveMeasureCount(context.getRemoveAffectedModels().values().stream()
                .map(ReloadTableAffectedModelContext::getMeasures).mapToLong(Set::size).sum());
        result.setRemoveMeasureModelCount(
                context.getRemoveAffectedModels().values().stream().filter(m -> !m.getMeasures().isEmpty()).count());
        result.setBrokenFavoriteQueryCount(context.getFavoriteQueries().size());

        return result;
    }

    public void reloadTable(String projectName, String tableIdentity) throws Exception {
        val self = (TableService) AopContext.currentProxy();
        self.innerReloadTable(projectName, tableIdentity);
        modelService.reloadCache(projectName);
        favoriteQueryService.asyncAdjustFavoriteQuery();
    }

    @Transaction(project = 0)
    void innerReloadTable(String projectName, String tableIdentity) throws Exception {
        val dataflowManager = getDataflowManager(projectName);
        val tableManager = getTableManager(projectName);
        val originTable = tableManager.getTableDesc(tableIdentity);
        Preconditions.checkNotNull(originTable);

        val project = getProjectManager().getProject(projectName);
        val context = calcReloadContext(projectName, tableIdentity);
        mergeTable(projectName, context, true);
        for (val model : dataflowManager.listUnderliningDataModels()) {
            updateModelByReloadTable(project, model, context);
        }

        val fqManager = getFavoriteQueryManager(projectName);
        context.getFavoriteQueries().forEach(fqManager::delete);
        favoriteQueryService.asyncAdjustFavoriteQuery();

        val loadingManager = getDataLoadingRangeManager(projectName);
        val removeCols = context.getRemoveColumnFullnames();
        loadingManager.getDataLoadingRanges().forEach(loadingRange -> {
            if (removeCols.contains(loadingRange.getColumnName())) {
                setPartitionKey(tableIdentity, projectName, null);
            }
        });

        mergeTable(projectName, context, false);
    }

    void updateModelByReloadTable(ProjectInstance project, NDataModel model, ReloadTableContext context)
            throws Exception {
        val modelManager = getDataModelManager(project.getName());
        val indexManager = getIndexPlanManager(project.getName());
        val dataflowManager = getDataflowManager(project.getName());
        val projectName = project.getName();

        if (!context.getRemoveAffectedModels().containsKey(model.getId())
                && !context.getChangeTypeAffectedModels().containsKey(model.getId())) {
            return;
        }
        val removeAffectedModel = context.getRemoveAffectedModels().getOrDefault(model.getId(),
                new ReloadTableAffectedModelContext());
        val changeTypeAffectedModel = context.getChangeTypeAffectedModels().getOrDefault(model.getId(),
                new ReloadTableAffectedModelContext());
        val removeDims = removeAffectedModel.getDimensions();
        if (removeAffectedModel.isBroken()) {
            if (project.getMaintainModelType() == MaintainModelType.AUTO_MAINTAIN) {
                modelManager.dropModel(model);
                dataflowManager.dropDataflow(model.getId());
                indexManager.dropIndexPlan(model.getId());
            } else {
                val df = dataflowManager.getDataflow(model.getId());
                val dfUpdate = new NDataflowUpdate(df.getId());
                dfUpdate.setStatus(RealizationStatusEnum.BROKEN);
                dfUpdate.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
                dataflowManager.updateDataflow(dfUpdate);
            }
            return;
        }

        val eventDao = getEventDao(projectName);
        val events = eventDao.getEventsByModel(model.getId());

        cleanIndexPlan(projectName, model, removeAffectedModel);

        val request = new ModelRequest(JsonUtil.deepCopy(model, NDataModel.class));
        request.setSimplifiedMeasures(model.getEffectiveMeasures().values().stream()
                .filter(m -> !removeAffectedModel.getMeasures().contains(m.getId())).map(SimplifiedMeasure::fromMeasure)
                .collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream()
                .filter(col -> !removeDims.contains(col.getId()) && col.isDimension()).collect(Collectors.toList()));
        request.setComputedColumnDescs(model.getComputedColumnDescs().stream()
                .filter(cc -> !removeAffectedModel.getComputedColumns().contains(cc.getFullName()))
                .collect(Collectors.toList()));
        request.setProject(projectName);
        request.setColumnsFetcher((tableRef, isFilterCC) -> TableRef.filterColumns(
                tableRef.getIdentity().equals(context.getTableDesc().getIdentity()) ? context.getTableDesc() : tableRef,
                isFilterCC));

        modelService.updateDataModelSemantic(projectName, request);

        if (CollectionUtils.isNotEmpty(changeTypeAffectedModel.getLayouts())) {
            val eventManager = getEventManager(projectName);
            val df = dataflowManager.getDataflow(model.getId());
            dataflowManager.removeLayouts(df, changeTypeAffectedModel.getLayouts());
            AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
            addCuboidEvent.setModelId(model.getId());
            addCuboidEvent.setJobId(UUID.randomUUID().toString());
            addCuboidEvent.setOwner(getUsername());
            eventManager.post(addCuboidEvent);

            PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
            postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
            postAddCuboidEvent.setModelId(model.getId());
            postAddCuboidEvent.setOwner(getUsername());
            eventManager.post(postAddCuboidEvent);
        }

        cleanRedundantEvents(projectName, model, events);
    }

    void cleanIndexPlan(String projectName, NDataModel model, ReloadTableAffectedModelContext removeAffectedModel) {
        val indexManager = getIndexPlanManager(projectName);
        val removeDims = removeAffectedModel.getDimensions();
        val removeColumnIds = removeAffectedModel.getColumnIds();

        UnaryOperator<Integer[]> dimFilter = input -> Stream.of(input).filter(i -> !removeDims.contains(i))
                .toArray(Integer[]::new);
        val indexPlan = indexManager.getIndexPlan(model.getId());
        val removeIndexes = removeAffectedModel.getIndexes();
        val newIndexPlan = indexManager.updateIndexPlan(model.getId(), copyForWrite -> {
            copyForWrite.setIndexes(copyForWrite.getIndexes().stream()
                    .filter(index -> !removeIndexes.contains(index.getId())).collect(Collectors.toList()));

            val overrideEncodings = Maps.newHashMap(copyForWrite.getIndexPlanOverrideEncodings());
            removeColumnIds.forEach(overrideEncodings::remove);
            copyForWrite.setIndexPlanOverrideEncodings(overrideEncodings);

            val overrideIndexes = Maps.newHashMap(copyForWrite.getIndexPlanOverrideIndexes());
            removeColumnIds.forEach(overrideIndexes::remove);
            copyForWrite.setIndexPlanOverrideIndexes(overrideIndexes);

            if (copyForWrite.getDictionaries() != null) {
                copyForWrite.setDictionaries(copyForWrite.getDictionaries().stream()
                        .filter(d -> !removeColumnIds.contains(d.getId())).collect(Collectors.toList()));
            }

            if (copyForWrite.getRuleBasedIndex() == null) {
                return;
            }
            val rule = JsonUtil.deepCopyQuietly(copyForWrite.getRuleBasedIndex(), NRuleBasedIndex.class);
            rule.setLayoutIdMapping(Lists.newArrayList());
            rule.setDimensions(
                    rule.getDimensions().stream().filter(d -> !removeDims.contains(d)).collect(Collectors.toList()));
            rule.setMeasures(rule.getMeasures().stream().filter(m -> !removeAffectedModel.getMeasures().contains(m))
                    .collect(Collectors.toList()));
            rule.getAggregationGroups().forEach(group -> {
                group.setIncludes(dimFilter.apply(group.getIncludes()));
                group.getSelectRule().mandatoryDims = dimFilter.apply(group.getSelectRule().mandatoryDims);
                group.getSelectRule().hierarchyDims = Stream.of(group.getSelectRule().hierarchyDims).map(dimFilter)
                        .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
                group.getSelectRule().jointDims = Stream.of(group.getSelectRule().jointDims).map(dimFilter)
                        .filter(dims -> dims.length > 0).toArray(Integer[][]::new);
            });
            copyForWrite.setRuleBasedIndex(rule);
        });
        if (indexPlan.getRuleBasedIndex() != null) {
            semanticHelper.handleIndexPlanUpdateRule(projectName, model.getId(), indexPlan.getRuleBasedIndex(),
                    newIndexPlan.getRuleBasedIndex(), false);
        }

    }

    void cleanRedundantEvents(String projectName, NDataModel model, List<Event> existEvents) {
        val eventDao = getEventDao(projectName);
        val events = eventDao.getEventsByModel(model.getId());
        if (events.size() - existEvents.size() > 2) {
            events.stream().skip(existEvents.size() + 2).forEach(event -> eventDao.deleteEvent(event.getId()));
        }
    }

    void mergeTable(String projectName, ReloadTableContext context, boolean keepTomb) {
        val tableManager = getTableManager(projectName);
        val originTable = tableManager.getTableDesc(context.getTableDesc().getIdentity());
        val originTableExt = tableManager.getTableExtIfExists(originTable);
        context.getTableDesc().setMvcc(originTable.getMvcc());
        Optional.ofNullable(originTableExt)
                .ifPresent(ext -> context.getTableExtDesc().setMvcc(originTableExt.getMvcc()));

        TableDesc loadDesc = context.getTableDesc();
        if (keepTomb) {
            val copy = tableManager.copyForWrite(originTable);
            val originColMap = Stream.of(copy.getColumns())
                    .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
            val newColMap = Stream.of(context.getTableDesc().getColumns())
                    .collect(Collectors.toMap(ColumnDesc::getName, Function.identity()));
            for (String addColumn : context.getAddColumns()) {
                originColMap.put(addColumn, newColMap.get(addColumn));
            }
            copy.setColumns(originColMap.values().toArray(new ColumnDesc[0]));
            loadDesc = copy;
        }
        loadTableToProject(loadDesc, context.getTableExtDesc(), projectName);
    }

    @VisibleForTesting
    ReloadTableContext calcReloadContext(String project, String tableIdentity) throws Exception {
        val context = new ReloadTableContext();
        val tableMeta = extractTableMeta(new String[] { tableIdentity }, project).get(0);
        val newTableDesc = tableMeta.getFirst();
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

        val modelManager = getDataModelManager(project);
        val dataflowManager = getDataflowManager(project);
        for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
            val affectedModel = calcAffectedModel(project, model, context.getRemoveColumns(), tableIdentity);
            if (affectedModel == null) {
                continue;
            }
            context.getRemoveAffectedModels().put(model.getId(), affectedModel);
            val keyColumns = model.getJoinTables().stream().flatMap(join -> Stream
                    .concat(Stream.of(join.getJoin().getPrimaryKey()), Stream.of(join.getJoin().getForeignKey())))
                    .collect(Collectors.toSet());
            if (model.getPartitionDesc().getPartitionDateColumnRef() != null) {
                keyColumns.add(model.getPartitionDesc().getPartitionDateColumnRef().getIdentity());
            }
            if (model.getPartitionDesc().getPartitionTimeColumnRef() != null) {
                keyColumns.add(model.getPartitionDesc().getPartitionTimeColumnRef().getIdentity());
            }
            affectedModel.setBroken(!Sets.intersection(affectedModel.getColumns(), keyColumns).isEmpty());
        }
        for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
            val affectedModel = calcAffectedModel(project, model, context.getChangeTypeColumns(), tableIdentity);
            if (affectedModel == null) {
                continue;
            }
            context.getChangeTypeAffectedModels().put(model.getId(), affectedModel);
        }
        val fqManager = getFavoriteQueryManager(project);
        context.setFavoriteQueries(fqManager.getAll().stream().filter(fq -> fq.getRealizations().stream()
                .anyMatch(fqr -> context.getRemoveAffectedModels().containsKey(fqr.getModelId()) && context
                        .getRemoveAffectedModels().get(fqr.getModelId()).getLayouts().contains(fqr.getLayoutId())))
                .map(FavoriteQuery::getId).collect(Collectors.toSet()));

        return context;
    }

    ReloadTableAffectedModelContext calcAffectedModel(String project, NDataModel model, Set<String> changedColumns,
            String tableIdentity) {
        if (model.getAllTables().stream().noneMatch(ref -> ref.getTableIdentity().equalsIgnoreCase(tableIdentity))) {
            return null;
        }

        val modelAffectedColumns = model.getAliasMap().entrySet().stream()
                .filter(entry -> entry.getValue().getTableIdentity().equals(tableIdentity)) //
                .map(Map.Entry::getKey).flatMap(alias -> changedColumns.stream().map(col -> alias + "." + col)) //
                .collect(Collectors.toSet());
        val affectedComputedColumns = model.getComputedColumnDescs().stream()
                .filter(cc -> modelAffectedColumns.stream().anyMatch(col -> cc.getInnerExpression().contains(col)))
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toSet());

        modelAffectedColumns.addAll(affectedComputedColumns);

        val affectedColIds = model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist) //
                .filter(nc -> modelAffectedColumns.contains(nc.getAliasDotColumn())).map(NDataModel.NamedColumn::getId)
                .collect(Collectors.toSet());
        val affectedDims = model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension) //
                .filter(nc -> modelAffectedColumns.contains(nc.getAliasDotColumn())).map(NDataModel.NamedColumn::getId)
                .collect(Collectors.toSet());
        val affectedMeasures = model.getEffectiveMeasureMap().values().stream() //
                .filter(m -> m.getFunction().getColRefs().stream()
                        .anyMatch(colRef -> modelAffectedColumns.contains(colRef.getIdentity())))
                .map(NDataModel.Measure::getId).collect(Collectors.toSet());
        val affectedModel = new ReloadTableAffectedModelContext();

        affectedModel.setColumnIds(affectedColIds);
        affectedModel.setColumns(modelAffectedColumns);
        affectedModel.setComputedColumns(affectedComputedColumns);
        affectedModel.setDimensions(affectedDims);
        affectedModel.setMeasures(affectedMeasures);

        val indexManager = getIndexPlanManager(project);
        val indexPlan = indexManager.getIndexPlan(model.getId());
        val affectedLayouts = indexPlan.getAllIndexes().stream()
                .filter(index -> !Sets.intersection(index.getEffectiveDimCols().keySet(), affectedDims).isEmpty()
                        || !Sets.intersection(index.getEffectiveMeasures().keySet(), affectedMeasures).isEmpty())
                .flatMap(index -> index.getLayouts().stream()).map(LayoutEntity::getId).collect(Collectors.toSet());

        affectedModel.setLayouts(affectedLayouts);

        return affectedModel;
    }

    public Set<String> getLoadedDatabases(String project) {
        NTableMetadataManager tableManager = getTableManager(project);
        List<TableDesc> tables = tableManager.listAllTables();
        Set<String> loadedDatabases = new HashSet<>();
        for (TableDesc table : tables) {
            loadedDatabases.add(table.getDatabase());
        }
        return loadedDatabases;
    }

}
