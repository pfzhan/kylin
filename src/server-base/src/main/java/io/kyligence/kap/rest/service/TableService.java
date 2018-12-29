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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NSegmentConfigHelper;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;
import lombok.var;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    private static Message msg = MsgPicker.getMsg();

    @Autowired
    private ModelService modelService;

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
                NTableExtDesc nTableExtDesc = new NTableExtDesc(extDesc);
                if (origExt == null || origExt.getProject() == null) {
                    nTableExtDesc.setUuid(UUID.randomUUID().toString());
                    nTableExtDesc.setLastModified(0);
                } else {
                    nTableExtDesc.setUuid(origExt.getUuid());
                    nTableExtDesc.setLastModified(origExt.getLastModified());
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

    public List<Pair<TableDesc, TableExtDesc>> extractTableMeta(String[] tables, String project, int sourceType)
            throws Exception {
        // de-dup
        SetMultimap<String, String> databaseTables = LinkedHashMultimap.create();
        for (String fullTableName : tables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName);
            databaseTables.put(parts[0], parts[1]);
        }
        // load all tables first
        List<Pair<TableDesc, TableExtDesc>> allMeta = Lists.newArrayList();
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance).getSourceMetadataExplorer();
        for (Map.Entry<String, String> entry : databaseTables.entries()) {
            Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue(), project);
            TableDesc tableDesc = pair.getFirst();
            Preconditions.checkState(tableDesc.getDatabase().equals(entry.getKey().toUpperCase()));
            Preconditions.checkState(tableDesc.getName().equals(entry.getValue().toUpperCase()));
            Preconditions.checkState(tableDesc.getIdentity()
                    .equals(entry.getKey().toUpperCase() + "." + entry.getValue().toUpperCase()));
            TableExtDesc extDesc = pair.getSecond();
            Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getIdentity()));
            allMeta.add(pair);
        }
        return allMeta;
    }

    public List<String> getSourceDbNames(String project, int dataSourceType) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listDatabases();
    }

    public List<String> getSourceTableNames(String project, String database, int dataSourceType, final String table)
            throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listTables(database).stream().filter(s -> {
            if (StringUtils.isEmpty(table)) {
                return true;
            } else {
                return s.toLowerCase().contains(table.toLowerCase());
            }
        }).collect(Collectors.toList());
    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, int dataSourceType,
            final String table) throws Exception {
        List<TableNameResponse> tableNameResponses = new ArrayList<>();
        NTableMetadataManager tableManager = getTableManager(project);
        List<String> tables = getSourceTableNames(project, database, dataSourceType, table);
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
        Map<String, Long> cardinality = new HashMap<String, Long>();
        Map<String, String> dataSourceProp = new HashMap<>();
        String cardinalityString = tableExtDesc.getCardinality();
        if (!StringUtils.isEmpty(cardinalityString)) {
            String[] cardinalities = StringUtils.split(cardinalityString, ",");
            ColumnDesc[] columuDescs = tableDescResponse.getColumns();
            for (int i = 0; i < columuDescs.length; i++) {
                ColumnDesc columnDesc = columuDescs[i];
                long card = i < cardinalities.length ? Long.parseLong(cardinalities[i]) : 0L;
                cardinality.put(columnDesc.getName(), card);
            }
            tableDescResponse.setCardinality(cardinality);
        }
        dataSourceProp.putAll(tableExtDesc.getDataSourceProp());
        tableDescResponse.setDescExd(dataSourceProp);
        return tableDescResponse;
    }

    private List<TableDesc> getTablesResponse(List<TableDesc> tables, String project, boolean withExt) throws IOException {
        List<TableDesc> descs = new ArrayList<TableDesc>();
        NDataModelManager dataModelManager = getDataModelManager(project);
        for (val table : tables) {
            TableDescResponse rtableDesc;
            List<String> models = dataModelManager.getModelsUsingRootTable(table);
            List<String> modelsUsingTable = dataModelManager.getModelsUsingTable(table);
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
                rtableDesc.setSegmentRange(dataLoadingRange.getCoveredSegmentRange());
                rtableDesc.setActualQueryStart(dataLoadingRange.getActualQueryStart());
                rtableDesc.setActualQueryEnd(dataLoadingRange.getActualQueryEnd());
                SegmentRange segmentRange = dataLoadingRange.getCoveredReadySegmentRange();
                if (segmentRange != null) {
                    rtableDesc.setReadyStart(Long.parseLong(segmentRange.getStart().toString()));
                    rtableDesc.setReadyEnd(Long.parseLong(segmentRange.getEnd().toString()));

                }
            }
            rtableDesc.setForeignKey(tableColumnType.getSecond());
            rtableDesc.setPrimaryKey(tableColumnType.getFirst());
            descs.add(rtableDesc);
        }

        return descs;
    }

    private long getSnapshotSize(String project, List<String> modelsUsingTable, String table) throws IOException {
        val dfManager = getDataflowManager(project);
        var hasReadySegs = false;
        var size = 0;
        val df = dfManager.getDataflowByModelName(modelsUsingTable.get(0));
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


    private long getStorageSize(String project, List<String> models) {
        val dfManger = getDataflowManager(project);
        boolean hasReadySegs = false;
        long size = 0;
        for (val model : models) {
            val df = dfManger.getDataflowByModelName(model);
            val readySegs = df.getSegments(SegmentStatusEnum.READY);
            if (CollectionUtils.isNotEmpty(readySegs)) {
                hasReadySegs = true;
                size += dfManger.getDataflowByteSize(model);
            }
        }
        if (!hasReadySegs) {
            return -1;
        } else {
            return size;
        }
    }

    private Map<SegmentRange, SegmentStatusEnum> getSegmentRangesWithStatus(NDataLoadingRange dataLoadingRange) {
        Map<SegmentRange, SegmentStatusEnum> segmentRangeResult = new HashMap<>();
        List<SegmentRange> segmentRanges = dataLoadingRange.getSegmentRanges();
        for (int i = 0; i < segmentRanges.size(); i++) {
            if (i > dataLoadingRange.getWaterMarkStart() && i <= dataLoadingRange.getWaterMarkEnd()) {
                segmentRangeResult.put(segmentRanges.get(i), SegmentStatusEnum.READY);
            } else {
                segmentRangeResult.put(segmentRanges.get(i), SegmentStatusEnum.NEW);

            }
        }
        return segmentRangeResult;
    }

    //get table's primaryKeys(pair first) and foreignKeys(pari second)
    private Pair<Set<String>, Set<String>> getTableColumnType(TableDesc table, String project) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<String> models = dataModelManager.getModelsUsingTable(table);
        Set<String> primaryKey = new HashSet<>();
        Set<String> foreignKey = new HashSet<>();
        for (String model : models) {
            val joinTables = dataModelManager.getDataModelDesc(model).getJoinTables();
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

        val modelManager = getDataModelManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
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

        //toogle table type,remove all segments in related models
        val models = modelManager.getTableOrientedModelsUsingRootTable(tableDesc);
        for (val model : models) {
            //follow semanticVersion,#8196
            modelService.purgeModel(model, project);
            modelService.syncPartitionDesc(model, project);
            if (StringUtils.isEmpty(column)) {
                buildFullSegment(model, project);
            } else {
                //await table's range being set in next REST call
            }
        }
    }

    private void buildFullSegment(String model, String project) {
        val eventManager = getEventManager(project);
        val dataflowManager = getDataflowManager(project);
        val cubePlanManager = getCubePlanManager(project);
        val cubePlan = cubePlanManager.findMatchingCubePlan(model);
        val dataflow = dataflowManager.getDataflow(cubePlan.getName());
        val newSegment = dataflowManager.appendSegment(dataflow,
                new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));

        val addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setSegmentId(newSegment.getId());
        addSegmentEvent.setCubePlanName(cubePlan.getName());
        addSegmentEvent.setModelName(model);
        addSegmentEvent.setJobId(UUID.randomUUID().toString());
        addSegmentEvent.setOwner(getUsername());
        eventManager.post(addSegmentEvent);

        PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setSegmentId(newSegment.getId());
        postAddSegmentEvent.setCubePlanName(cubePlan.getName());
        postAddSegmentEvent.setModelName(model);
        postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
        postAddSegmentEvent.setOwner(getUsername());
        eventManager.post(postAddSegmentEvent);
    }

    @Transaction(project = 0)
    public void setDataRange(String project, DateRangeRequest dateRangeRequest) throws Exception {
        String table = dateRangeRequest.getTable();
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        SegmentRange readyRange = dataLoadingRange.getCoveredReadySegmentRange();
        SegmentRange allRange = dataLoadingRange.getCoveredSegmentRange();

        Pair<String, String> pushdownResult;
        if (needPushdown(dateRangeRequest.getStart(), dateRangeRequest.getEnd(), dataLoadingRange)) {
            pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);

            if (StringUtils.isEmpty(dateRangeRequest.getStart())) {
                if (allRange == null)
                    dateRangeRequest.setStart(pushdownResult.getFirst());
                else
                    dateRangeRequest.setStart(allRange.getEnd().toString());
            }

            if (StringUtils.isEmpty(dateRangeRequest.getEnd()))
                dateRangeRequest.setEnd(pushdownResult.getSecond());
        }

        if (StringUtils.isEmpty(dateRangeRequest.getStart())) {
            dateRangeRequest.setStart(dataLoadingRange.getCoveredSegmentRange().getEnd().toString());
        }

        if (allRange != null && allRange.getEnd().toString().equals(dateRangeRequest.getEnd()))
            throw new IllegalStateException("There is no more new data to load");

        // propose partition column date format if not exist
        proposeAndSaveDateFormatIfNotExist(project, table);

        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        SegmentRange newSegmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(),
                dateRangeRequest.getEnd());

        dataLoadingRange = getDataLoadingRange(project, table);
        dataLoadingRange = rangeManager.appendSegmentRange(dataLoadingRange, newSegmentRange);
        handleLoadingRangeUpdate(project, table, newSegmentRange);

        if (readyRange == null) {
            return;
        } else {
            NDataLoadingRange dataLoadingRangeUpdate = rangeManager.copyForWrite(dataLoadingRange);
            long start = newSegmentRange.getStart().compareTo(readyRange.getStart()) < 0
                    ? Long.parseLong(readyRange.getStart().toString())
                    : Long.parseLong(newSegmentRange.getStart().toString());
            long end = newSegmentRange.getEnd().compareTo(readyRange.getEnd()) < 0
                    ? Long.parseLong(newSegmentRange.getEnd().toString())
                    : Long.parseLong(readyRange.getEnd().toString());
            dataLoadingRangeUpdate.setActualQueryStart(start);
            dataLoadingRangeUpdate.setActualQueryEnd(end);
            rangeManager.updateDataLoadingRange(dataLoadingRangeUpdate);
        }

    }

    private boolean needPushdown(String start, String end, NDataLoadingRange dataLoadingRange) {
        if (StringUtils.isEmpty(start) && dataLoadingRange.getCoveredSegmentRange() == null)
            return true;

        if (StringUtils.isEmpty(end))
            return true;

        return false;
    }

    public ExistedDataRangeResponse getLatestDataRange(String project, String table) throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        String lastEnd;
        Pair<String, String> pushdownResult = getMaxAndMinTimeInPartitionColumnByPushdown(project, table);

        if (dataLoadingRange.getCoveredSegmentRange() != null)
            lastEnd = dataLoadingRange.getCoveredSegmentRange().getEnd().toString();
        else
            lastEnd = pushdownResult.getFirst();

        String currentMaxTime = pushdownResult.getSecond();

        return new ExistedDataRangeResponse(lastEnd, currentMaxTime);
    }



    public Pair<String, String> getMaxAndMinTimeInPartitionColumnByPushdown(String project, String table)
            throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        String partitionColumn = dataLoadingRange.getColumnName();
        String sql = String.format("select min(%s), max(%s) from %s", partitionColumn, partitionColumn, table);

        // pushdown
        List<List<String>> returnRows = PushDownUtil.trySimplePushDownSelectQuery(sql).getFirst();

        if (returnRows.size() == 0 || returnRows.get(0).get(0) == null || returnRows.get(0).get(1) == null)
            throw new BadRequestException(String.format("There are no data in table %s", table));

        String minTime = returnRows.get(0).get(0);
        String maxTime = returnRows.get(0).get(1);

        String dateFormat;
        if (StringUtils.isEmpty(dataLoadingRange.getPartitionDateFormat()))
            dateFormat = setPartitionColumnFormat(minTime, project, table);
        else
            dateFormat = dataLoadingRange.getPartitionDateFormat();

        return new Pair<>(DateFormat.getFormattedDate(minTime, dateFormat), DateFormat.getFormattedDate(maxTime, dateFormat));
    }

    @Transaction(project = 1)
    private String setPartitionColumnFormat(String time, String project, String table) {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);

        String format = DateFormat.proposeDateFormat(time);
        val copy = rangeManager.copyForWrite(dataLoadingRange);
        copy.setPartitionDateFormat(format);
        rangeManager.updateDataLoadingRange(copy);

        // sync to all related models
        NDataModelManager modelManager = getDataModelManager(project);
        TableDesc tableDesc = getTableManager(project).getTableDesc(table);
        val models = modelManager.getTableOrientedModelsUsingRootTable(tableDesc);
        for (val model : models) {
            modelService.syncPartitionDesc(model, project);
        }

        return format;
    }

    private void proposeAndSaveDateFormatIfNotExist(String project, String table) throws Exception {
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        if (StringUtils.isNotEmpty(dataLoadingRange.getPartitionDateFormat()))
            return;

        String partitionColumn = dataLoadingRange.getColumnName();

        String sql = String.format("select %s from %s where %s is not null limit 1", partitionColumn, table, partitionColumn);

        // push down
        List<List<String>> returnRows = PushDownUtil.trySimplePushDownSelectQuery(sql).getFirst();
        if (returnRows.size() == 0)
            throw new BadRequestException(String.format("There are no data in table %s", table));

        setPartitionColumnFormat(returnRows.get(0).get(0), project, table);
    }

    private void handleLoadingRangeUpdate(String project, String tableName, SegmentRange segmentRange)
            throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(tableName);
        if (tableDesc == null) {
            throw new IllegalArgumentException("TableDesc '" + tableName + "' does not exist");
        }
        List<String> modelNames = NDataModelManager.getInstance(kylinConfig, project)
                .getTableOrientedModelsUsingRootTable(tableDesc);
        if (CollectionUtils.isNotEmpty(modelNames)) {
            EventManager eventManager = EventManager.getInstance(kylinConfig, project);
            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
            for (String modelName : modelNames) {

                NCubePlan cubePlan = NCubePlanManager.getInstance(kylinConfig, project).findMatchingCubePlan(modelName);
                NDataflow df = dataflowManager.getDataflow(cubePlan.getName());
                NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
                AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
                addSegmentEvent.setModelName(modelName);
                addSegmentEvent.setCubePlanName(cubePlan.getName());
                addSegmentEvent.setSegmentId((dataSegment.getId()));
                addSegmentEvent.setJobId(UUID.randomUUID().toString());
                addSegmentEvent.setOwner(getUsername());
                eventManager.post(addSegmentEvent);

                PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
                postAddSegmentEvent.setCubePlanName(cubePlan.getName());
                postAddSegmentEvent.setModelName(modelName);
                postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
                postAddSegmentEvent.setSegmentId((dataSegment.getId()));
                postAddSegmentEvent.setOwner(getUsername());
                eventManager.post(postAddSegmentEvent);

                logger.info(
                        "LoadingRangeUpdateHandler produce AddSegmentEvent project : {}, model : {}, segmentRange : {}",
                        project, modelName, segmentRange);
            }
        } else {
            // there is no models, just update the dataLoadingRange waterMark
            NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig,
                    project);
            dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);
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

    @Transaction(project = 0)
    public void unloadTable(String project, String table) {
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        tableMetadataManager.removeTableExt(table);
        tableMetadataManager.removeSourceTable(table);
    }

    @Transaction(project = 1)
    public void setTop(String table, String project, boolean top) {
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        TableDesc tableDesc = nTableMetadataManager.getTableDesc(table);
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
        SegmentRange readySegmentRange = dataLoadingRange.getCoveredReadySegmentRange();
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

    public AutoMergeConfigResponse getAutoMergeConfigByModel(String project, String modelName) {
        NDataModelManager dataModelManager = getDataModelManager(project);
        AutoMergeConfigResponse mergeConfig = new AutoMergeConfigResponse();

        NDataModel model = dataModelManager.getDataModelDesc(modelName);
        if (model == null) {
            throw new BadRequestException("Model " + modelName + " does not exist in project " + project);
        }
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelName);
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
        String modelName = autoMergeRequest.getModel();
        NDataModelManager dataModelManager = getDataModelManager(project);
        List<AutoMergeTimeEnum> autoMergeRanges = new ArrayList<>();
        for (String range : autoMergeRequest.getAutoMergeTimeRanges()) {
            autoMergeRanges.add(AutoMergeTimeEnum.valueOf(range));
        }
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.valueOf(autoMergeRequest.getVolatileRangeType()));
        volatileRange.setVolatileRangeEnabled(autoMergeRequest.isVolatileRangeEnabled());
        volatileRange.setVolatileRangeNumber(autoMergeRequest.getVolatileRangeNumber());

        NDataModel model = dataModelManager.getDataModelDesc(modelName);
        if (model == null) {
            throw new IllegalStateException("Model " + modelName + "does not exist in project " + project);
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
