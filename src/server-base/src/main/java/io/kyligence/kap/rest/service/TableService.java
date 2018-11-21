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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;

import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableDesc;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.LoadingRangeUpdateEvent;
import lombok.val;
import lombok.var;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
            tables.addAll(Lists.newArrayList(
                    FluentIterable.from(nTableMetadataManager.listAllTables()).filter(new Predicate<TableDesc>() {
                        @Override
                        public boolean apply(TableDesc tableDesc) {
                            if (StringUtils.isEmpty(database)) {
                                return true;
                            }
                            return tableDesc.getDatabase().equalsIgnoreCase(database);
                        }
                    }).filter(new Predicate<TableDesc>() {
                        @Override
                        public boolean apply(TableDesc tableDesc) {
                            if (StringUtils.isEmpty(tableName)) {
                                return true;
                            }
                            return tableDesc.getName().toLowerCase().contains(tableName.toLowerCase());
                        }
                    }).toSortedList(new Comparator<TableDesc>() {
                        @Override
                        public int compare(TableDesc o1, TableDesc o2) {
                            return compareTableDesc(o1, o2);
                        }
                    })));
        }
        tables = getTablesResponse(tables, project, withExt);
        return tables;
    }

    private int compareTableDesc(TableDesc table1, TableDesc table2) {
        if (!(table1.isTop() ^ table2.isTop())) {
            if (!(table1.getFact() ^ table2.getFact())) {
                return table1.getName().compareToIgnoreCase(table2.getName());
            } else {
                return table1.getFact() && !table2.getFact() ? -1 : 1;
            }
        } else {
            return table1.isTop() && !table2.isTop() ? -1 : 1;
        }
    }

    public String[] loadTableToProject(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {
        return loadTablesToProject(Lists.newArrayList(Pair.newPair(tableDesc, extDesc)), project);
    }

    private String[] loadTablesToProject(List<Pair<TableDesc, TableExtDesc>> allMeta, String project)
            throws IOException {
        final NTableMetadataManager tableMetaMgr = getTableManager(project);
        // save table meta
        List<String> saved = Lists.newArrayList();
        List<TableDesc> savedTables = Lists.newArrayList();
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();
            TableDesc origTable = tableMetaMgr.getTableDesc(tableDesc.getIdentity());
            NTableDesc nTableDesc = new NTableDesc(tableDesc);
            if (origTable == null || origTable.getProject() == null) {
                nTableDesc.setUuid(UUID.randomUUID().toString());
                nTableDesc.setLastModified(0);
            } else {
                nTableDesc.setUuid(origTable.getUuid());
                nTableDesc.setLastModified(origTable.getLastModified());
                nTableDesc.setFact(origTable.getFact());
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
        String[] result = (String[]) saved.toArray(new String[saved.size()]);
        addTableToProject(savedTables, project);
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

    private void addTableToProject(List<TableDesc> tables, String project) throws IOException {
        getProjectManager().addTableDescToProject(tables, project);
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
        List<String> tables = Lists
                .newArrayList(FluentIterable.from(explr.listTables(database)).filter(new Predicate<String>() {
                    @Override
                    public boolean apply(String s) {
                        if (StringUtils.isEmpty(table)) {
                            return true;
                        } else if (s.toLowerCase().contains(table.toLowerCase())) {
                            return true;
                        }
                        return false;
                    }
                }));
        return tables;
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
        TableExtDesc tableExtDesc = getTableManager(project).getOrCreateTableExt(table.getIdentity());
        // get TableDescResponse
        TableDescResponse tableDescResponse = new TableDescResponse(table);
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

    private List<TableDesc> getTablesResponse(List<TableDesc> tables, String project, boolean withExt)
            throws IOException {
        List<TableDesc> descs = new ArrayList<TableDesc>();
        NDataModelManager dataModelManager = getDataModelManager(project);
        Iterator<TableDesc> it = tables.iterator();
        while (it.hasNext()) {
            TableDescResponse rtableDesc;
            TableDesc table = it.next();
            List<String> models = dataModelManager.getModelsUsingRootTable(table);
            List<String> modelsUsingTable = dataModelManager.getModelsUsingTable(table);
            if (withExt) {
                rtableDesc = getTableResponse(table, project);
            } else {
                rtableDesc = new TableDescResponse(table);
            }
            if (CollectionUtils.isNotEmpty(models)) {
                rtableDesc.setRootFact(true);
            } else if (CollectionUtils.isNotEmpty(modelsUsingTable)) {
                rtableDesc.setLookup(true);
            }
            Pair<Set<String>, Set<String>> tableColumnType = getTableColumnType(table, project);
            NDataLoadingRange dataLoadingRange = getDataLoadingRangeManager(project)
                    .getDataLoadingRange(table.getIdentity());
            if (null != dataLoadingRange) {
                rtableDesc.setPartitionedColumn(dataLoadingRange.getColumnName());
                rtableDesc.setSegmentRanges(getSegmentRangesWithStatus(dataLoadingRange));
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
    private Pair<Set<String>, Set<String>> getTableColumnType(TableDesc table, String project) throws IOException {
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

    public void setFact(String table, String project, boolean fact, String column, String dateFormat)
            throws IOException, PersistentException {
        NTableMetadataManager tableManager = getTableManager(project);

        val modelManager = getDataModelManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        String tableName = table.substring(table.lastIndexOf(".") + 1);
        String columnIdentity = tableName + "." + column;
        boolean oldFact = tableDesc.getFact();
        //toogle table type,remove all segments in related models
        if (fact == oldFact) {
            return;
        } else if (fact) {
            modelService.checkSingleIncrementingLoadingTable(project, table);
            NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
            dataLoadingRange.updateRandomUuid();
            dataLoadingRange.setProject(project);
            dataLoadingRange.setTableName(table);
            dataLoadingRange.setColumnName(columnIdentity);
            dataLoadingRange.setPartitionDateFormat(dateFormat);
            dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
            tableDesc.setFact(fact);
            tableManager.updateTableDesc(tableDesc);

        } else {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
            tableDesc.setFact(fact);
            tableManager.updateTableDesc(tableDesc);
        }

        val models = modelManager.getTableOrientedModelsUsingRootTable(tableDesc);
        for (val model : models) {
            //follow semanticVersion,#8196
            modelService.purgeModel(model, project);
            syncPartitionDesc(model, project, columnIdentity, dateFormat);
            if (!fact) {
                buildFullSegment(model, project);
            }
        }
    }

    private void syncPartitionDesc(String model, String project, String column, String dateFormat) throws IOException {
        val dataloadingManager = getDataLoadingRangeManager(project);
        val datamodelManager = getDataModelManager(project);
        val modelDesc = datamodelManager.getDataModelDesc(model);
        val dataloadingRange = dataloadingManager.getDataLoadingRange(modelDesc.getRootFactTableName());
        val modelUpdate = datamodelManager.copyForWrite(modelDesc);
        //full load
        if (dataloadingRange == null) {
            modelUpdate.setPartitionDesc(null);
        } else {
            var partition = modelUpdate.getPartitionDesc();
            if (partition == null) {
                partition = new PartitionDesc();
            }
            partition.setPartitionDateColumn(column);
            if (StringUtils.isNotEmpty(dateFormat)) {
                partition.setPartitionDateFormat(dateFormat);
            }
            modelUpdate.setPartitionDesc(partition);
        }
        datamodelManager.updateDataModelDesc(modelUpdate);
    }

    private void buildFullSegment(String model, String project) throws IOException, PersistentException {
        val eventManager = getEventManager(project);
        val dataflowManager = getDataflowManager(project);
        val cubePlanManager = getCubePlanManager(project);
        val cubePlan = cubePlanManager.findMatchingCubePlan(model, project, KylinConfig.getInstanceFromEnv());
        val dataflow = dataflowManager.getDataflow(cubePlan.getName());
        val newSegment = dataflowManager.appendSegment(dataflow,
                new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        val event = new AddSegmentEvent();
        event.setSegmentIds(Lists.newArrayList(newSegment.getId()));
        event.setApproved(true);
        event.setCubePlanName(cubePlan.getName());
        event.setModelName(model);
        event.setSegmentRange(newSegment.getSegRange());
        event.setProject(project);
        eventManager.post(event);
    }

    public void setDataRange(DateRangeRequest dateRangeRequest) throws IOException, PersistentException {
        String project = dateRangeRequest.getProject();
        String table = dateRangeRequest.getTable();
        SegmentRange segmentRange = getSegmentRangeByTable(dateRangeRequest);
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(dateRangeRequest.getProject(),
                dateRangeRequest.getTable());
        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        SegmentRange newSegmentRange = SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(),
                dateRangeRequest.getEnd());
        SegmentRange readyRange = dataLoadingRange.getCoveredReadySegmentRange();
        SegmentRange allRange = dataLoadingRange.getCoveredSegmentRange();

        //has some building segments
        checkShrinkRangeInBuildingSide(allRange, readyRange, newSegmentRange);

        List<SegmentRange> segmentRanges = getNewSegmentRanges(rangeManager.getDataLoadingRange(table), segmentRange);
        EventManager eventManager = getEventManager(project);
        for (SegmentRange seg : segmentRanges) {
            LoadingRangeUpdateEvent updateEvent = new LoadingRangeUpdateEvent();
            updateEvent.setTableName(table);
            updateEvent.setApproved(true);
            updateEvent.setProject(project);
            updateEvent.setSegmentRange(seg);
            eventManager.post(updateEvent);
            dataLoadingRange = rangeManager.appendSegmentRange(dataLoadingRange, seg);
        }

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

    private void checkShrinkRangeInBuildingSide(SegmentRange allRange, SegmentRange readyRange,
            SegmentRange newSegmentRange) {
        if (allRange == null) {
            return;
        } else {
            //having some building segments in tail
            if (readyRange == null || readyRange.getEnd().compareTo(allRange.getEnd()) < 0) {
                if (newSegmentRange.getEnd().compareTo(allRange.getEnd()) < 0)
                    throw new BadRequestException(
                            "Some segments is building, can not set data range smaller than before");
            }

            //having some building segments in head
            if (readyRange == null || readyRange.getStart().compareTo(allRange.getStart()) > 0) {
                if (newSegmentRange.getStart().compareTo(allRange.getStart()) > 0)
                    throw new BadRequestException(
                            "Some segments is building, can not set data range smaller than before");
            }
        }
    }

    private List<SegmentRange> getNewSegmentRanges(NDataLoadingRange dataLoadingRange, SegmentRange newRange) {
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange oldSegmentRange = dataLoadingRange.getCoveredSegmentRange();
        if (dataLoadingRange == null || null == oldSegmentRange || !oldSegmentRange.overlaps(newRange)) {
            segmentRanges.add(newRange);
            return segmentRanges;
        }

        if (oldSegmentRange.contains(newRange)) {
            //do nothing but set range to new start and end
            return segmentRanges;
        }

        if (newRange.getStart().compareTo(oldSegmentRange.getStart()) < 0) {

            segmentRanges.add(newRange.getStartDeviation(oldSegmentRange));
        }
        if (newRange.getEnd().compareTo(oldSegmentRange.getEnd()) > 0) {
            segmentRanges.add(oldSegmentRange.getEndDeviation(newRange));
        }

        return segmentRanges;
    }

    public SegmentRange getSegmentRangeByTable(DateRangeRequest dateRangeRequest) {
        String project = dateRangeRequest.getProject();
        String table = dateRangeRequest.getTable();
        NTableMetadataManager nProjectManager = getTableManager(project);
        TableDesc tableDesc = nProjectManager.getTableDesc(table);
        return SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(),
                dateRangeRequest.getEnd());

    }

    public void unloadTable(String project, String table) throws IOException {
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        tableMetadataManager.removeTableExt(table);
        tableMetadataManager.removeSourceTable(table);
    }

    public void setTop(String table, String project, boolean top) throws IOException {
        NTableMetadataManager nTableMetadataManager = getTableManager(project);
        TableDesc tableDesc = nTableMetadataManager.getTableDesc(table);
        tableDesc.setTop(top);
        nTableMetadataManager.updateTableDesc(tableDesc);
    }

    public List<TablesAndColumnsResponse> getTableAndColomns(String project) {
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
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, table);
        SegmentRange readySegmentRange = dataLoadingRange.getCoveredReadySegmentRange();
        if (readySegmentRange == null) {
            throw new BadRequestException("There is no ready segment to refresh!");
        }
        NTableMetadataManager tableMetadataManager = getTableManager(project);
        TableDesc tableDesc = tableMetadataManager.getTableDesc(table);
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

    public void setPushDownMode(String project, String table, boolean pushdownRangeLimited) throws IOException {
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
        if (model.getManagementType().equals(ManagementType.MODEL_BASED)) {
            mergeConfig.setAutoMergeEnabled(model.isAutoMergeEnabled());
            mergeConfig.setAutoMergeTimeRanges(model.getAutoMergeTimeRanges());
            mergeConfig.setVolatileRange(model.getVolatileRange());
        } else {
            mergeConfig = getAutoMergeConfigByTable(project, model.getRootFactTable().getTableIdentity());
        }

        return mergeConfig;
    }

    public AutoMergeConfigResponse getAutoMergeConfigByTable(String project, String tableName) {
        AutoMergeConfigResponse mergeConfig = new AutoMergeConfigResponse();
        NDataLoadingRange dataLoadingRange = getDataLoadingRange(project, tableName);
        mergeConfig.setAutoMergeEnabled(dataLoadingRange.isAutoMergeEnabled());
        mergeConfig.setAutoMergeTimeRanges(dataLoadingRange.getAutoMergeTimeRanges());
        mergeConfig.setVolatileRange(dataLoadingRange.getVolatileRange());
        return mergeConfig;
    }

    public void setAutoMergeConfigByModel(AutoMergeRequest autoMergeRequest) throws IOException {
        String project = autoMergeRequest.getProject();
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
            model.setVolatileRange(volatileRange);
            model.setAutoMergeTimeRanges(autoMergeRanges);
            model.setAutoMergeEnabled(autoMergeRequest.isAutoMergeEnabled());
            NDataModel modelUpdate = dataModelManager.copyForWrite(model);
            dataModelManager.updateDataModelDesc(modelUpdate);

        } else {
            autoMergeRequest.setTable(model.getRootFactTable().getTableIdentity());
            setAutoMergeConfigByTable(autoMergeRequest);

        }
    }

    public boolean getPushDownMode(String project, String table) {
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        return dataLoadingRange.isPushdownRangeLimited();
    }

    public void setAutoMergeConfigByTable(AutoMergeRequest autoMergeRequest) throws IOException {
        String project = autoMergeRequest.getProject();
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
        dataLoadingRange.setAutoMergeEnabled(autoMergeRequest.isAutoMergeEnabled());
        dataLoadingRange.setAutoMergeTimeRanges(autoMergeRanges);
        dataLoadingRange.setVolatileRange(volatileRange);
        NDataLoadingRange dataLoadingRangeUpdate = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
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
