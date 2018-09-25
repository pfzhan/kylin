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
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableDesc;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.LoadingRangeUpdateEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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


    public List<TableDesc> getTableDesc(String project, boolean withExt, String tableName) throws IOException {
        List<TableDesc> tables = new ArrayList<>();
        if (StringUtils.isEmpty(tableName)) {
            tables.addAll(getProjectManager().listDefinedTables(project));
        } else {
            tables.add(getTableManager(project).getTableDesc(tableName));
        }
        tables = getTablesResponse(tables, project, withExt);
        return tables;
    }

    public String[] loadTableToProject(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {
        return loadTablesToProject(Lists.newArrayList(Pair.newPair(tableDesc, extDesc)), project);
    }

    private String[] loadTablesToProject(List<Pair<TableDesc, TableExtDesc>> allMeta, String project)
            throws IOException {
        final NTableMetadataManager tableMetaMgr = getTableManager(project);
        // save table meta
        List<String> saved = Lists.newArrayList();
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
            }

            tableMetaMgr.saveSourceTable(nTableDesc);
            if (extDesc != null) {
                TableExtDesc origExt = tableMetaMgr.getTableExt(tableDesc.getIdentity());
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
        }
        String[] result = (String[]) saved.toArray(new String[saved.size()]);
        addTableToProject(result, project);
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

    private void addTableToProject(String[] tables, String project) throws IOException {
        getProjectManager().addTableDescToProject(tables, project);
    }

    public List<String> getSourceDbNames(String project, int dataSourceType) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listDatabases();
    }

    public List<String> getSourceTableNames(String project, String database, int dataSourceType) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getSource(getProjectManager().getProject(project))
                .getSourceMetadataExplorer();
        return explr.listTables(database);
    }

    private TableDescResponse getTableResponse(TableDesc table, String project) {
        TableExtDesc tableExtDesc = getTableManager(project).getTableExt(table.getIdentity());
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
                cardinality.put(columnDesc.getName(), Long.parseLong(cardinalities[i]));
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
                rtableDesc.setWaterMarkStart(dataLoadingRange.getWaterMarkStart());
                rtableDesc.setWaterMarkEnd(dataLoadingRange.getWaterMarkEnd());
                SegmentRange segmentRange = dataLoadingRange.getCoveredSegmentRange();
                if (segmentRange != null) {
                    rtableDesc.setStartTime(Long.parseLong(segmentRange.getStart().toString()));
                    rtableDesc.setEndTime(Long.parseLong(segmentRange.getEnd().toString()));

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
            JoinTableDesc[] joinTables = dataModelManager.getDataModelDesc(model).getJoinTables();
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

    public void setFact(String table, String project, boolean fact, String column) throws IOException {
        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        String tableName = table.substring(table.lastIndexOf(".") + 1);
        String ColumnIdentity = tableName + "." + column;
        boolean oldFact = tableDesc.getFact();
        if (fact && !oldFact) {
            NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
            dataLoadingRange.updateRandomUuid();
            dataLoadingRange.setProject(project);
            dataLoadingRange.setTableName(table);
            dataLoadingRange.setColumnName(ColumnIdentity);
            dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
            tableDesc.setFact(fact);
            tableManager.updateTableDesc(tableDesc);
        } else if (!fact && oldFact) {
            NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
            dataLoadingRangeManager.removeDataLoadingRange(dataLoadingRange);
            tableDesc.setFact(fact);
            tableManager.updateTableDesc(tableDesc);
        }
    }

    public void setDataRange(DateRangeRequest dateRangeRequest) throws IOException, PersistentException {
        String project = dateRangeRequest.getProject();
        String table = dateRangeRequest.getTable();
        SegmentRange segmentRange = getSegmentRangeByTable(dateRangeRequest);
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange(table);
        NTableMetadataManager tableManager = getTableManager(project);
        if (dataLoadingRange != null) {
            TableDesc tableDesc = tableManager.getTableDesc(table);
            tableManager.updateTableDesc(tableDesc);
            List<SegmentRange> segmentRanges = getSegmentRanges(rangeManager.getDataLoadingRange(table), segmentRange);
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

        } else {
            throw new IllegalArgumentException(
                    "this table can not set date range, plz check table " + table + "is fact or else");
        }
    }

    private List<SegmentRange> getSegmentRanges(NDataLoadingRange dataLoadingRange, SegmentRange newRange) {
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
        return SourceFactory.getSource(tableDesc).getSegmentRange(dateRangeRequest.getStart(), dateRangeRequest.getEnd());

    }
}
