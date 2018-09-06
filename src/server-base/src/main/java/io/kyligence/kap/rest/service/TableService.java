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
import io.kyligence.kap.metadata.model.NTableDesc;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.model.LoadingRangeUpdateEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    public List<TableDesc> getTableDesc(String project, boolean withExt) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        List<TableDesc> tables = getProjectManager().listDefinedTables(project);
        if (null == tables) {
            return Collections.emptyList();
        }
        if (withExt) {
            aclEvaluate.checkProjectWritePermission(project);
            tables = cloneTablesDesc(tables, project);
        }
        return tables;
    }

    public TableDesc getTableDescByName(String tableName, boolean withExt, String prj) {
        aclEvaluate.checkProjectReadPermission(prj);
        TableDesc table = getTableManager(prj).getTableDesc(tableName);
        if (table == null) {
            throw new BadRequestException("this table does not exsits");
        }
        if (withExt) {
            aclEvaluate.checkProjectWritePermission(prj);
            table = cloneTableDesc(table, prj);
        }
        return table;
    }

    public String[] loadTableToProject(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {
        return loadTablesToProject(Lists.newArrayList(Pair.newPair(tableDesc, extDesc)), project);
    }

    private String[] loadTablesToProject(List<Pair<TableDesc, TableExtDesc>> allMeta, String project)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
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

    protected void removeTableFromProject(String tableName, String projectName) throws IOException {
        tableName = normalizeHiveTableName(tableName);
        getProjectManager().removeTableDescFromProject(tableName, projectName);
    }

    public boolean unloadTable(String tableName, String project) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        Message msg = MsgPicker.getMsg();
        boolean rtn = false;
        int tableType = 0;
        tableName = normalizeHiveTableName(tableName);
        TableDesc desc = getTableManager(project).getTableDesc(tableName);
        // unload of legacy global table is not supported for now
        if (desc == null || desc.getProject() == null) {
            logger.warn("Unload Table {} in Project {} failed, could not find TableDesc or related Project", tableName,
                    project);
            return false;
        }
        tableType = desc.getSourceType();
        if (!modelService.isTableInModel(desc, project)) {
            removeTableFromProject(tableName, project);
            rtn = true;
        } else {
            List<String> models = modelService.getModelsUsingTable(desc, project);
            throw new BadRequestException(String.format(msg.getTABLE_IN_USE_BY_MODEL(), models));
        }
        // it is a project local table, ready to remove since no model is using it within the project
        NTableMetadataManager metaMgr = getTableManager(project);
        metaMgr.removeTableExt(tableName);
        metaMgr.removeSourceTable(tableName);
        return rtn;
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

    private TableDescResponse cloneTableDesc(TableDesc table, String project) {
        TableExtDesc tableExtDesc = getTableManager(project).getTableExt(table.getIdentity());
        // Clone TableDesc
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

    private List<TableDesc> cloneTablesDesc(List<TableDesc> tables, String project) throws IOException {
        List<TableDesc> descs = new ArrayList<TableDesc>();
        Iterator<TableDesc> it = tables.iterator();
        while (it.hasNext()) {
            TableDesc table = it.next();
            TableDescResponse rtableDesc = cloneTableDesc(table, project);
            descs.add(rtableDesc);
        }

        return descs;
    }

    public String normalizeHiveTableName(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase();
    }

    public void setFact(String table, String project, boolean fact, String column) throws IOException {
        NTableMetadataManager tableManager = getTableManager(project);
        TableDesc tableDesc = tableManager.getTableDesc(table);
        NDataLoadingRangeManager dataLoadingRangeManager = getDataLoadingRangeManager(project);
        String ColumnIdentity = table + "." + column;
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

    public void setDataRange(String project, String table, long startTime, long endTime) throws IOException, PersistentException {
        NDataLoadingRangeManager rangeManager = getDataLoadingRangeManager(project);
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange(table);

        if (dataLoadingRange != null) {
            List<SegmentRange> segmentRanges = getSegmentRange(dataLoadingRange.getDataLoadingRange(), startTime,
                    endTime);
            SegmentRange.TimePartitionedDataLoadingRange range = new SegmentRange.TimePartitionedDataLoadingRange(
                    startTime, endTime);
            dataLoadingRange.setDataLoadingRange(range);
            rangeManager.updateDataLoadingRange(dataLoadingRange);
            EventManager eventManager = getEventManager(project);
            for (SegmentRange seg : segmentRanges) {
                LoadingRangeUpdateEvent updateEvent = new LoadingRangeUpdateEvent();
                updateEvent.setTableName(table);
                updateEvent.setApproved(true);
                updateEvent.setProject(project);
                updateEvent.setSegmentRange(seg);
                eventManager.post(updateEvent);
            }

        } else {
            throw new IllegalArgumentException(
                    "this table can not set date range, plz check table " + table + "is fact or else");
        }
    }

    private List<SegmentRange> getSegmentRange(SegmentRange dataLoadingRange, long startTime, long endTime) {
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange.TimePartitionedDataLoadingRange newRange = new SegmentRange.TimePartitionedDataLoadingRange(
                startTime, endTime);
        if (dataLoadingRange == null || !dataLoadingRange.overlaps(newRange)) {
            SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange = new SegmentRange.TimePartitionedSegmentRange(
                    newRange.getStart(), newRange.getEnd());
            segmentRanges.add(timePartitionedSegmentRange);
            return segmentRanges;
        }
        if (dataLoadingRange.contains(newRange)) {
            //do nothing but set range to new start and end
            return segmentRanges;
        }

        long oldStartTime = ((SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange).getStart();
        long oldEndTime = ((SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange).getEnd();
        if (startTime < oldStartTime) {
            segmentRanges.add(new SegmentRange.TimePartitionedSegmentRange(startTime, oldStartTime - 1));
        }
        if (endTime > oldEndTime) {
            segmentRanges.add(new SegmentRange.TimePartitionedSegmentRange(oldEndTime + 1, endTime));
        }

        return segmentRanges;
    }

}
