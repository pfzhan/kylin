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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.BatchLoadTableResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.TableDescResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableServiceTest extends CSVSourceTestCase {

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        super.setup();
        System.setProperty("HADOOP_USER_NAME", "root");
        ReflectionTestUtils.setField(tableService, "modelService", modelService);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "9");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testGetTableDesc() throws IOException {

        List<TableDesc> tableDesc = tableService.getTableDesc("default", true, "", "DEFAULT", true);
        Assert.assertEquals(true, tableDesc.size() == 9);
        List<TableDesc> tableDesc2 = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", false);
        Assert.assertEquals(1, tableDesc2.size());
        List<TableDesc> tables3 = tableService.getTableDesc("default", true, "", "", true);
        Assert.assertEquals(true, tables3.size() == 12);
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", true);
        Assert.assertEquals("TEST_KYLIN_FACT", tables.get(0).getName());
        Assert.assertEquals(5633024, ((TableDescResponse) tables.get(0)).getStorageSize());
        Assert.assertEquals(0, ((TableDescResponse) tables.get(0)).getTotalRecords());

        List<TableDesc> table2 = tableService.getTableDesc("default", true, "country", "DEFAULT", true);
        Assert.assertEquals(true, table2.get(0).getName().equals("TEST_COUNTRY"));

        // get a not existing table desc
        tableDesc = tableService.getTableDesc("default", true, "not_exist_table", "DEFAULT", false);
        Assert.assertEquals(0, tableDesc.size());
    }

    @Test
    public void testGetTableDescAndVerifyColumnsInfo() throws IOException {
        final String tableIdentity = "DEFAULT.TEST_COUNTRY";
        final NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "newten");
        final TableDesc tableDesc = tableMgr.getTableDesc(tableIdentity);
        final TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);

        // mock table ext desc
        TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
        tableExt.setIdentity(tableIdentity);
        TableExtDesc.ColumnStats col1 = new TableExtDesc.ColumnStats();
        col1.setCardinality(100);
        col1.setTableExtDesc(tableExt);
        col1.setColumnName(tableDesc.getColumns()[0].getName());
        col1.setMinValue("America");
        col1.setMaxValue("Zimbabwe");
        col1.setNullCount(0);
        tableExt.setColumnStats(Lists.newArrayList(col1));
        tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);

        // verify the column stats update successfully
        final TableExtDesc newTableExt = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertEquals(1, newTableExt.getAllColumnStats().size());

        // call api to check tableDescResponse has the correct value
        final List<TableDesc> tables = tableService.getTableDesc("newten", true, "TEST_COUNTRY", "DEFAULT", true);
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.get(0) instanceof TableDescResponse);
        TableDescResponse t = (TableDescResponse) tables.get(0);
        final TableDescResponse.ColumnDescResponse[] extColumns = t.getExtColumns();
        Assert.assertEquals(100L, extColumns[0].getCardinality().longValue());
        Assert.assertEquals("America", extColumns[0].getMinValue());
        Assert.assertEquals("Zimbabwe", extColumns[0].getMaxValue());
        Assert.assertEquals(0L, extColumns[0].getNullCount().longValue());
    }

    @Test
    public void testGetTableDescWithSchemaChange() throws IOException {
        final String tableIdentity = "DEFAULT.TEST_COUNTRY";
        final NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "newten");
        final TableDesc tableDesc = tableMgr.getTableDesc(tableIdentity);
        final TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);

        // mock table ext desc
        TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
        tableExt.setIdentity(tableIdentity);
        TableExtDesc.ColumnStats col1 = new TableExtDesc.ColumnStats();
        col1.setCardinality(100);
        col1.setTableExtDesc(tableExt);
        col1.setColumnName(tableDesc.getColumns()[0].getName());
        col1.setMinValue("America");
        col1.setMaxValue("Zimbabwe");
        col1.setNullCount(0);
        TableExtDesc.ColumnStats col2 = new TableExtDesc.ColumnStats();
        col2.setCardinality(1000);
        col2.setTableExtDesc(tableExt);
        col2.setColumnName(tableDesc.getColumns()[1].getName());
        col2.setMinValue("2300.0");
        col2.setMaxValue("2600.0");
        col2.setNullCount(0);
        TableExtDesc.ColumnStats col3 = new TableExtDesc.ColumnStats();
        col3.setCardinality(10000);
        col3.setTableExtDesc(tableExt);
        col3.setColumnName(tableDesc.getColumns()[2].getName());
        col3.setMinValue("3300.0");
        col3.setMaxValue("3600.0");
        col3.setNullCount(0);
        TableExtDesc.ColumnStats col4 = new TableExtDesc.ColumnStats();
        col4.setCardinality(40000);
        col4.setTableExtDesc(tableExt);
        col4.setColumnName(tableDesc.getColumns()[3].getName());
        col4.setMinValue("AAAA");
        col4.setMaxValue("ZZZZ");
        col4.setNullCount(10);
        tableExt.setColumnStats(Lists.newArrayList(col1, col2, col3, col4));
        tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);

        // verify the column stats update successfully
        final TableExtDesc newTableExt = tableMgr.getTableExtIfExists(tableDesc);
        Assert.assertEquals(4, newTableExt.getAllColumnStats().size());

        // table desc schema change
        TableDesc changedTable = new TableDesc(tableDesc);
        final ColumnDesc[] columns = changedTable.getColumns();
        Assert.assertEquals(4, columns.length);
        columns[0].setName("COUNTRY_NEW");
        columns[1].setName(columns[3].getName());
        columns[2].setDatatype("float");
        ColumnDesc[] newColumns = new ColumnDesc[3];
        System.arraycopy(columns, 0, newColumns, 0, 3);
        changedTable.setColumns(newColumns);
        tableMgr.updateTableDesc(changedTable);

        // verify update table desc changed successfully
        final TableDesc confirmedTableDesc = tableMgr.getTableDesc(tableIdentity);
        Assert.assertEquals(3, confirmedTableDesc.getColumnCount());
        Assert.assertEquals("COUNTRY_NEW", confirmedTableDesc.getColumns()[0].getName());
        Assert.assertEquals("NAME", confirmedTableDesc.getColumns()[1].getName());
        Assert.assertEquals("float", confirmedTableDesc.getColumns()[2].getDatatype());

        // call api to check tableDescResponse has the correct value
        final List<TableDesc> tables = tableService.getTableDesc("newten", true, "TEST_COUNTRY", "DEFAULT", true);
        Assert.assertEquals(1, tables.size());
        Assert.assertTrue(tables.get(0) instanceof TableDescResponse);
        TableDescResponse t = (TableDescResponse) tables.get(0);
        final TableDescResponse.ColumnDescResponse[] extColumns = t.getExtColumns();
        Assert.assertNull(extColumns[0].getCardinality());
        Assert.assertNull(extColumns[0].getMinValue());
        Assert.assertNull(extColumns[0].getMaxValue());
        Assert.assertNull(extColumns[0].getNullCount());
        Assert.assertEquals(40000L, extColumns[1].getCardinality().longValue());
        Assert.assertEquals("AAAA", extColumns[1].getMinValue());
        Assert.assertEquals("ZZZZ", extColumns[1].getMaxValue());
        Assert.assertEquals(10L, extColumns[1].getNullCount().longValue());
        Assert.assertEquals(10000L, extColumns[2].getCardinality().longValue());
        Assert.assertEquals("3300.0", extColumns[2].getMinValue());
        Assert.assertEquals("3600.0", extColumns[2].getMaxValue());
        Assert.assertEquals("float", extColumns[2].getDatatype());
    }

    @Test
    public void testExtractTableMeta() throws Exception {
        String[] tables = { "DEFAULT.TEST_ACCOUNT", "DEFAULT.TEST_KYLIN_FACT" };
        List<Pair<TableDesc, TableExtDesc>> result = tableService.extractTableMeta(tables, "default");
        Assert.assertEquals(true, result.size() == 2);

    }

    @Test
    public void testLoadTableToProject() throws IOException {
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", true);
        TableDesc nTableDesc = new TableDesc(tables.get(0));
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("DEFAULT.TEST_COUNTRY");
        TableExtDesc tableExtDesc = new TableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(nTableDesc, tableExtDesc, "default");
        Assert.assertTrue(result.length == 1);
    }

    @Test
    public void testLoadCaseSensitiveTableToProject() throws IOException {
        NTableMetadataManager tableManager = tableService.getTableManager("case_sensitive");
        Serializer<TableDesc> serializer = tableManager.getTableMetadataSerializer();
        String contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/case_sensitive/table_desc/CASE_SENSITIVE.TEST_KYLIN_FACT.json")
                        .toPath(),
                Charset.defaultCharset()), "\n");
        InputStream originStream = IOUtils.toInputStream(contents, Charset.defaultCharset());
        TableDesc origin = serializer.deserialize(new DataInputStream(originStream));
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("CASE_SENSITIVE.TEST_KYLIN_FACT");
        TableExtDesc tableExtDesc = new TableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(origin, tableExtDesc, "case_sensitive");
        Assert.assertTrue(result.length == 1);
        ObjectMapper mapper = new ObjectMapper();
        String jsonContent = mapper.writeValueAsString(origin);
        InputStream savedStream = IOUtils.toInputStream(jsonContent, Charset.defaultCharset());
        TableDesc saved = serializer.deserialize(new DataInputStream(savedStream));

        Assert.assertEquals("test_kylin_fact", saved.getCaseSensitiveName());
        Assert.assertEquals("TEST_KYLIN_FACT", saved.getName());
        Assert.assertEquals("case_sensitive", saved.getCaseSensitiveDatabase());
        Assert.assertEquals("CASE_SENSITIVE", saved.getDatabase());
        Assert.assertEquals("trans_id", saved.getColumns()[0].getCaseSensitiveName());
        Assert.assertEquals("TRANS_ID", saved.getColumns()[0].getName());

    }

    @Test
    public void testReloadExistTable() throws IOException {
        testLoadTableToProject();
        testLoadTableToProject();
    }

    @Test
    public void testUnloadTable() throws IOException {
        TableDesc tableDesc = new TableDesc();
        List<ColumnDesc> columns = new ArrayList<>();
        columns.add(new ColumnDesc());
        ColumnDesc[] colomnArr = new ColumnDesc[1];
        tableDesc.setColumns(columns.toArray(colomnArr));
        tableDesc.setName("TEST_UNLOAD");
        tableDesc.setDatabase("DEFAULT");
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("DEFAULT.TEST_UNLOAD");
        TableExtDesc tableExtDesc = new TableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(tableDesc, tableExtDesc, "default");
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default");
        Assert.assertTrue(result.length == 1);
        val size = nTableMetadataManager.listAllTables().size();
        tableService.unloadTable("default", "DEFAULT.TEST_UNLOAD", false);

        Assert.assertEquals(null, nTableMetadataManager.getTableDesc("DEFAULT.TEST_UNLOAD"));
        Assert.assertEquals(size - 1, nTableMetadataManager.listAllTables().size());
    }

    @Test
    public void testUnloadTable_RemoveModels() throws IOException {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val originSize = dfMgr.listUnderliningDataModels().size();
        val response = tableService.preUnloadTable("default", "EDW.TEST_SITES");
        Assert.assertTrue(response.isHasModel());
        tableService.unloadTable("default", "EDW.TEST_SITES", true);
        Assert.assertEquals(originSize - 4, dfMgr.listUnderliningDataModels().size());
    }

    @Test
    public void testUnloadNotExistTable() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Cannot find table 'DEFAULT.not_exist_table'");
        tableService.unloadTable("default", "DEFAULT.not_exist_table", false);
    }

    @Test
    public void testUnloadTable_RemoveNDataLoadingRange() throws Exception {
        setupPushdownEnv();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";

        NTableMetadataManager nTableMetadataManager = NTableMetadataManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val originSize = nTableMetadataManager.listAllTables().size();

        // Add partition_key and data_loading_range
        DateRangeRequest request = mockDateRangeRequest();
        tableService.setPartitionKey(tableName, "default", "CAL_DT");
        tableService.setDataRange("default", request);
        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(tableName);
        Assert.assertNotNull(dataLoadingRange);
        Assert.assertEquals(1294364500000L, dataLoadingRange.getCoveredRange().getStart());
        Assert.assertEquals(1294450900000L, dataLoadingRange.getCoveredRange().getEnd());

        // unload table
        tableService.unloadTable("default", tableName, false);
        Assert.assertEquals(originSize - 1, nTableMetadataManager.listAllTables().size());

        // reload table
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT" };
        List<Pair<TableDesc, TableExtDesc>> extractTableMeta = tableService.extractTableMeta(tables, "default");
        tableService.loadTableToProject(extractTableMeta.get(0).getFirst(), extractTableMeta.get(0).getSecond(),
                "default");
        Assert.assertEquals(originSize, nTableMetadataManager.listAllTables().size());
        dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(tableName);
        Assert.assertNull(dataLoadingRange);
    }

    @Test
    public void testGetSourceDbNames() throws Exception {
        List<String> dbNames = tableService.getSourceDbNames("default");
        ArrayList<String> dbs = Lists.newArrayList(dbNames);
        Assert.assertTrue(dbs.contains("DEFAULT"));
    }

    @Test
    public void testGetSourceTableNames() throws Exception {
        List<String> tableNames = tableService.getSourceTableNames("default", "DEFAULT", "");
        Assert.assertTrue(tableNames.contains("TEST_ACCOUNT"));
    }

    @Test
    public void testNormalizeHiveTableName() {
        String tableName = tableService.normalizeHiveTableName("DEFaULT.TeST_ACCOUNT");
        Assert.assertTrue(tableName.equals("DEFAULT.TEST_ACCOUNT"));
    }

    @Test
    public void testSetPartitionKeyAndSetDataRange() throws Exception {
        setupPushdownEnv();
        testGetBatchLoadTablesBefore();
        testSetPartitionKeyAndSetDataRangeWithoutException();
        testGetBatchLoadTablesAfter();
        testSetDataRangeWhenNoNewData();
        testSetDataRangeOverlapOrGap();
        testGetLatestData();
        cleanPushdownEnv();
    }

    private void testGetBatchLoadTablesBefore() {
        List<BatchLoadTableResponse> responses = tableService.getBatchLoadTables("default");
        Assert.assertEquals(0, responses.size());
    }

    private void testGetBatchLoadTablesAfter() {
        List<BatchLoadTableResponse> responses = tableService.getBatchLoadTables("default");
        Assert.assertEquals(1, responses.size());
        BatchLoadTableResponse response = responses.get(0);
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", response.getTable());
        Assert.assertEquals(62, response.getRelatedIndexNum());
    }

    private void testSetPartitionKeyAndSetDataRangeWithoutException() throws Exception {
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        List<TableDesc> tables = tableService.getTableDesc("default", false, "", "DEFAULT", true);
        //test set fact and table list order by fact
        Assert.assertTrue(tables.get(0).getName().equals("TEST_KYLIN_FACT") && tables.get(0).isIncrementLoading());
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(0L, 1294364500000L);
        dataLoadingRange.setCoveredRange(segmentRange);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        DateRangeRequest request = mockDateRangeRequest();
        tableService.setDataRange("default", request);
        rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");

        // case of load existing data
        tableService.setDataRange("default", mockeDateRangeRequestWithoutTime());
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        sdf.setTimeZone(TimeZone.getDefault());

        long t2 = sdf.parse("2014/01/01").getTime();

        Assert.assertEquals(t2, dataLoadingRange.getCoveredRange().getEnd());
    }

    //test toggle partition Key,A to null, null to A ,A to B with model:with lag behind, without lag behind
    @Test
    public void testTogglePartitionKey_NullToNotNull() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");

        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getSegments().size() == 1);
        Assert.assertTrue(loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT") == null);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getSegments().size() == 0);
        val loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertTrue(loadingRange != null);
        Assert.assertTrue(loadingRange.getColumnName().equals("TEST_KYLIN_FACT.CAL_DT"));

    }

    @Test
    public void testTogglePartitionKey_OneToAnother() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getSegments().size() == 1);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getSegments().size() == 0);

        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        val copy = loadingRangeMgr.copyForWrite(loadingRange);
        copy.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, 100000L));
        loadingRangeMgr.updateDataLoadingRange(copy);

        //change partition

        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "ORDER_ID");
        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertTrue(loadingRange.getCoveredRange() == null);
        Assert.assertEquals(loadingRange.getColumnName(), "TEST_KYLIN_FACT.ORDER_ID");

    }

    @Test
    public void testTogglePartitionKey_OneToNull() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getSegments().size() == 1);
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRangeMgr.createDataLoadingRange(loadingRange);

        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange.getCoveredRange());
        Assert.assertEquals(loadingRange.getColumnName(), "TEST_KYLIN_FACT.CAL_DT");

        //set null
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(df.getSegments().size(), 1);

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        Assert.assertEquals(eventDao.getEventsByModel(df.getUuid()).size(), 2);

        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange);
    }

    @Test
    public void testTogglePartitionKey_NullToOneWithLagBehindModel() throws Exception {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertTrue(df.getStatus().equals(RealizationStatusEnum.ONLINE));
        dfMgr.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNotNull(loadingRange);
        Assert.assertEquals(loadingRange.getColumnName(), "TEST_KYLIN_FACT.CAL_DT");
        Assert.assertTrue(df.getStatus().equals(RealizationStatusEnum.ONLINE));

    }

    private void testSetDataRangeWhenNoNewData() {
        DateRangeRequest request = mockDateRangeRequest();
        // case of no more new data
        request.setStart(null);
        request.setEnd(null);
        try {
            tableService.setDataRange("default", request);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalStateException.class, ex.getClass());
            Assert.assertEquals("There is no more new data to load", ex.getMessage());
        }
    }

    private void testSetDataRangeOverlapOrGap() throws ParseException {

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        sdf.setTimeZone(TimeZone.getDefault());

        long t1 = sdf.parse("2012/01/01").getTime();
        long t2 = sdf.parse("2014/01/01").getTime();
        long t3 = sdf.parse("2012/02/01").getTime();
        long t4 = sdf.parse("2014/03/01").getTime();

        DateRangeRequest request = mockDateRangeRequest();
        // 2012-02-01
        request.setStart(String.valueOf(t3));
        // 2012-03-01
        request.setEnd(String.valueOf(t4));
        try {
            tableService.setDataRange("default", request);
        } catch (Exception ex) {
            Assert.assertEquals(TransactionException.class, ex.getClass());
            Assert.assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
            Assert.assertEquals("NDataLoadingRange appendSegmentRange TimePartitionedSegmentRange[" + String.valueOf(t3)
                    + "," + String.valueOf(t4)
                    + ") has overlaps/gap with existing segmentRanges TimePartitionedSegmentRange[0,"
                    + String.valueOf(t2) + ")", ex.getCause().getMessage());
        }

        // case of having gap with current loading range
        long t5 = sdf.parse("2014/01/02").getTime();
        long t6 = sdf.parse("2014/01/03").getTime();

        request.setStart(String.valueOf(t5));
        request.setEnd(String.valueOf(t6));
        try {
            tableService.setDataRange("default", request);
        } catch (Exception ex) {
            Assert.assertEquals(TransactionException.class, ex.getClass());
            Assert.assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
            Assert.assertEquals("NDataLoadingRange appendSegmentRange TimePartitionedSegmentRange[" + t5 + "," + t6
                    + ") has overlaps/gap with existing segmentRanges TimePartitionedSegmentRange[0," + t2 + ")",
                    ex.getCause().getMessage());
        }
    }

    private void testGetLatestData() throws Exception {
        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        sdf.setTimeZone(TimeZone.getDefault());

        long t1 = sdf.parse("2012/01/01").getTime();
        long t2 = sdf.parse("2014/01/01").getTime();

        ExistedDataRangeResponse response = tableService.getLatestDataRange("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(String.valueOf(t2), response.getEndTime());
    }

    @Test
    public void testSetDateRangeException() throws Exception {
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setTable("DEFAULT.TEST_ACCOUNT");
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("this table can not set date range, please check table");
        tableService.setDataRange("default", dateRangeRequest);
    }

    @Test
    public void testGetTableAndColumns() {
        List<TablesAndColumnsResponse> result = tableService.getTableAndColumns("default");
        Assert.assertEquals(12, result.size());
    }

    @Test
    public void testGetSegmentRange() {
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        SegmentRange segmentRange = tableService.getSegmentRangeByTable(dateRangeRequest);
        Assert.assertTrue(segmentRange instanceof SegmentRange.TimePartitionedSegmentRange);
    }

    @Test
    public void testSetTop() throws IOException {
        TopTableRequest topTableRequest = mockTopTableRequest();
        tableService.setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        List<TableDesc> tables = tableService.getTableDesc("default", false, "", "DEFAULT", true);
        Assert.assertTrue(tables.get(0).isTop());
    }

    @Test
    public void checkRefreshDataRangeException1() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("There is no ready segment to refresh!");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        tableService.checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "1294364500000");
    }

    @Test
    public void checkRefreshDataRangeException2() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data during refresh range must be ready!");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(1294364400000L, 1294364500000L);
        dataLoadingRange.setCoveredRange(segmentRange);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        tableService.checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "1294364500000");
    }

    @Test
    public void testGetAutoMergeConfigException() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Model default does not exist in project default");
        tableService.getAutoMergeConfigByModel("default", "default");
    }

    @Test
    public void testGetAutoMergeConfig() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        //table oriented model
        AutoMergeConfigResponse response = tableService.getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(response.getVolatileRange().getVolatileRangeNumber(), 0);
        Assert.assertEquals(response.isAutoMergeEnabled(), true);
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

        dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        //model Based model
        response = tableService.getAutoMergeConfigByModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(response.getVolatileRange().getVolatileRangeNumber(), 0);
        Assert.assertEquals(response.isAutoMergeEnabled(), true);
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

    }

    @Test
    public void testSetAutoMergeConfigByTable() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        tableService.setAutoMergeConfigByTable("default", autoMergeRequest);
        AutoMergeConfigResponse respone = tableService.getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(respone.isAutoMergeEnabled(), autoMergeRequest.isAutoMergeEnabled());
        Assert.assertEquals(respone.getAutoMergeTimeRanges().size(), autoMergeRequest.getAutoMergeTimeRanges().length);
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeNumber(),
                autoMergeRequest.getVolatileRangeNumber());
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeType().toString(),
                autoMergeRequest.getVolatileRangeType());

    }

    @Test
    public void testSetAutoMergeConfigByModel() {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        autoMergeRequest.setTable("");
        autoMergeRequest.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        tableService.setAutoMergeConfigByModel("default", autoMergeRequest);
        AutoMergeConfigResponse respone = tableService.getAutoMergeConfigByModel("default",
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(respone.isAutoMergeEnabled(), autoMergeRequest.isAutoMergeEnabled());
        Assert.assertEquals(respone.getAutoMergeTimeRanges().size(), autoMergeRequest.getAutoMergeTimeRanges().length);
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeNumber(),
                autoMergeRequest.getVolatileRangeNumber());
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeType().toString(),
                autoMergeRequest.getVolatileRangeType());

    }

    @Test
    public void testSetPushDownMode() {
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        tableService.setPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT", true);
        boolean result = tableService.getPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetTableNameResponse_PASS() throws Exception {
        List<TableNameResponse> result = tableService.getTableNameResponses("default", "DEFAULT", "");
        Assert.assertEquals(9, result.size());
        Assert.assertTrue(result.get(0).isLoaded());

    }

    @Test
    public void testSetFact_NoRelatedModels_PASS() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
    }

    @Test
    public void testSetFact_NotRootFactTable_Exception() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incremental loading, as another model 'nmodel_basic_inner' uses it as a lookup table");
        tableService.setPartitionKey("DEFAULT.TEST_ACCOUNT", "default", "CAL_DT");
    }

    @Test
    public void testSetFact_IncrementingExists_Exception() {
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incremental loading, as another model 'nmodel_basic_inner' uses it as a lookup table");
        tableService.setPartitionKey("DEFAULT.TEST_ACCOUNT", "default", "CAL_DT");
    }

    @Test
    public void testSetFact_HasRelatedModels_PASS() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");

        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertEquals(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getPartitionDesc()
                .getPartitionDateColumn(), "TEST_KYLIN_FACT.CAL_DT");

        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        eventDao.deleteAllEvents();
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());

        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertNull(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getPartitionDesc());
        val events = eventDao.getEvents();
        //do not build immediately
        Assert.assertEquals(8, events.size());
        // TODO check other events
        //        Assert.assertEquals(0L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        //        Assert.assertEquals(Long.MAX_VALUE, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));
    }

    @Test
    public void testGetLoadedDatabases() {
        Set<String> loadedDatabases = tableService.getLoadedDatabases("default");
        Assert.assertEquals(loadedDatabases.size(), 2);
    }

    private TopTableRequest mockTopTableRequest() {
        TopTableRequest topTableRequest = new TopTableRequest();
        topTableRequest.setProject("default");
        topTableRequest.setTable("DEFAULT.TEST_COUNTRY");
        topTableRequest.setTop(true);
        return topTableRequest;
    }

    private AutoMergeRequest mockAutoMergeRequest() {
        AutoMergeRequest autoMergeRequest = new AutoMergeRequest();
        autoMergeRequest.setProject("default");
        autoMergeRequest.setTable("DEFAULT.TEST_KYLIN_FACT");
        autoMergeRequest.setAutoMergeEnabled(true);
        autoMergeRequest.setAutoMergeTimeRanges(new String[] { "HOUR" });
        autoMergeRequest.setVolatileRangeEnabled(true);
        autoMergeRequest.setVolatileRangeNumber(7);
        autoMergeRequest.setVolatileRangeType("HOUR");
        return autoMergeRequest;
    }

    private DateRangeRequest mockDateRangeRequest() {
        DateRangeRequest request = new DateRangeRequest();
        request.setStart("1294364500000");
        request.setEnd("1294450900000");
        request.setProject("default");
        request.setTable("DEFAULT.TEST_KYLIN_FACT");
        return request;
    }

    private DateRangeRequest mockeDateRangeRequestWithoutTime() {
        DateRangeRequest request = new DateRangeRequest();
        request.setProject("default");
        request.setTable("DEFAULT.TEST_KYLIN_FACT");
        return request;
    }

    @Test
    public void testGetProjectTables() throws Exception {
        NInitTablesResponse response = null;
        response = tableService.getProjectTables("default", "SSB.SS", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(response.getDatabases().size(), 0);

        response = tableService.getProjectTables("default", "SSB.CU", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(response.getDatabases().size(), 1);
        Assert.assertEquals(response.getDatabases().get(0).getTables().size(), 1);

        response = tableService.getProjectTables("default", "", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(response.getDatabases().size(), 3);
        Assert.assertEquals(response.getDatabases().get(0).getTables().size()
                + response.getDatabases().get(1).getTables().size() + response.getDatabases().get(2).getTables().size(),
                17);

        response = tableService.getProjectTables("default", "TEST", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(response.getDatabases().size(), 2);
        Assert.assertEquals(
                response.getDatabases().get(0).getTables().size() + response.getDatabases().get(1).getTables().size(),
                11);

        response = tableService.getProjectTables("default", "EDW.", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(response.getDatabases().size(), 1);
        Assert.assertEquals(response.getDatabases().get(0).getTables().size(), 3);

        response = tableService.getProjectTables("default", "EDW.", 0, 14, false, (databaseName, tableName) -> {
            return tableService.getTableDesc("default", true, tableName, databaseName, true);
        });
        Assert.assertEquals(response.getDatabases().size(), 1);
        Assert.assertEquals(response.getDatabases().get(0).getTables().size(), 3);

        response = tableService.getProjectTables("default", "DEFAULT.TEST_ORDER", 0, 14, false,
                (databaseName, tableName) -> {
                    return tableService.getTableDesc("default", true, tableName, databaseName, true);
                });
        Assert.assertEquals(response.getDatabases().size(), 1);
        Assert.assertEquals(response.getDatabases().get(0).getTables().size(), 1);

        response = tableService.getProjectTables("default", ".TEST_ORDER", 0, 14, false, (databaseName, tableName) -> {
            return tableService.getTableDesc("default", true, tableName, databaseName, true);
        });
        Assert.assertEquals(response.getDatabases().size(), 0);

    }

    @Test
    public void testClassifyDbTables() throws Exception {
        String project = "default";

        String[] tables1 = { "ssb", "ssb.KK", "DEFAULT", "DEFAULT.TEST", "DEFAULT.TEST_ACCOUNT" };
        Pair res = tableService.classifyDbTables(project, tables1);
        Assert.assertEquals("ssb", ((String[]) res.getFirst())[0]);
        Assert.assertEquals("DEFAULT", ((String[]) res.getFirst())[1]);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[2]);
        Assert.assertEquals(2, ((Set) res.getSecond()).size());

        String[] tables2 = { "KKK", "KKK.KK", ".DEFAULT", "DEFAULT.TEST", "DEFAULT.TEST_ACCOUNT" };
        res = tableService.classifyDbTables(project, tables2);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[0]);
        Assert.assertEquals(4, ((Set) res.getSecond()).size());

        String[] tables3 = { "DEFAULT.TEST_ACCOUNT", "SsB" };
        res = tableService.classifyDbTables(project, tables3);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[0]);
        Assert.assertEquals("SsB", ((String[]) res.getFirst())[1]);
        Assert.assertEquals(0, ((Set) res.getSecond()).size());
    }

}
