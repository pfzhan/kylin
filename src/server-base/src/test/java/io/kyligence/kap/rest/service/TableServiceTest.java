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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.rest.response.BatchLoadTableResponse;
import io.kyligence.kap.rest.response.ExistedDataRangeResponse;
import io.kyligence.kap.rest.response.TableDescResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");

    }

    @Before
    public void setup() {
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(tableService, "modelService", modelService);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "11");
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
        Assert.assertEquals(true, tableDesc.size() == 8);
        List<TableDesc> tableDesc2 = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", false);
        Assert.assertEquals(1, tableDesc2.size());
        List<TableDesc> tables3 = tableService.getTableDesc("default", true, "", "", true);
        Assert.assertEquals(true, tables3.size() == 11);
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", true);
        Assert.assertEquals("TEST_KYLIN_FACT", tables.get(0).getName());
        Assert.assertEquals(5633024, ((TableDescResponse) tables.get(0)).getStorageSize());
        Assert.assertEquals(0, ((TableDescResponse) tables.get(0)).getTotalRecords());

        List<TableDesc> table2 = tableService.getTableDesc("default", true, "country", "DEFAULT", true);
        Assert.assertEquals(true, table2.get(0).getName().equals("TEST_COUNTRY"));
    }

    @Test
    public void testExtractTableMeta() throws Exception {
        String[] tables = { "DEFAULT.TEST_ACCOUNT", "DEFAULT.TEST_KYLIN_FACT" };
        List<Pair<TableDesc, TableExtDesc>> result = tableService.extractTableMeta(tables, "default", 11);
        Assert.assertEquals(true, result.size() == 2);

    }

    @Test
    public void testLoadTableToProject() throws IOException {
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", true);
        TableDesc nTableDesc = new TableDesc(tables.get(0));
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("DEFAULT.TEST_COUNTRY");
        tableExt.updateRandomUuid();
        NTableExtDesc tableExtDesc = new NTableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(nTableDesc, tableExtDesc, "default");
        Assert.assertTrue(result.length == 1);
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
        tableExt.updateRandomUuid();
        NTableExtDesc tableExtDesc = new NTableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(tableDesc, tableExtDesc, "default");
        NTableMetadataManager nTableMetadataManager = NTableMetadataManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default");
        Assert.assertTrue(result.length == 1);
        val size = nTableMetadataManager.listAllTables().size();
        tableService.unloadTable("default", "DEFAULT.TEST_UNLOAD");

        Assert.assertEquals(null, nTableMetadataManager.getTableDesc("DEFAULT.TEST_UNLOAD"));
        Assert.assertEquals(size - 1, nTableMetadataManager.listAllTables().size());
    }

    @Test
    public void testGetSourceDbNames() throws Exception {
        List<String> dbNames = tableService.getSourceDbNames("default", 11);
        ArrayList<String> dbs = Lists.newArrayList(dbNames);
        Assert.assertTrue(dbs.contains("DEFAULT"));
    }

    @Test
    public void testGetSourceTableNames() throws Exception {
        List<String> tableNames = tableService.getSourceTableNames("default", "DEFAULT", 11, "");
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
        Assert.assertEquals(61, response.getRelatedIndexNum());
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
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");

        // case of no start time
        request.setStart(null);
        request.setEnd("1328054400000");
        tableService.setDataRange("default", request);
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals("0", dataLoadingRange.getCoveredRange().getStart().toString());
        Assert.assertEquals("1328054400000", dataLoadingRange.getCoveredRange().getEnd().toString());

        // case of load existing data
        tableService.setDataRange("default", mockeDateRangeRequestWithoutTime());
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        // 2014-01-01
        String latestDateInEpoch = "1388534400000";
        Assert.assertEquals(latestDateInEpoch, dataLoadingRange.getCoveredRange().getEnd().toString());
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

    private void testSetDataRangeOverlapOrGap() {
        DateRangeRequest request = mockDateRangeRequest();
        // 2012-02-01
        request.setStart("1328054400000");
        // 2012-03-01
        request.setEnd("1330560000000");
        try {
            tableService.setDataRange("default", request);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(
                    "NDataLoadingRange appendSegmentRange TimePartitionedSegmentRange[1328054400000,1330560000000) has overlaps/gap with existing segmentRanges TimePartitionedSegmentRange[0,1388534400000)",
                    ex.getMessage());
        }

        // case of having gap with current loading range
        request.setStart("1388534500000");
        request.setEnd("1388534600000");
        try {
            tableService.setDataRange("default", request);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(
                    "NDataLoadingRange appendSegmentRange TimePartitionedSegmentRange[1388534500000,1388534600000) has overlaps/gap with existing segmentRanges TimePartitionedSegmentRange[0,1388534400000)",
                    ex.getMessage());
        }
    }

    private void testGetLatestData() throws Exception {
        ExistedDataRangeResponse response = tableService.getLatestDataRange("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals("1388534400000", response.getEndTime());
    }

    @Test
    public void testSetDateRangeException() throws Exception {
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setTable("DEFAULT.TEST_ACCOUNT");
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("this table can not set date range, please check table");
        tableService.setDataRange("default", dateRangeRequest);
    }

    private void setupPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerJdbcImpl");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");
    }

    @Test
    public void testGetTableAndColumns() {
        List<TablesAndColumnsResponse> result = tableService.getTableAndColumns("default");
        Assert.assertEquals(11, result.size());
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
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        //table oriented model
        AutoMergeConfigResponse response = tableService.getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(response.getVolatileRange().getVolatileRangeNumber(), 0);
        Assert.assertEquals(response.isAutoMergeEnabled(), true);
        Assert.assertEquals(response.getAutoMergeTimeRanges().size(), 2);

        dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        //model Based model
        response = tableService.getAutoMergeConfigByModel("default", "nmodel_basic");
        Assert.assertEquals(response.getVolatileRange().getVolatileRangeNumber(), 0);
        Assert.assertEquals(response.isAutoMergeEnabled(), true);
        Assert.assertEquals(response.getAutoMergeTimeRanges().size(), 2);

    }

    @Test
    public void testSetAutoMergeConfigByTable() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
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
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        autoMergeRequest.setTable("");
        autoMergeRequest.setModel("nmodel_basic");
        tableService.setAutoMergeConfigByModel("default", autoMergeRequest);
        AutoMergeConfigResponse respone = tableService.getAutoMergeConfigByModel("default", "nmodel_basic");
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
        List<TableNameResponse> result = tableService.getTableNameResponses("default", "DEFAULT", 11, "");
        Assert.assertEquals(8, result.size());
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
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incremental loading, due to another incremental loading table existed in model 'all_fixed_length'!");
        tableService.setPartitionKey("DEFAULT.TEST_ACCOUNT", "default", "CAL_DT");
    }

    @Test
    public void testSetFact_IncrementingExists_Exception() {
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incremental loading, due to another incremental loading table existed in model 'all_fixed_length'!");
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
        Assert.assertEquals(modelManager.getDataModelDesc("nmodel_basic").getPartitionDesc().getPartitionDateColumn(),
                "TEST_KYLIN_FACT.CAL_DT");

        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        eventDao.deleteAllEvents();
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());

        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertNull(modelManager.getDataModelDesc("nmodel_basic").getPartitionDesc());
        val events = eventDao.getEvents();
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

    private void cleanPushdownEnv() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa",
                "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.dropAllTables();
        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }
}
