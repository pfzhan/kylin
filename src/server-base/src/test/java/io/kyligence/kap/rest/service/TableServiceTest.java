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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.response.TableNameResponse;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.junit.AfterClass;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableDesc;
import io.kyligence.kap.metadata.model.NTableExtDesc;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.AutoMergeConfigResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.test.util.ReflectionTestUtils;

@Slf4j
public class TableServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private TableService tableService = Mockito.spy(new TableService());

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() throws Exception {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(tableService, "modelService", modelService);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "11");
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setOverrideKylinProps(overrideKylinProps);
        projectManager.updateProject(projectInstanceUpdate);
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testGetTableDesc() throws Exception {

        List<TableDesc> tableDesc = tableService.getTableDesc("default", true, "", "DEFAULT", true);
        Assert.assertEquals(true, tableDesc.size() == 8);
        List<TableDesc> tableDesc2 = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", false);
        Assert.assertEquals(1, tableDesc2.size());
        List<TableDesc> tables3 = tableService.getTableDesc("default", true, "", "", true);
        Assert.assertEquals(true, tables3.size() == 11);
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", true);
        Assert.assertEquals(true, tables.get(0).getName().equals("TEST_COUNTRY"));
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
        NTableDesc nTableDesc = new NTableDesc(tables.get(0));
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
    public void testSetFactAndSetDataRange() throws Exception {
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        List<TableDesc> tables = tableService.getTableDesc("default", false, "", "DEFAULT", true);
        //test set fact and table list order by fact
        Assert.assertTrue(tables.get(0).getName().equals("TEST_KYLIN_FACT") && tables.get(0).getFact());
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(0L, 1294364500000L);
        segmentRanges.add(segmentRange);
        dataLoadingRange.setSegmentRanges(segmentRanges);
        dataLoadingRange.setWaterMarkStart(-1);
        dataLoadingRange.setWaterMarkEnd(0);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        tableService.setDataRange(mockDateRangeRequest());
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(dataLoadingRange.getActualQueryStart(), 0);
        Assert.assertEquals(dataLoadingRange.getActualQueryEnd(), 1294364500000L);
    }

    @Test
    public void testSetDateRangeException() throws IOException, PersistentException {
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setTable("DEFAULT.TEST_ACCOUNT");
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("this table can not set date range, please check table");
        tableService.setDataRange(dateRangeRequest);
    }

    @Test
    public void testSetDateRangeException3() throws IOException, PersistentException {
        //test some building segments in tail,and set dataRange smaller
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(0L, 1294364500000L);
        segmentRanges.add(segmentRange);
        SegmentRange segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(0L, 1294364600000L);
        segmentRanges.add(segmentRange2);
        dataLoadingRange.setSegmentRanges(segmentRanges);
        dataLoadingRange.setWaterMarkStart(-1);
        //segment2 building
        dataLoadingRange.setWaterMarkEnd(0);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("0");
        dateRangeRequest.setEnd("1294364700000");
        tableService.setDataRange(dateRangeRequest);
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(0, dataLoadingRange.getActualQueryStart());
        Assert.assertEquals(1294364500000L, dataLoadingRange.getActualQueryEnd());
        //shrink head
        dateRangeRequest.setStart("100");
        tableService.setDataRange(dateRangeRequest);
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(100, dataLoadingRange.getActualQueryStart());
        Assert.assertEquals(1294364500000L, dataLoadingRange.getActualQueryEnd());
        //shrink tail
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Some segments is building, can not set data range smaller than before");
        dateRangeRequest.setEnd("1294364600000");
        tableService.setDataRange(dateRangeRequest);
    }

    @Test
    public void testSetDateRangeException4() throws IOException, PersistentException {
        //test some building segments in header,and set dataRange smaller
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(1294364400000L, 1294364500000L);
        segmentRanges.add(segmentRange);
        SegmentRange segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(1294364500000L, 1294364600000L);
        segmentRanges.add(segmentRange2);
        dataLoadingRange.setSegmentRanges(segmentRanges);
        dataLoadingRange.setWaterMarkStart(0);
        //segment1 is building
        dataLoadingRange.setWaterMarkEnd(1);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("1294364300000");
        dateRangeRequest.setEnd("1294364700000");
        tableService.setDataRange(dateRangeRequest);
        dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(1294364500000L, dataLoadingRange.getActualQueryStart());
        Assert.assertEquals(1294364600000L, dataLoadingRange.getActualQueryEnd());
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Some segments is building, can not set data range smaller than before");
        dateRangeRequest.setStart("1294364350000");
        tableService.setDataRange(dateRangeRequest);
    }

    @Test
    public void testGetTableAndColumns() {
        List<TablesAndColumnsResponse> result = tableService.getTableAndColomns("default");
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
    public void checkRefreshDataRangeException1() throws IOException, PersistentException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("There is no ready segment to refresh!");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "YYMM");
        tableService.checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "1294364500000");
    }

    @Test
    public void checkRefreshDataRangeException2() throws IOException, PersistentException {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("Data during refresh range must be ready!");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        List<SegmentRange> segmentRanges = new ArrayList<>();
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(1294364400000L, 1294364500000L);
        segmentRanges.add(segmentRange);
        dataLoadingRange.setSegmentRanges(segmentRanges);
        dataLoadingRange.setWaterMarkStart(-1);
        dataLoadingRange.setWaterMarkEnd(0);
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
    public void testGetAutoMergeConfig() throws IOException, PersistentException {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
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
    public void testSetAutoMergeConfigByTable() throws IOException, PersistentException {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        tableService.setAutoMergeConfigByTable(autoMergeRequest);
        AutoMergeConfigResponse respone = tableService.getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(respone.isAutoMergeEnabled(), autoMergeRequest.isAutoMergeEnabled());
        Assert.assertEquals(respone.getAutoMergeTimeRanges().size(), autoMergeRequest.getAutoMergeTimeRanges().length);
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeNumber(),
                autoMergeRequest.getVolatileRangeNumber());
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeType().toString(),
                autoMergeRequest.getVolatileRangeType());

    }

    @Test
    public void testSetAutoMergeConfigByModel() throws IOException {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("nmodel_basic");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        autoMergeRequest.setTable("");
        autoMergeRequest.setModel("nmodel_basic");
        tableService.setAutoMergeConfigByModel(autoMergeRequest);
        AutoMergeConfigResponse respone = tableService.getAutoMergeConfigByModel("default", "nmodel_basic");
        Assert.assertEquals(respone.isAutoMergeEnabled(), autoMergeRequest.isAutoMergeEnabled());
        Assert.assertEquals(respone.getAutoMergeTimeRanges().size(), autoMergeRequest.getAutoMergeTimeRanges().length);
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeNumber(),
                autoMergeRequest.getVolatileRangeNumber());
        Assert.assertEquals(respone.getVolatileRange().getVolatileRangeType().toString(),
                autoMergeRequest.getVolatileRangeType());

    }

    @Test
    public void testSetPushDownMode() throws IOException, PersistentException {
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
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
    public void testSetFact_NoRelatedModels_PASS() throws Exception {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", false, "", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").getFact());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").getFact());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
    }

    @Test
    public void testSetFact_NotRootFactTable_Exception() throws Exception {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", false, "", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").getFact());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incrementing loading, due to another incrementing loading table existed in model 'all_fixed_length'!");
        tableService.setFact("DEFAULT.TEST_ACCOUNT", "default", true, "CAL_DT", "");
    }

    @Test
    public void testSetFact_IncrementingExists_Exception() throws Exception {
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage(
                "Can not set table 'DEFAULT.TEST_ACCOUNT' incrementing loading, due to another incrementing loading table existed in model 'all_fixed_length'!");
        tableService.setFact("DEFAULT.TEST_ACCOUNT", "default", true, "CAL_DT", "");
    }

    @Test
    public void testSetFact_HasRelatedModels_PASS() throws Exception {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", false, "", "");

        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", true, "CAL_DT", "Format1");
        Assert.assertEquals(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT").getPartitionDateFormat(),
                "Format1");
        Assert.assertTrue(modelManager.getDataModelDesc("nmodel_basic").getPartitionDesc().getPartitionDateFormat()
                .equals("Format1"));
        Assert.assertEquals(modelManager.getDataModelDesc("nmodel_basic").getPartitionDesc().getPartitionDateColumn(),
                "TEST_KYLIN_FACT.CAL_DT");

        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").getFact());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));

        val eventDao = EventDao.getInstance(getTestConfig(), "default");
        eventDao.deleteAllEvents();
        tableService.setFact("DEFAULT.TEST_KYLIN_FACT", "default", false, "", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").getFact());

        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertNull(modelManager.getDataModelDesc("nmodel_basic").getPartitionDesc());
        val events = eventDao.getEvents();
        Assert.assertEquals(4, events.size());
        Assert.assertEquals(0L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        Assert.assertEquals(Long.MAX_VALUE, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));
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
        request.setStart("0");
        request.setEnd("1294450900000");
        request.setProject("default");
        request.setTable("DEFAULT.TEST_KYLIN_FACT");
        return request;
    }
}
