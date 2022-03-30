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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.TableRefresh;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
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
import io.kyligence.kap.rest.source.DataSourceState;
import io.kyligence.kap.rest.source.NHiveSourceInfo;
import io.kyligence.kap.streaming.jobs.StreamingJobListener;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableServiceTest extends CSVSourceTestCase {

    @InjectMocks
    private final TableService tableService = Mockito.spy(new TableService());

    @InjectMocks
    private final JobSupporter jobService = Mockito.spy(JobSupporter.class);

    @Mock
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private KafkaService kafkaServiceMock = Mockito.mock(KafkaService.class);

    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    private StreamingJobListener eventListener = new StreamingJobListener();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "modelService", modelService);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(tableService, "kafkaService", kafkaServiceMock);
        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);
        ReflectionTestUtils.setField(tableService, "jobService", jobService);
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
        try {
            new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            //
        }
        EventBusFactory.getInstance().register(eventListener, true);
    }

    @After
    public void tearDown() {
        EventBusFactory.getInstance().unregister(eventListener);
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../server-base/metastore_db"));
    }

    @Test
    public void testGetTableDesc() throws IOException {

        List<TableDesc> tableDesc = tableService.getTableDesc("default", true, "", "DEFAULT", true);
        Assert.assertEquals(11, tableDesc.size());
        List<TableDesc> tableDesc2 = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", false);
        Assert.assertEquals(1, tableDesc2.size());
        List<TableDesc> tables3 = tableService.getTableDesc("default", true, "", "", true);
        Assert.assertEquals(20, tables3.size());
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", true);
        Assert.assertEquals("TEST_KYLIN_FACT", tables.get(0).getName());
        Assert.assertEquals(5633024, ((TableDescResponse) tables.get(0)).getStorageSize());
        Assert.assertEquals(0, ((TableDescResponse) tables.get(0)).getTotalRecords());

        List<TableDesc> table2 = tableService.getTableDesc("default", true, "country", "DEFAULT", true);
        Assert.assertEquals("TEST_COUNTRY", table2.get(0).getName());
        Assert.assertEquals(0L, ((TableDescResponse) table2.get(0)).getStorageSize());

        val manager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val countryTable = manager.copyForWrite(manager.getTableDesc("DEFAULT.TEST_COUNTRY"));
        countryTable.setLastSnapshotPath("cannot/find/it");
        manager.updateTableDesc(countryTable);

        table2 = tableService.getTableDesc("default", true, "country", "DEFAULT", true);
        Assert.assertEquals("TEST_COUNTRY", table2.get(0).getName());
        Assert.assertEquals(0L, ((TableDescResponse) table2.get(0)).getStorageSize());

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
        tableExt.setJodID("949afe5d-0221-420f-92db-cdd91cb31ac8");
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
    public void testFilterSamplingRows() {
        final String tableIdentity = "DEFAULT.TEST_COUNTRY";
        final NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "newten");
        final TableDesc originTableDesc = tableMgr.getTableDesc(tableIdentity);
        AclTCR aclTCR = new AclTCR();
        AclTCR.Table table = new AclTCR.Table();
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        AclTCR.Column column = new AclTCR.Column();
        column.add("COUNTRY");
        column.add("LONGITUDE");
        column.add("NAME");
        columnRow.setColumn(column);
        // Equal Condition Row
        AclTCR.Row row = new AclTCR.Row();
        AclTCR.RealRow realRow = new AclTCR.RealRow();
        realRow.add("country_a");
        row.put("COUNTRY", realRow);
        columnRow.setRow(row);
        // Like Condition Row
        AclTCR.Row likeRow = new AclTCR.Row();
        AclTCR.RealRow likeRealRow = new AclTCR.RealRow();
        likeRealRow.add("name\\_\\%%");
        likeRow.put("NAME", likeRealRow);
        columnRow.setLikeRow(likeRow);

        table.put("DEFAULT.TEST_COUNTRY", columnRow);
        aclTCR.setTable(table);
        List<AclTCR> aclTCRs = Lists.newArrayList(aclTCR);
        TableDesc tableDesc = tableService.getAuthorizedTableDesc(getProject(), false, originTableDesc, aclTCRs);
        TableDescResponse tableDescResponse = new TableDescResponse(tableDesc);

        List<String[]> sampleRows = Lists.newArrayList();
        sampleRows.add(new String[] { "country_a", "10.10", "11.11", "name_%a" });
        sampleRows.add(new String[] { "country_b", "20.20", "22.22", "name_%b" });
        sampleRows.add(new String[] { "country_c", "30.30", "33.33", "name_%c" });
        sampleRows.add(new String[] { "country_d", "40.40", "44.44", "name_%d" });
        tableDescResponse.setSamplingRows(sampleRows);

        tableService.filterSamplingRows("newten", tableDescResponse, false, aclTCRs);

        Assert.assertEquals(1, tableDescResponse.getSamplingRows().size());
        Assert.assertEquals("country_a,11.11,name_%a", String.join(",", tableDescResponse.getSamplingRows().get(0)));
    }

    @Test
    public void testExtractTableMeta() {
        String[] tables = { "DEFAULT.TEST_ACCOUNT", "DEFAULT.TEST_KYLIN_FACT" };
        List<Pair<TableDesc, TableExtDesc>> result = tableService.extractTableMeta(tables, "default");
        Assert.assertEquals(2, result.size());

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Can’t find the table. Please check and try again");
        String[] emptyTables = new String[] { "" };
        tableService.extractTableMeta(emptyTables, "default");
    }

    @Test
    public void testExtraTableMetaException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t load table \"DEFAULT.NOT_EXISTS\". Please ensure that the table(s) could be found in the data source.");
        String[] notExistsTables = new String[] { "DEFAULT.NOT_EXISTS" };
        tableService.extractTableMeta(notExistsTables, "default");
    }

    @Test
    public void testLoadTableToProject() throws IOException {
        List<TableDesc> tables = tableService.getTableDesc("default", true, "TEST_COUNTRY", "DEFAULT", true);
        TableDesc nTableDesc = new TableDesc(tables.get(0));
        TableExtDesc tableExt = new TableExtDesc();
        tableExt.setIdentity("DEFAULT.TEST_COUNTRY");
        TableExtDesc tableExtDesc = new TableExtDesc(tableExt);
        String[] result = tableService.loadTableToProject(nTableDesc, tableExtDesc, "default");
        Assert.assertEquals(1, result.length);
    }

    @Test
    public void testLoadCaseSensitiveTableToProject() throws IOException {
        NTableMetadataManager tableManager = tableService.getManager(NTableMetadataManager.class, "case_sensitive");
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
        Assert.assertEquals(1, result.length);
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
    public void testUnloadTable() {
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
        Assert.assertEquals(1, result.length);
        val size = nTableMetadataManager.listAllTables().size();
        String unloadedTable = tableService.unloadTable("default", "DEFAULT.TEST_UNLOAD", false);
        Assert.assertEquals(tableDesc.getIdentity(), unloadedTable);

        Assert.assertNull(nTableMetadataManager.getTableDesc("DEFAULT.TEST_UNLOAD"));
        Assert.assertEquals(size - 1, nTableMetadataManager.listAllTables().size());
    }

    @Test
    public void testUnloadTable_RemoveDB() {
        String removeDB = "EDW";
        NProjectManager npr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        ProjectInstance projectInstance = npr.getProject("default");
        projectInstance.setDefaultDatabase(removeDB);
        npr.updateProject(projectInstance);
        Assert.assertEquals(removeDB, npr.getDefaultDatabase("default"));

        for (TableDesc table : tableManager.listAllTables()) {
            if (removeDB.equalsIgnoreCase(table.getDatabase())) {
                tableService.unloadTable("default", table.getIdentity(), false);
            }
        }

        Assert.assertEquals("DEFAULT", npr.getDefaultDatabase("default"));
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
        String tableNotExist = "DEFAULT.not_exist_table";
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableNotExist));
        tableService.unloadTable("default", tableNotExist, false);
    }

    @Test
    public void testPrepareUnloadNotExistTable() throws IOException {
        String tableNotExist = "DEFAULT.not_exist_table";
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, MsgPicker.getMsg().getTABLE_NOT_FOUND(), tableNotExist));
        tableService.preUnloadTable("default", tableNotExist);
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
        tableService.setPartitionKey(tableName, "default", "CAL_DT", "yyyy-MM-dd");
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
        cleanPushdownEnv();
    }

    @Test
    public void testUnloadKafkaTable() {
        String project = "streaming_test";
        NProjectManager npr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        StreamingJobManager mgr = StreamingJobManager.getInstance(getTestConfig(), project);
        var buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        var mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        var buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        var mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNotNull(buildJobMeta);
        Assert.assertNotNull(mergeJobMeta);
        for (TableDesc table : tableManager.listAllTables()) {
            if (table.getKafkaConfig() != null
                    && "P_LINEORDER_STR".equalsIgnoreCase(table.getKafkaConfig().getName())) {
                tableService.unloadTable(project, table.getIdentity(), true);
            }
        }
        buildJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_build";
        mergeJobId = "e78a89dd-847f-4574-8afa-8768b4228b72_merge";
        buildJobMeta = mgr.getStreamingJobByUuid(buildJobId);
        mergeJobMeta = mgr.getStreamingJobByUuid(mergeJobId);
        Assert.assertNull(buildJobMeta);
        Assert.assertNull(mergeJobMeta);
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
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", tableName);
    }

    @Test
    public void testSetPartitionKeyAndSetDataRange() throws Exception {
        setupPushdownEnv();
        testGetBatchLoadTablesBefore();
        testSetPartitionKeyAndSetDataRangeWithoutException();
        testGetBatchLoadTablesAfter();
        testSetDataRangeWhenNoNewData();
        testSetDataRangeOverlapOrGap();
        testgetPartitionColumnFormat();
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
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
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

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT));
        sdf.setTimeZone(TimeZone.getDefault());

        long t2 = sdf.parse("2014/01/01").getTime();

        Assert.assertEquals(t2, dataLoadingRange.getCoveredRange().getEnd());
    }

    @Test
    public void testSetPartitionKeyAndSetDataRangeWhenPushdownIsOff() {
        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");
        try {
            tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
            tableService.setDataRange("default", mockeDateRangeRequestWithoutTime());
        } catch (Exception ex) {
            Assert.assertFalse(ex.getCause() instanceof ClassNotFoundException);
        }
    }

    //test toggle partition Key,A to null, null to A ,A to B with model:with lag behind, without lag behind
    @Test
    public void testTogglePartitionKey_NullToNotNull() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");

        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());
        Assert.assertNull(loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(0, df.getSegments().size());
        val loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNotNull(loadingRange);
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", loadingRange.getColumnName());

    }

    @Test
    public void testTogglePartitionKey_OneToAnother() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(0, df.getSegments().size());

        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        val copy = loadingRangeMgr.copyForWrite(loadingRange);
        copy.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(0L, 100000L));
        loadingRangeMgr.updateDataLoadingRange(copy);

        //change partition

        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "ORDER_ID", "yyyy-MM-dd");
        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange.getCoveredRange());
        Assert.assertEquals("TEST_KYLIN_FACT.ORDER_ID", loadingRange.getColumnName());

    }

    @Test
    public void testTogglePartitionKey_OneToNull() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        loadingRangeMgr.createDataLoadingRange(loadingRange);

        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange.getCoveredRange());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", loadingRange.getColumnName());

        //set null
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "", "");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());

        val executables = getRunningExecutables("default", df.getUuid());
        Assert.assertEquals(1, executables.size());

        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange);
    }

    @Test
    public void testTogglePartitionKey_NullToOneWithLagBehindModel() {
        val dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        var df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
        dfMgr.updateDataflowStatus(df.getId(), RealizationStatusEnum.LAG_BEHIND);
        val loadingRangeMgr = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        var loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNull(loadingRange);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        loadingRange = loadingRangeMgr.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        Assert.assertNotNull(loadingRange);
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", loadingRange.getColumnName());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

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
            Assert.assertTrue(StringUtils.contains(ex.getMessage(), "There is no more new data to load"));
        }
    }

    private void testSetDataRangeOverlapOrGap() throws ParseException {

        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT));
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

    private void testgetPartitionColumnFormat() throws Exception {
        // Test on batch table
        String format = tableService.getPartitionColumnFormat("default", "DEFAULT.TEST_KYLIN_FACT", "CAL_DT");
        Assert.assertEquals("yyyy-MM-dd", format);

        // Test on streaming table
        NTableMetadataManager mgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        TableDesc desc = mgr.listAllTables().get(0);
        desc.setKafkaConfig(new KafkaConfig());

        val msg = "{\"a\": 2, \"b\": 2, \"minute_start\": \"2000-01-01 05:06:12\"}";
        val base64Msg = new String(Base64.encodeBase64(msg.getBytes()));
        ByteBuffer buffer = ByteBuffer.wrap(base64Msg.getBytes(StandardCharsets.UTF_8));
        List<ByteBuffer> encodedMessages = new ArrayList<>();
        encodedMessages.add(buffer);
        encodedMessages.add(buffer);
        List<String> messages = new ArrayList<>();
        messages.add(msg);
        messages.add(msg);
        when(kafkaServiceMock.getMessages(any(), any(String.class), any(Integer.class))).thenReturn(encodedMessages);

        Map<String, Object> mockResp = new HashMap<>();
        mockResp.put("message_type", true);
        mockResp.put("message", messages);
        when(kafkaServiceMock.getMessageTypeAndDecodedMessages(any())).thenReturn(mockResp);

        String format2 = tableService.getPartitionColumnFormat("default", "DEFAULT.STREAMING_TABLE", "MINUTE_START");
        Assert.assertEquals("yyyy-MM-dd HH:mm:ss", format2);

        when(kafkaServiceMock.getMessages(any(), any(String.class), any(Integer.class))).thenCallRealMethod();
        when(kafkaServiceMock.getMessageTypeAndDecodedMessages(any())).thenCallRealMethod();
        desc.setKafkaConfig(null);
    }

    private void testGetLatestData() throws Exception {
        java.text.DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd", Locale.getDefault(Locale.Category.FORMAT));
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
        Assert.assertEquals(20, result.size());
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
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT());
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        tableService.checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "1294364500000");
    }

    @Test
    public void checkRefreshDataRangeException2() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_REFRESH_SEGMENT_BY_NOT_READY());
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        NDataLoadingRangeManager rangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        NDataLoadingRange dataLoadingRange = rangeManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(1294364400000L, 1294364500000L);
        dataLoadingRange.setCoveredRange(segmentRange);
        NDataLoadingRange updateRange = rangeManager.copyForWrite(dataLoadingRange);
        rangeManager.updateDataLoadingRange(updateRange);
        tableService.checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "1294364500000");
    }

    @Test
    public void testGetAutoMergeConfigException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getMODEL_NOT_FOUND(), "default"));
        tableService.getAutoMergeConfigByModel("default", "default");
    }

    @Test
    public void testGetAutoMergeConfig() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        //table oriented model
        AutoMergeConfigResponse response = tableService.getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertEquals(0, response.getVolatileRange().getVolatileRangeNumber());
        Assert.assertFalse(response.isAutoMergeEnabled());
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

        dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.MODEL_BASED);
        dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        //model Based model
        response = tableService.getAutoMergeConfigByModel("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, response.getVolatileRange().getVolatileRangeNumber());
        Assert.assertFalse(response.isAutoMergeEnabled());
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

    }

    @Test
    public void testSetAutoMergeConfigByTable() {
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel dataModel = modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setManagementType(ManagementType.TABLE_ORIENTED);
        NDataModel dataModelUpdate = modelManager.copyForWrite(dataModel);
        modelManager.updateDataModelDesc(dataModelUpdate);
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
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
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        tableService.setPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT", true);
        boolean result = tableService.getPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT");
        Assert.assertTrue(result);
    }

    @Test
    public void testGetTableNameResponse_PASS() throws Exception {
        List<TableNameResponse> result = tableService.getTableNameResponses("default", "DEFAULT", "");
        Assert.assertEquals(11, result.size());
        Assert.assertTrue(result.get(0).isLoaded());

    }

    @Test
    public void testSetFact_NoRelatedModels_PASS() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "", "yyyy-MM-dd");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
    }

    @Test
    public void testSetFact_NotRootFactTable_Exception() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getINVALID_SET_TABLE_INC_LOADING(),
                "DEFAULT.TEST_ACCOUNT", "nmodel_basic_inner"));
        tableService.setPartitionKey("DEFAULT.TEST_ACCOUNT", "default", "CAL_DT", "yyyy-MM-dd");
    }

    @Test
    public void testSetFact_IncrementingExists_Exception() {
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getINVALID_SET_TABLE_INC_LOADING(),
                "DEFAULT.TEST_ACCOUNT", "nmodel_basic_inner"));
        tableService.setPartitionKey("DEFAULT.TEST_ACCOUNT", "default", "CAL_DT", "yyyy-MM-dd");
    }

    @Test
    public void testSetFact_HasRelatedModels_PASS() {
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), "default");
        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        val dataloadingManager = NDataLoadingRangeManager.getInstance(getTestConfig(), "default");
        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "", "");

        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "CAL_DT", "yyyy-MM-dd");
        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertEquals("TEST_KYLIN_FACT.CAL_DT", modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getPartitionDesc()
                .getPartitionDateColumn());

        Assert.assertTrue(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());
        Assert.assertNotNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));

        tableService.setPartitionKey("DEFAULT.TEST_KYLIN_FACT", "default", "", "");
        Assert.assertFalse(tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT").isIncrementLoading());

        Assert.assertNull(dataloadingManager.getDataLoadingRange("DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertNull(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getPartitionDesc());
        val executables = getRunningExecutables("default", null);
        Assert.assertEquals(4, executables.size());
    }

    @Test
    public void testGetLoadedDatabases() {
        Set<String> loadedDatabases = tableService.getLoadedDatabases("default");
        Assert.assertEquals(3, loadedDatabases.size());
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
        NInitTablesResponse response;
        response = tableService.getProjectTables("default", "SSB.SS", 0, 14, true,
                (databaseName, tableName) -> tableService.getTableNameResponses("default", databaseName, tableName));
        Assert.assertEquals(0, response.getDatabases().size());

        response = tableService.getProjectTables("default", "SSB.CU", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(1, response.getDatabases().size());
        Assert.assertEquals(1, response.getDatabases().get(0).getTables().size());

        response = tableService.getProjectTables("default", "", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(3, response.getDatabases().size());
        Assert.assertEquals(20, response.getDatabases().get(0).getTables().size()
                        + response.getDatabases().get(1).getTables().size() + response.getDatabases().get(2).getTables().size());

        response = tableService.getProjectTables("default", "TEST", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(2, response.getDatabases().size());
        Assert.assertEquals(13,
                response.getDatabases().get(0).getTables().size() + response.getDatabases().get(1).getTables().size());

        response = tableService.getProjectTables("default", "EDW.", 0, 14, true, (databaseName, tableName) -> {
            return tableService.getTableNameResponses("default", databaseName, tableName);
        });
        Assert.assertEquals(1, response.getDatabases().size());
        Assert.assertEquals(3, response.getDatabases().get(0).getTables().size());

        response = tableService.getProjectTables("default", "EDW.", 0, 14, false, (databaseName, tableName) -> {
            return tableService.getTableDesc("default", true, tableName, databaseName, true);
        });
        Assert.assertEquals(1, response.getDatabases().size());
        Assert.assertEquals(3, response.getDatabases().get(0).getTables().size());

        response = tableService.getProjectTables("default", "DEFAULT.TEST_ORDER", 0, 14, false,
                (databaseName, tableName) -> tableService.getTableDesc("default", true, tableName, databaseName, true));
        Assert.assertEquals(1, response.getDatabases().size());
        Assert.assertEquals(1, response.getDatabases().get(0).getTables().size());

        response = tableService.getProjectTables("default", ".TEST_ORDER", 0, 14, false,
                (databaseName, tableName) -> tableService.getTableDesc("default", true, tableName, databaseName, true));
        Assert.assertEquals(0, response.getDatabases().size());

    }

    @Test
    public void testClassifyDbTables() throws Exception {
        String project = "default";

        String[] tables1 = { "ssb", "ssb.KK", "DEFAULT", "DEFAULT.TEST", "DEFAULT.TEST_ACCOUNT" };
        Pair<String[], Set<String>> res = tableService.classifyDbTables(project, tables1);
        Assert.assertEquals("ssb", ((String[]) res.getFirst())[0]);
        Assert.assertEquals("DEFAULT", ((String[]) res.getFirst())[1]);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[2]);
        Assert.assertEquals(2, (res.getSecond()).size());

        String[] tables2 = { "KKK", "KKK.KK", ".DEFAULT", "DEFAULT.TEST", "DEFAULT.TEST_ACCOUNT" };
        res = tableService.classifyDbTables(project, tables2);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[0]);
        Assert.assertEquals(4, (res.getSecond()).size());

        String[] tables3 = { "DEFAULT.TEST_ACCOUNT", "SsB" };
        res = tableService.classifyDbTables(project, tables3);
        Assert.assertEquals("DEFAULT.TEST_ACCOUNT", ((String[]) res.getFirst())[0]);
        Assert.assertEquals("SsB", ((String[]) res.getFirst())[1]);
        Assert.assertEquals(0, (res.getSecond()).size());
    }

    @Test
    public void testGetTableNameResponsesInCache() throws Exception {
        Map<String, List<String>> testData = new HashMap<>();
        testData.put("t", Arrays.asList("aa", "ab", "bc"));
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        sourceInfo.setTables(testData);
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        DataSourceState.getInstance().putCache("ugi#" + ugi.getUserName(), sourceInfo);
        List<?> tables = tableService.getTableNameResponsesInCache("default", "t", "a");
        Assert.assertEquals(2, tables.size());
    }

    @Test
    public void testloadProjectHiveTableNameToCacheImmediately() throws Exception {
        List<?> tables = tableService.getTableNameResponsesInCache("default", "SSB", "");
        Assert.assertEquals(0, tables.size());

        KylinConfig.getInstanceFromEnv().setProperty("kylin.source.hive.databases", "default");
        Assert.assertEquals(1, KylinConfig.getInstanceFromEnv().getHiveDatabases().length);
        tableService.loadProjectHiveTableNameToCacheImmediately("default", true);
        tables = tableService.getTableNameResponsesInCache("default", "SSB", "");
        Assert.assertEquals(0, tables.size());

        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default").getConfig()
                .setProperty("kylin.source.hive.databases", "ssb");
        tableService.loadProjectHiveTableNameToCacheImmediately("default", true);
        tables = tableService.getTableNameResponsesInCache("default", "SSB", "");
        Assert.assertEquals(6, tables.size());

        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default").setPrincipal("default");
        tableService.loadHiveTableNameToCache();
        tables = tableService.getTableNameResponsesInCache("default", "EDW", "");
        Assert.assertEquals(0, tables.size());
    }

    @Test
    public void testloadProjectHiveTableNameToCacheImmediatelyCase2() throws Exception {
        List<?> tables = tableService.getTableNameResponsesInCache("default", "SSB", "");
        Assert.assertEquals(0, tables.size());
        tableService.loadProjectHiveTableNameToCacheImmediately("default", false);
        tables = tableService.getTableNameResponsesInCache("default", "SSB", "");
        Assert.assertEquals(0, tables.size());
    }

    @Test
    public void testGetTableNameResponsesInCacheJdbc() throws Exception {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "8");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        Map<String, List<String>> testData = new HashMap<>();
        testData.put("t", Arrays.asList("aa", "ab", "bc"));
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        sourceInfo.setTables(testData);
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        DataSourceState.getInstance().putCache("project#default", sourceInfo);
        List<?> tables = tableService.getTableNameResponsesInCache("default", "t", "a");
        Assert.assertEquals(2, tables.size());
    }

    @Test
    public void testCheckTableExistOrLoad() {
        TableDesc tableDesc = new TableDesc();
        tableDesc.setKafkaConfig(new KafkaConfig());
        TableNameResponse response = new TableNameResponse();
        tableService.checkTableExistOrLoad(response, tableDesc);
        Assert.assertTrue(response.isExisted());

        TableNameResponse response2 = new TableNameResponse();
        tableService.checkTableExistOrLoad(response2, null);
        Assert.assertFalse(response2.isExisted());

        TableNameResponse response3 = new TableNameResponse();
        tableService.checkTableExistOrLoad(response3, new TableDesc());
        Assert.assertTrue(response3.isLoaded());
    }

    @Test
    public void testIsSqlContainsColumns() {
        Assert.assertFalse(tableService.isSqlContainsColumns("a > 10", "DB.A", Sets.newHashSet("b")));
        Assert.assertTrue(tableService.isSqlContainsColumns("a > 10 AND b < 1", "DB.A", Sets.newHashSet("a", "b")));
        Assert.assertTrue(tableService.isSqlContainsColumns("a > 10 OR b < 1", "DB.A", Sets.newHashSet("b")));
        Assert.assertFalse(
                tableService.isSqlContainsColumns("A.a > 10 AND B.b < 1", "DB.C", Sets.newHashSet("a", "b")));
        Assert.assertTrue(
                tableService.isSqlContainsColumns("A.a  > 10 AND B.b < 1", "DB.A", Sets.newHashSet("a", "b", "c")));
        Assert.assertFalse(tableService.isSqlContainsColumns("(A.a > 10 AND B.b < 1) OR C.c != 'string'", "DB.B",
                Sets.newHashSet("a", "c")));
        Assert.assertFalse(tableService.isSqlContainsColumns("(A.a > 10 AND B.b < 1) OR C.c != 'string'", "DB.D",
                Sets.newHashSet("a", "b", "c")));
        Assert.assertTrue(tableService.isSqlContainsColumns("(A.a > 10) AND B.b < 1", "A", Sets.newHashSet("a")));
        Assert.assertTrue(
                tableService.isSqlContainsColumns("A.a  > 10 AND B.b < 1", "DB.A", Sets.newHashSet("a", "b", "c")));

    }

    @Test
    public void testRefreshSingleCatalogCache() {
        Map<String, List<String>> request = mockRefreshTable("DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_KYLIN_FAKE");
        TableRefresh tableRefresh = tableService.refreshSingleCatalogCache(request);
        Assert.assertEquals(KylinException.CODE_UNDEFINED, tableRefresh.getCode());
        Assert.assertEquals(1, tableRefresh.getRefreshed().size());
        Assert.assertEquals(1, tableRefresh.getFailed().size());
    }

    @Test
    public void testRefreshSparkTable() throws Exception {
        CliCommandExecutor command = new CliCommandExecutor();
        String warehousePath = getTestConfig().exportToProperties()
                .getProperty("kylin.storage.columnar.spark-conf.spark.sql.warehouse.dir").substring(5)
                + "/test_kylin_refresh/";
        PushDownUtil.trySimplePushDownExecute("drop table if exists test_kylin_refresh", null);
        PushDownUtil.trySimplePushDownExecute("create table test_kylin_refresh (word string) STORED AS PARQUET", null);
        PushDownUtil.trySimplePushDownExecute("insert into test_kylin_refresh values ('a')", null);
        PushDownUtil.trySimplePushDownExecute("insert into test_kylin_refresh values ('c')", null);
        PushDownUtil.trySimplePushDownExecute("select * from test_kylin_refresh", null);
        CliCommandExecutor.CliCmdExecResult res = command.execute("ls " + warehousePath, null, null);
        val files = Arrays.stream(res.getCmd().split("\n")).filter(file -> file.endsWith("parquet"))
                .collect(Collectors.toList());
        command.execute("rm " + warehousePath + files.get(0), null, null);

        try {
            PushDownUtil.trySimplePushDownExecute("select * from test_kylin_refresh", null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("REFRESH TABLE tableName"));
        }

        HashMap<String, Object> request = Maps.newHashMap();
        request.put("tables", Collections.singletonList("test_kylin_refresh"));
        TableRefresh refreshRes = tableService.refreshSingleCatalogCache(request);
        PushDownUtil.trySimplePushDownExecute("select * from test_kylin_refresh", null);
        Assert.assertEquals(1, refreshRes.getRefreshed().size());
        Assert.assertEquals("test_kylin_refresh", refreshRes.getRefreshed().get(0));
        SparderEnv.getSparkSession().stop();
    }

    private HashMap<String, List<String>> mockRefreshTable(String... tables) {
        Mockito.doAnswer(invocation -> {
            String table = invocation.getArgument(0);
            List<String> refreshed = invocation.getArgument(1);
            List<String> failed = invocation.getArgument(2);
            if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
                refreshed.add("DEFAULT.TEST_KYLIN_FACT");
            } else {
                failed.add(table);
            }
            return null;
        }).when(tableService).refreshTable(Mockito.any(), Mockito.any(), Mockito.any());
        HashMap<String, List<String>> request = Maps.newHashMap();
        request.put("tables", Arrays.asList(tables));
        return request;
    }

    @Test
    public void testGetHiveTableNameResponses() throws Exception {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("test", "test", Constant.ROLE_ANALYST));
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.source.load-hive-tablename-enabled", "false");
        config.setProperty("kylin.query.security.acl-tcr-enabled", "true");
        Assert.assertEquals(6, tableService.getHiveTableNameResponses("default", "SSB", "").size());
        Assert.assertEquals(11, tableService.getHiveTableNameResponses("default", "DEFAULT", "").size());

        val table = NTableMetadataManager.getInstance(getTestConfig(), "default").getTableDesc("DEFAULT.TEST_ENCODING");
        AclTCRManager manager = AclTCRManager.getInstance(getTestConfig(), "default");
        AclTCR acl = new AclTCR();
        AclTCR.Table aclTable = new AclTCR.Table();
        AclTCR.ColumnRow aclColumnRow = new AclTCR.ColumnRow();
        AclTCR.Column aclColumns = new AclTCR.Column();
        Arrays.stream(table.getColumns()).forEach(x -> aclColumns.add(x.getName()));
        aclColumnRow.setColumn(aclColumns);
        aclTable.put("DEFAULT.TEST_ENCODING", aclColumnRow);
        acl.setTable(aclTable);
        manager.updateAclTCR(acl, "test", true);

        Assert.assertEquals(6, tableService.getHiveTableNameResponses("default", "SSB", "").size());
        Assert.assertEquals(11, tableService.getHiveTableNameResponses("default", "DEFAULT", "").size());
        config.setProperty("kylin.source.load-hive-tablename-enabled", "true");
        config.setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }

    @Test
    public void testGetTableExtDescJobID() throws IOException {
        final String tableIdentity = "DEFAULT.TEST_COUNTRY";
        final NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), "newten");
        final TableDesc tableDesc = tableMgr.getTableDesc(tableIdentity);
        TableExtDesc oldExtDesc = tableMgr.getOrCreateTableExt(tableDesc);

        // mock table ext desc
        TableExtDesc tableExt = new TableExtDesc(oldExtDesc);
        tableExt.setIdentity(tableIdentity);
        tableExt.setJodID("949afe5d-0221-420f-92db-cdd91cb31ac8");
        tableMgr.mergeAndUpdateTableExt(oldExtDesc, tableExt);

        List<TableDesc> tables = tableService.getTableDesc("newten", true, "TEST_COUNTRY", "DEFAULT", true);
        Assert.assertEquals(1, tables.size());

        Assert.assertEquals("949afe5d-0221-420f-92db-cdd91cb31ac8", ((TableDescResponse) tables.get(0)).getJodID());
    }

    @Test
    public void testGetModelTables() {
        String project = "default";
        // normal model
        String modelName = "nmodel_basic";
        List<TableDesc> tableDescs = tableService.getTablesOfModel(project, modelName);
        Assert.assertEquals(10, tableDescs.size());

        // table deleted
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", Boolean.FALSE);
        tableDescs = tableService.getTablesOfModel(project, modelName);
        Assert.assertEquals(9, tableDescs.size());

        // model not exist
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT, Message.getInstance().getMODEL_NOT_FOUND(), "nomodel"));
        tableService.getTablesOfModel(project, "nomodel");
    }

    @Test
    public void testGetTableDescByType() {
        String project = "streaming_test";
        try {
            val tableDescs = tableService.getTableDescByType(project, true, "", "default", true, 1);
            Assert.assertNotNull(tableDescs);

            val tableDescs1 = tableService.getTableDescByType(project, true, "P_LINEORDER_STREAMING", "ssb", true, 1);
            Assert.assertEquals(1, tableDescs1.size());
            val tableDesc1 = tableDescs1.get(0);
            Assert.assertEquals(tableDesc1.getTableAlias(), tableDesc1.getKafkaConfig().getBatchTable());

            val tableDescs2 = tableService.getTableDescByType(project, true, "LINEORDER_HIVE", "SSB", false, 9);
            Assert.assertEquals(1, tableDescs2.size());
            val tableDesc2 = tableDescs2.get(0);
            Assert.assertEquals(tableDesc2.getTableAlias(), tableDesc2.getIdentity());
        } catch (Exception e) {
            Assert.fail();
        }

    }

    @Test
    public void testGetTableDescByTypes() {
        String project = "streaming_test";
        try {
            List<Integer> sourceTypes = Arrays.asList(1, 9);
            val tableDescs2 = tableService.getTableDescByTypes(project, true, "", "SSB", false, sourceTypes);
            assert tableDescs2.stream().filter(tableDesc -> tableDesc.getSourceType() == 1).collect(Collectors.toList())
                    .size() > 0;
            assert tableDescs2.stream().filter(tableDesc -> tableDesc.getSourceType() == 9).collect(Collectors.toList())
                    .size() > 0;
        } catch (Exception e) {
            Assert.fail();
        }

    }

    @Test
    public void testUnloadKafkaConfig() {
        String project = "streaming_test";
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), project);
        tableService.unloadTable(project, "DEFAULT.SSB_TOPIC", true);
        val table = tableMgr.getTableDesc("DEFAULT.SSB_TOPIC");
        Assert.assertNull(table);

        tableService.unloadTable(project, "SSB.LINEORDER_HIVE", true);
        val table1 = tableMgr.getTableDesc("SSB.P_LINEORDER_STREAMING");
        Assert.assertNull(table1);
    }

    @Test
    public void testStopStreamingJobByTables1() {
        String project = "streaming_test";

        val streamingJobMgr = StreamingJobManager.getInstance(getTestConfig(), project);
        val jobId = "4965c827-fbb4-4ea1-a744-3f341a3b030d_merge";
        Assert.assertEquals(JobStatusEnum.RUNNING, streamingJobMgr.getStreamingJobByUuid(jobId).getCurrentStatus());

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), project);
        tableService.unloadTable(project, "DEFAULT.SSB_STREAMING", false);
        val table = tableMgr.getTableDesc("DEFAULT.SSB_STREAMING");
        Assert.assertNull(table);
        Assert.assertEquals(JobStatusEnum.STOPPED, streamingJobMgr.getStreamingJobByUuid(jobId).getCurrentStatus());
    }

    @Test
    public void testStopStreamingJobByTables2() {
        String project = "streaming_test";

        val streamingJobMgr = StreamingJobManager.getInstance(getTestConfig(), project);
        val jobId = "4965c827-fbb4-4ea1-a744-3f341a3b030d_merge";
        Assert.assertEquals(JobStatusEnum.RUNNING, streamingJobMgr.getStreamingJobByUuid(jobId).getCurrentStatus());

        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(getTestConfig(), project);
        tableService.unloadTable(project, "SSB.LINEORDER_HIVE", false);
        val table = tableMgr.getTableDesc("SSB.P_LINEORDER_STREAMING");
        Assert.assertNotNull(table);
        Assert.assertEquals(JobStatusEnum.STOPPED, streamingJobMgr.getStreamingJobByUuid(jobId).getCurrentStatus());

    }
}
