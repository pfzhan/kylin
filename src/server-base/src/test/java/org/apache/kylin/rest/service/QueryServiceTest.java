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

package org.apache.kylin.rest.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.metrics.QueryMetricsContext;
import lombok.val;
import net.sf.ehcache.CacheManager;

/**
 * @author xduo
 */
public class QueryServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private CacheManager cacheManager = Mockito
            .spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache-test.xml")));

    private ClusterManager clusterManager = new DefaultClusterManager(8080);

    @InjectMocks
    private QueryService queryService = Mockito.spy(new QueryService());

    @InjectMocks
    private AppConfig appConfig = Mockito.spy(new AppConfig());

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("kylin.query.cache-threshold-duration", String.valueOf(-1));
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(queryService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
        ReflectionTestUtils.setField(queryService, "cacheManager", cacheManager);
        ReflectionTestUtils.setField(queryService, "clusterManager", clusterManager);
        Mockito.when(appConfig.getPort()).thenReturn(7070);
        ReflectionTestUtils.setField(queryService, "appConfig", appConfig);
    }

    private void stubQueryConnection(final String sql, final String project) throws SQLException {
        final ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);
        Mockito.when(metaData.getColumnCount()).thenReturn(0);

        final ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(metaData);

        final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        Mockito.when(statement.executeQuery()).thenReturn(resultSet);
        Mockito.when(statement.executeQuery(sql)).thenReturn(resultSet);

        final Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(sql)).thenReturn(statement);

        Mockito.when(queryService.getConnection(project)).thenReturn(connection);
    }

    private void stubQueryConnectionSQLException(final String sql, final String project) throws Exception {
        final SQLException sqlException = new SQLException();

        final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        Mockito.when(statement.executeQuery()).thenThrow(sqlException);
        Mockito.when(statement.executeQuery(sql)).thenThrow(sqlException);

        final Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(sql)).thenReturn(statement);

        Mockito.when(queryService.getConnection(project)).thenReturn(connection);

        // mock PushDownUtil
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        Mockito.when(queryService.tryPushDownSelectQuery(sqlRequest, null, sqlException, false)).thenReturn(
                new Pair<List<List<String>>, List<SelectedColumnMeta>>(Collections.EMPTY_LIST, Collections.EMPTY_LIST));

    }

    private void stubPushDownException(final String sql, final String project) throws Exception {
        final SQLException sqlException = new SQLException();

        final PreparedStatement statement = Mockito.mock(PreparedStatement.class);
        Mockito.when(statement.executeQuery()).thenThrow(sqlException);
        Mockito.when(statement.executeQuery(sql)).thenThrow(sqlException);

        final Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement(sql)).thenReturn(statement);

        Mockito.when(queryService.getConnection(project)).thenReturn(connection);

        QueryContext.current().setPushdownEngine("HIVE");

        // mock PushDownUtil
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        Mockito.when(queryService.tryPushDownSelectQuery(sqlRequest, null, sqlException, false))
                .thenThrow(new SQLException("push down error"));
    }

    private void stubQueryConnectionException(final String project) throws Exception {
        Mockito.when(queryService.getConnection(project))
                .thenThrow(new RuntimeException(new ResourceLimitExceededException("")));
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testQueryPushDown() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        stubQueryConnectionSQLException(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final String expectedQueryID = QueryContext.current().getQueryId();

        final SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(true, response.isQueryPushDown());
        Assert.assertEquals(expectedQueryID, response.getQueryId());

        Mockito.verify(queryService).recordMetric(request, response);
    }

    @Test
    public void testQueryPushDownErrorMessage() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";
        stubPushDownException(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(response.isException());
        Assert.assertEquals("[HIVE Exception] push down error", response.getExceptionMessage());
    }

    @Test
    public void testQueryWithCacheFailedForProjectNotExist() {
        final String sql = "select * from success_table";
        final String notExistProject = "default0";
        final SQLRequest request = new SQLRequest();
        request.setProject(notExistProject);
        request.setSql(sql);
        try {
            queryService.doQueryWithCache(request, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TransactionException);
            Assert.assertTrue(e.getCause() instanceof BadRequestException);
            Assert.assertEquals("Cannot find project 'default0'.", e.getCause().getMessage());
        }
    }

    @Test
    public void testQueryWithCacheFailedForSqlNotExist(){
        final String sql = "";
        final String notExistProject = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(notExistProject);
        request.setSql(sql);
        try {
            queryService.doQueryWithCache(request, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof TransactionException);
            Assert.assertTrue(e.getCause() instanceof BadRequestException);
            Assert.assertEquals("SQL should not be empty.", e.getCause().getMessage());
        }
    }

    @Test
    public void testQueryWithCache() throws Exception {

        final String sql = "select * from success_table";
        final String project = "default";
        stubQueryConnection(sql, project);
        mockOLAPContext();

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        String expectedQueryID = QueryContext.current().getQueryId();
        final SQLResponse firstSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(expectedQueryID, firstSuccess.getQueryId());
        Assert.assertEquals(2, firstSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, firstSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(QueryMetricsContext.TABLE_INDEX,
                firstSuccess.getNativeRealizations().get(1).getIndexType());
        Assert.assertEquals(Lists.newArrayList("mock_model_alias1", "mock_model_alias2"),
                firstSuccess.getNativeRealizations().stream().map(NativeQueryRealization::getModelAlias)
                        .collect(Collectors.toList()));
        // assert log info
        String log = queryService.logQuery(request, firstSuccess);
        Assert.assertTrue(log.contains("mock_model_alias1"));
        Assert.assertTrue(log.contains("mock_model_alias2"));

        // case of hitting cache
        expectedQueryID = QueryContext.current().getQueryId();
        final SQLResponse secondSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(true, secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(expectedQueryID, secondSuccess.getQueryId());
        Assert.assertEquals(2, secondSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, secondSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(QueryMetricsContext.TABLE_INDEX,
                secondSuccess.getNativeRealizations().get(1).getIndexType());
        Assert.assertEquals("mock_model_alias1", secondSuccess.getNativeRealizations().get(0).getModelAlias());
        // assert log info
        log = queryService.logQuery(request, secondSuccess);
        Assert.assertTrue(log.contains("mock_model_alias1"));
        Assert.assertTrue(log.contains("mock_model_alias2"));
    }

    private void mockOLAPContext() {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));
        Mockito.doReturn(modelManager).when(queryService).getDataModelManager("default");
        for (long i = 1; i <= 2; i++) {
            final OLAPContext mock = new OLAPContext((int) i);

            final NDataModel mockModel = Mockito.spy(new NDataModel());
            Mockito.when(mockModel.getUuid()).thenReturn("mock_model" + i);
            Mockito.when(mockModel.getAlias()).thenReturn("mock_model_alias" + i);
            Mockito.doReturn(mockModel).when(modelManager).getDataModelDesc("mock_model" + i);
            final IRealization mockRealization = Mockito.mock(IRealization.class);
            Mockito.when(mockRealization.getModel()).thenReturn(mockModel);
            mock.realization = mockRealization;

            final IndexEntity mockIndexEntity = new IndexEntity();
            if (i == 1) {
                // agg index
                mockIndexEntity.setId(i);
            } else {
                // table index
                mockIndexEntity.setId(IndexEntity.TABLE_INDEX_START_ID + i);
            }
            final LayoutEntity mockLayout = new LayoutEntity();
            mockLayout.setIndex(mockIndexEntity);
            mock.storageContext.setCandidate(new NLayoutCandidate(mockLayout));
            mock.storageContext.setCuboidLayoutId(i);

            OLAPContext.registerContext(mock);
        }

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
    }

    private void mockOLAPContextWithOneModelInfo(String modelId, String modelAlias, long layoutId) {
        final OLAPContext mock = new OLAPContext(1);

        final NDataModel mockModel = Mockito.spy(new NDataModel());
        Mockito.when(mockModel.getUuid()).thenReturn(modelId);
        Mockito.when(mockModel.getAlias()).thenReturn(modelAlias);
        final IRealization mockRealization = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization.getModel()).thenReturn(mockModel);
        mock.realization = mockRealization;

        final IndexEntity mockIndexEntity = new IndexEntity();
        mockIndexEntity.setId(layoutId);
        final LayoutEntity mockLayout = new LayoutEntity();
        mockLayout.setIndex(mockIndexEntity);
        mock.storageContext.setCandidate(new NLayoutCandidate(mockLayout));
        mock.storageContext.setCuboidLayoutId(layoutId);

        OLAPContext.registerContext(mock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
    }

    @Test
    public void testQueryWithTimeOutException() throws Exception {
        final String sql = "select * from exception_table";
        final String project = "newten";

        Mockito.when(queryService.getConnection(project))
                .thenThrow(new RuntimeException(new KylinTimeoutException("calcite timeout exception")));

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final SQLResponse sqlResponse = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(sqlResponse.isException());
        String log = queryService.logQuery(request, sqlResponse);
        Assert.assertTrue(log.contains("Is Timeout: true"));
    }

    @Test
    public void testQueryWithCacheException() throws Exception {
        final String sql = "select * from exception_table";
        final String project = "default";
        stubQueryConnection(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        stubQueryConnectionException(project);
        try {
            final String expectedQueryID = QueryContext.current().getQueryId();
            final SQLResponse response = queryService.doQueryWithCache(request, false);
            Assert.assertEquals(false, response.isHitExceptionCache());
            Assert.assertEquals(true, response.isException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
            Mockito.verify(queryService).recordMetric(request, response);
        } catch (InternalErrorException ex) {
            // ignore
        }

        try {
            final String expectedQueryID = QueryContext.current().getQueryId();
            final SQLResponse response = queryService.doQueryWithCache(request, false);
            Assert.assertEquals(true, response.isHitExceptionCache());
            Assert.assertEquals(true, response.isException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
        } catch (InternalErrorException ex) {
            // ignore
        }
    }

    @Ignore("see issue 11326")
    @Test
    public void testCreateTableToWith() throws IOException {
        String create_table1 = " create table tableId as select * from some_table1;";
        String create_table2 = "CREATE TABLE tableId2 AS select * FROM some_table2;";
        String select_table = "select * from tableId join tableId2 on tableId.a = tableId2.b;";

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.convert-create-table-to-with", "true");
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(create_table1);
            queryService.doQueryWithCache(request, false);

            request.setSql(create_table2);
            queryService.doQueryWithCache(request, false);

            request.setSql(select_table);
            SQLResponse response = queryService.doQueryWithCache(request, true);

            Assert.assertEquals(
                    "WITH tableId as (select * from some_table1) , tableId2 AS (select * FROM some_table2) select * from tableId join tableId2 on tableId.a = tableId2.b;",
                    response.getExceptionMessage());
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownDisabled() throws Exception {
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");

        //we have two projects: testproject2 and testproject1. different projects exposes different views of
        //table, depending on what ready cube it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(9, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(22, factColumns.size());
        }

        //disable the one ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(1, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(1, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_MEASURE"));
        }

        // enable the ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);

            Thread.sleep(1000);

            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(9, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(22, factColumns.size());
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownEnabled() throws Exception {

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");

        //we have two projects: default and testproject1. different projects exposes different views of
        //table, depending on what model it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas4default);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(2, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(11, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(23, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        //add a new model with new cc
        {
            NDataModel dKapModel = makeModelWithMoreCC();
            modelManager.updateDataModelDesc(dKapModel);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12 + columnDescs.length, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        //remove a cc from model
        {
            NDataModel dKapModel = makeModelWithLessCC();
            modelManager.updateDataModelDesc(dKapModel);

            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default");
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(11 + columnDescs.length, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
                    "_CC_LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "_CC_LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "_CC_LEFTJOIN_BUYER_COUNTRY_ABBR", "_CC_LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }
    }

    private ColumnDesc[] findColumnDescs() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "default");
        tableMetadataManager.resetProjectSpecificTableDesc();
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc[] columnDescs = tableDesc.getColumns();
        return columnDescs;
    }

    private NDataModel makeModelWithLessCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Serializer<NDataModel> dataModelSerializer = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel dKapModel = dataModelSerializer.deserialize(new DataInputStream(bais));

        dKapModel.getComputedColumnDescs().remove(dKapModel.getComputedColumnDescs().size() - 1);
        dKapModel.setMvcc(model.getMvcc());
        return dKapModel;
    }

    private NDataModel makeModelWithMoreCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Serializer<NDataModel> dataModelSerializer = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel dKapModel = dataModelSerializer.deserialize(new DataInputStream(bais));

        String newCCStr = " {\n" + "      \"tableIdentity\": \"DEFAULT.TEST_KYLIN_FACT\",\n"
                + "      \"tableAlias\": \"TEST_KYLIN_FACT\",\n" + "      \"columnName\": \"DEAL_YEAR_PLUS_ONE\",\n"
                + "      \"expression\": \"year(TEST_KYLIN_FACT.CAL_DT)+1\",\n" + "      \"datatype\": \"integer\",\n"
                + "      \"comment\": \"test use\"\n" + "    }";
        ComputedColumnDesc computedColumnDesc = JsonUtil.readValue(newCCStr, ComputedColumnDesc.class);
        dKapModel.getComputedColumnDescs().add(computedColumnDesc);
        dKapModel.setMvcc(model.getMvcc());
        return dKapModel;
    }

    private Pair<Set<String>, Set<String>> getSchemasAndTables(List<TableMetaWithType> tableMetas) {
        Set<String> tableSchemas = Sets.newHashSet();
        Set<String> tableNames = Sets.newHashSet();
        for (TableMetaWithType tableMetaWithType : tableMetas) {
            tableSchemas.add(tableMetaWithType.getTABLE_SCHEM());
            tableNames.add(tableMetaWithType.getTABLE_NAME());
        }

        return Pair.newPair(tableSchemas, tableNames);
    }

    private List<ColumnMeta> getFactColumns(List<TableMetaWithType> tableMetas) {
        Optional<TableMetaWithType> factTable = tableMetas.stream()
                .filter(tableMetaWithType -> tableMetaWithType.getTABLE_NAME().equals("TEST_KYLIN_FACT")).findFirst();
        Assert.assertTrue(factTable.isPresent());
        return factTable.get().getColumns();
    }

    private Set<String> getColumnNames(List<ColumnMeta> columns) {
        return columns.stream().map(ColumnMeta::getCOLUMN_NAME).collect(Collectors.toSet());
    }

    @Test
    public void testQueryWithConstants() throws IOException, SQLException {
        String sql = "select price from test_kylin_fact where 1 <> 1";
        stubQueryConnection(sql, "default");

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertEquals("CONSTANTS", response.getEngineType());
    }

    @Test
    public void testSaveQuery() throws IOException {
        Query query = new Query("test", "default", "test_sql", "test_description");
        queryService.saveQuery("admin", "default", query);
        QueryService.QueryRecord queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(1, queryRecord.getQueries().size());
        Assert.assertEquals("test", queryRecord.getQueries().get(0).getName());

        query.setSql("test_sql_2");
        try {
            queryService.saveQuery("admin", "default", query);
        } catch (Exception ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals("Duplicate query name 'test'", ex.getMessage());
        }

        queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(1, queryRecord.getQueries().size());
        Assert.assertEquals("test", queryRecord.getQueries().get(0).getName());
    }

    @Test
    public void testSaveLargeQuery() throws IOException {
        for (int i = 0; i < 10; i++) {
            Query query = new Query("test-" + i, "default", StringUtils.repeat("abc", 10000), "test_description");
            queryService.saveQuery("admin", "default", query);
        }
        QueryService.QueryRecord queryRecord = queryService.getSavedQueries("admin", "default");
        Assert.assertEquals(10, queryRecord.getQueries().size());
        for (Query query : queryRecord.getQueries()) {
            Assert.assertEquals(StringUtils.repeat("abc", 10000), query.getSql());
        }
    }

    @Test
    public void testCacheSignature() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(
                Lists.newArrayList(new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX)));
        String signature = QueryCacheSignatureUtil.createCacheSignature(response, project);
        Assert.assertEquals(String.valueOf(dataflowManager.getDataflow(modelId).getLastModified()), signature);

        response.setSignature(signature);
        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        Assert.assertEquals(true, QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testQueryWithCacheSignatureExpired() throws Exception {
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelAlias = "nmodel_basic";
        long layoutId = 1000001L;
        final String project = "default";
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        final String sql = "select * from success_table";
        stubQueryConnection(sql, project);
        mockOLAPContextWithOneModelInfo(modelId, modelAlias, layoutId);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        final SQLResponse firstSuccess = queryService.doQueryWithCache(request, false);

        // case of hitting cache
        final SQLResponse secondSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(true, secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, secondSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, secondSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals("nmodel_basic", secondSuccess.getNativeRealizations().get(0).getModelAlias());

        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        // case of cache expired
        final SQLResponse thirdSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(false, thirdSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, thirdSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, thirdSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(modelAlias, thirdSuccess.getNativeRealizations().get(0).getModelAlias());
    }

}
