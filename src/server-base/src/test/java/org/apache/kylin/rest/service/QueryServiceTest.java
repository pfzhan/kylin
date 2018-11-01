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
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import net.sf.ehcache.CacheManager;


/**
 * @author xduo
 */
public class QueryServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private CacheManager cacheManager = Mockito.spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache-test.xml")));

    @InjectMocks
    private QueryService queryService = Mockito.spy(new QueryService());

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("kylin.query.cache-threshold-duration", String.valueOf(-1));
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        SecurityContextHolder.getContext().setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

        ReflectionTestUtils.setField(queryService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
        ReflectionTestUtils.setField(queryService, "cacheManager", cacheManager);
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
        Mockito.when(queryService.tryPushDownSelectQuery(project, sql, null, sqlException, false)).
                thenReturn(new Pair<List<List<String>>, List<SelectedColumnMeta>>(Collections.EMPTY_LIST, Collections.EMPTY_LIST));

    }

    private void stubQueryConnectionException(final String project) throws Exception {
        Mockito.when(queryService.getConnection(project)).thenThrow(new RuntimeException(new ResourceLimitExceededException("")));
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
        Assert.assertEquals(true, response.isPushDown());
        Assert.assertEquals(expectedQueryID, response.getQueryId());

    }

    @Test
    public void testQueryWithCache() throws Exception {
        final String sql = "select * from success_table";
        final String project = "default";
        stubQueryConnection(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        String expectedQueryID = QueryContext.current().getQueryId();
        final SQLResponse firstSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(expectedQueryID, firstSuccess.getQueryId());

        expectedQueryID = QueryContext.current().getQueryId();
        final SQLResponse secondSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(true, secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(expectedQueryID, secondSuccess.getQueryId());


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
            Assert.assertEquals(true, response.getIsException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
        } catch (InternalErrorException ex) {
            // ignore
        }

        try {
            final String expectedQueryID = QueryContext.current().getQueryId();
            final SQLResponse response = queryService.doQueryWithCache(request, false);
            Assert.assertEquals(true, response.isHitExceptionCache());
            Assert.assertEquals(true, response.getIsException());
            Assert.assertEquals(expectedQueryID, response.getQueryId());
        } catch (InternalErrorException ex) {
            // ignore
        }
    }

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

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

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
            Assert.assertEquals(21, factColumns.size());
        }

        //disable the one ready cube
        {
            NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
            NDataflow dataflow = dataflowManager.getDataflow("ncube_basic");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
            nDataflowUpdate.setStatus(RealizationStatusEnum.OFFLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("ncube_basic_inner");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
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
            NDataflow dataflow = dataflowManager.getDataflow("ncube_basic");
            NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
            nDataflowUpdate.setStatus(RealizationStatusEnum.ONLINE);
            dataflowManager.updateDataflow(nDataflowUpdate);
            dataflow = dataflowManager.getDataflow("ncube_basic_inner");
            nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
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
            Assert.assertEquals(21, factColumns.size());
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownEnabled() throws Exception {

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");

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
            Assert.assertEquals(21, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME",
                            "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME", "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        //add a new model with new cc
        {
            NDataModel dKapModel = makeModelWithMoreCC();
            modelManager.updateDataModelDesc(dKapModel);

            //wait for broadcast
            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(11 + columnDescs.length, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns).containsAll(
                    Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                            "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR", "DEAL_YEAR_PLUS_ONE")));
        }

        //remove a cc from model
        {
            NDataModel dKapModel = makeModelWithLessCC();
            modelManager.updateDataModelDesc(dKapModel);

            //wait for broadcast
            Thread.sleep(1000);

            final List<TableMetaWithType> tableMetas4default = queryService.getMetadataV2("default");
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(10 + columnDescs.length, factColumns.size());
            Assert.assertTrue(getColumnNames(factColumns)
                    .containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT", "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME",
                            "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME", "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }
    }

    private ColumnDesc[] findColumnDescs() {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        tableMetadataManager.resetProjectSpecificTableDesc();
        TableDesc tableDesc = tableMetadataManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        ColumnDesc[] columnDescs = tableDesc.getColumns();
        return columnDescs;
    }

    private NDataModel makeModelWithLessCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic_inner");
        Serializer<NDataModel> dataModelSerializer = NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelSerializer();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        dataModelSerializer.serialize(model, new DataOutputStream(baos));
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        NDataModel dKapModel = dataModelSerializer.deserialize(new DataInputStream(bais));

        dKapModel.getComputedColumnDescs().remove(dKapModel.getComputedColumnDescs().size() - 1);
        return dKapModel;
    }

    private NDataModel makeModelWithMoreCC() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager.getDataModelDesc("nmodel_basic_inner");
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
        Optional<TableMetaWithType> factTable = FluentIterable.from(tableMetas)
                .filter(new Predicate<TableMetaWithType>() {
                    @Override
                    public boolean apply(@Nullable TableMetaWithType tableMetaWithType) {
                        return tableMetaWithType.getTABLE_NAME().equals("TEST_KYLIN_FACT");
                    }
                }).first();
        Assert.assertTrue(factTable.isPresent());
        return factTable.get().getColumns();
    }

    private ImmutableSet<String> getColumnNames(List<ColumnMeta> columns) {
        return FluentIterable.from(columns).transform(new Function<ColumnMeta, String>() {
            @Nullable
            @Override
            public String apply(@Nullable ColumnMeta columnMeta) {
                return columnMeta.getCOLUMN_NAME();
            }
        }).toSet();
    }

}
