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

import static io.kyligence.kap.rest.metrics.QueryMetricsContextTest.getInfluxdbFields;
import static org.apache.kylin.common.QueryContext.PUSHDOWN_HIVE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.ResourceLimitExceededException;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.querymeta.ColumnMeta;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.QueryCacheSignatureUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.hystrix.NCircuitBreaker;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryMetricsContext;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.query.engine.PrepareSqlStateParam;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.QueryRoutingEngine;
import io.kyligence.kap.query.engine.data.QueryResult;
import io.kyligence.kap.rest.cache.QueryCacheManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.service.NUserGroupService;
import lombok.val;
import net.sf.ehcache.CacheManager;

/**
 * @author xduo
 */
public class QueryServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final CacheManager cacheManager = Mockito
            .spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache.xml")));

    private final QueryCacheManager queryCacheManager = new QueryCacheManager();

    private final ClusterManager clusterManager = new DefaultClusterManager(8080);

    private QueryService origin;

    @InjectMocks
    private QueryService queryService;

    @InjectMocks
    private final AppConfig appConfig = Mockito.spy(new AppConfig());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    protected AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected UserService userService = Mockito.spy(KylinUserService.class);

    @Mock
    protected AclService aclService = Mockito.spy(AclService.class);

    private int pushdownCount;

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.query.transaction-enable", "true");
        overwriteSystemProp("kylin.query.cache-threshold-duration", String.valueOf(-1));
        overwriteSystemProp("HADOOP_USER_NAME", "root");

        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        origin = new QueryService();
        queryService = Mockito.spy(origin);
        queryService.queryRoutingEngine = Mockito.spy(QueryRoutingEngine.class);

        ReflectionTestUtils.setField(queryCacheManager, "cacheManager", cacheManager);
        ReflectionTestUtils.setField(queryService, "aclEvaluate", Mockito.mock(AclEvaluate.class));
        ReflectionTestUtils.setField(queryService, "queryCacheManager", queryCacheManager);
        ReflectionTestUtils.setField(queryService, "clusterManager", clusterManager);
        ReflectionTestUtils.setField(queryService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(queryService, "accessService", accessService);
        ReflectionTestUtils.setField(accessService, "userService", userService);
        ReflectionTestUtils.setField(accessService, "aclService", aclService);
        Mockito.when(appConfig.getPort()).thenReturn(7070);
        ReflectionTestUtils.setField(queryService, "appConfig", appConfig);
        pushdownCount = 0;

        userService.createUser(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(new UserGrantedAuthority("ROLE_ADMIN"))));
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    private void stubQueryConnection(final String sql, final String project) throws Exception {
        final QueryResult queryResult = Mockito.mock(QueryResult.class);
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        queryService.queryRoutingEngine = Mockito.mock(QueryRoutingEngine.class);
        Mockito.when(queryExec.executeQuery(sql)).thenReturn(queryResult);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project);
        Mockito.when(queryService.newQueryExec(project)).thenReturn(queryExec);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project, null);
        Mockito.when(queryService.newQueryExec(project, null)).thenReturn(queryExec);
        Mockito.when(queryService.queryRoutingEngine.queryWithSqlMassage(Mockito.any()))
                .thenReturn(new Pair<>(Lists.newArrayList(), Lists.newArrayList()));
    }

    private void stubQueryConnectionException() throws Exception {
        Mockito.when(queryService.queryRoutingEngine.queryWithSqlMassage(Mockito.any()))
                .thenThrow(new RuntimeException(new ResourceLimitExceededException("")));
    }

    @Test
    public void testQueryPushDownForced() throws Throwable {
        final String sql = "select * from abc";
        final String project = "default";
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql(sql);
        sqlRequest.setProject(project);
        sqlRequest.setForcedToPushDown(true);

        QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(sqlRequest.getProject()),
                sqlRequest.getSql(), sqlRequest.getProject(), sqlRequest.getLimit(), sqlRequest.getOffset(),
                queryExec.getSchema(), true);
        String correctedSql = QueryUtil.massageSql(queryParams);

        Mockito.when(queryExec.executeQuery(correctedSql))
                .thenThrow(new RuntimeException("shouldnt execute queryexec"));
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project);
        Mockito.when(queryService.newQueryExec(project)).thenReturn(queryExec);
        Mockito.doAnswer(x -> queryExec).when(queryService).newQueryExec(project, null);
        Mockito.when(queryService.newQueryExec(project, null)).thenReturn(queryExec);

        Mockito.doAnswer(invocation -> new Pair<List<List<String>>, List<SelectedColumnMeta>>(Collections.EMPTY_LIST,
                Collections.EMPTY_LIST)).when(queryService.queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        final SQLResponse response = queryService.doQueryWithCache(sqlRequest, false);

        Assert.assertTrue(response.isQueryPushDown());
    }

    @Test
    public void testQueryPushDownErrorMessage() throws Exception {
        final String sql = "select * from success_table_2";
        final String project = "default";

        Mockito.doAnswer(invocation -> {
            QueryContext.current().setPushdownEngine(PUSHDOWN_HIVE);
            throw new SQLException("push down error");
        }).when(queryService.queryRoutingEngine).tryPushDownSelectQuery(Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        final SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(response.isException());
        Assert.assertTrue(StringUtils.contains(response.getExceptionMessage(), "[HIVE Exception] push down error"));
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
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("Can't find project \"default0\". Please check and try again.", e.getMessage());
        }
    }

    @Test
    public void testQueryWithCacheFailedForSqlNotExist() {
        final String sql = "";
        final String notExistProject = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(notExistProject);
        request.setSql(sql);
        try {
            queryService.doQueryWithCache(request, false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals("SQL can’t be empty. Please check and try again.", e.getMessage());
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

    private void mockOLAPContextForEmptyLayout() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));

        Mockito.doReturn(modelManager).when(queryService).getDataModelManager("default");
        // mock empty index realization
        OLAPContext mock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Mockito.when(mockModel1.getAlias()).thenReturn("mock_model_alias1");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("mock_model1");
        IRealization mockRealization1 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization1.getModel()).thenReturn(mockModel1);
        mock.realization = mockRealization1;
        mock.storageContext.setEmptyLayout(true);
        mock.storageContext.setCandidate(NLayoutCandidate.EMPTY);
        mock.storageContext.setCuboidLayoutId(null);
        mock.storageContext.setPrunedSegments(Lists.newArrayList());
        OLAPContext.registerContext(mock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockOLAPContext() throws Exception {
        val modelManager = Mockito.spy(NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default"));

        Mockito.doReturn(modelManager).when(queryService).getDataModelManager("default");
        // mock agg index realization
        OLAPContext aggMock = new OLAPContext(1);
        NDataModel mockModel1 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel1.getUuid()).thenReturn("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Mockito.when(mockModel1.getAlias()).thenReturn("mock_model_alias1");
        Mockito.doReturn(mockModel1).when(modelManager).getDataModelDesc("mock_model1");
        IRealization mockRealization1 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization1.getModel()).thenReturn(mockModel1);
        aggMock.realization = mockRealization1;
        IndexEntity mockIndexEntity1 = new IndexEntity();
        mockIndexEntity1.setId(1);
        LayoutEntity mockLayout1 = new LayoutEntity();
        mockLayout1.setIndex(mockIndexEntity1);
        aggMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout1));
        aggMock.storageContext.setCuboidLayoutId(1L);
        aggMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(aggMock);

        // mock table index realization
        OLAPContext tableMock = new OLAPContext(2);
        NDataModel mockModel2 = Mockito.spy(new NDataModel());
        Mockito.when(mockModel2.getUuid()).thenReturn("741ca86a-1f13-46da-a59f-95fb68615e3a");
        Mockito.when(mockModel2.getAlias()).thenReturn("mock_model_alias2");
        Mockito.doReturn(mockModel2).when(modelManager).getDataModelDesc("mock_model2");
        IRealization mockRealization2 = Mockito.mock(IRealization.class);
        Mockito.when(mockRealization2.getModel()).thenReturn(mockModel2);
        tableMock.realization = mockRealization2;
        IndexEntity mockIndexEntity2 = new IndexEntity();
        mockIndexEntity2.setId(IndexEntity.TABLE_INDEX_START_ID + 1);
        LayoutEntity mockLayout2 = new LayoutEntity();
        mockLayout2.setIndex(mockIndexEntity2);
        tableMock.storageContext.setCandidate(new NLayoutCandidate(mockLayout2));
        tableMock.storageContext.setCuboidLayoutId(1L);
        tableMock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));
        OLAPContext.registerContext(tableMock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();
        mockQueryWithSqlMassage();
    }

    private void mockQueryWithSqlMassage() throws Exception {
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                return new Pair<>(Lists.newArrayList(), Lists.newArrayList());
            }
        }).when(queryService.queryRoutingEngine).queryWithSqlMassage(Mockito.any());
    }

    private void mockOLAPContextWithOneModelInfo(String modelId, String modelAlias, long layoutId) throws Exception {
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
        mock.storageContext.setPrunedSegments(Lists.newArrayList(new NDataSegment()));

        OLAPContext.registerContext(mock);

        Mockito.doNothing().when(queryService).clearThreadLocalContexts();

        mockQueryWithSqlMassage();
    }

    @Test
    public void testQueryWithTimeOutException() throws Exception {
        final String sql = "select * from exception_table";
        final String project = "newten";

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        Mockito.doThrow(new RuntimeException(new KylinTimeoutException("calcite timeout exception")))
                .when(queryService).query(request);

        final SQLResponse sqlResponse = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(sqlResponse.isException());
        String log = queryService.logQuery(request, sqlResponse);
        Assert.assertTrue(log.contains("Is Timeout: true"));
    }

    @Test
    public void testQueryWithCacheException() throws Throwable {
        final String sql = "select * from exception_table";
        final String project = "default";
        stubQueryConnection(sql, project);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        stubQueryConnectionException();
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
    public void testExposedColumnsProjectConfig() throws Exception {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        // expose computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "true"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertTrue(getColumnNames(factColumns).containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT",
                    "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }

        // hide computed column
        {
            projectManager.updateProject("default", copyForWrite -> copyForWrite.getOverrideKylinProps()
                    .put("kylin.query.metadata.expose-computed-column", "false"));
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            List<ColumnMeta> factColumns;
            ColumnDesc[] columnDescs = findColumnDescs();
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(columnDescs.length, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("DEAL_YEAR", "DEAL_AMOUNT",
                    "LEFTJOIN_BUYER_ID_AND_COUNTRY_NAME", "LEFTJOIN_SELLER_ID_AND_COUNTRY_NAME",
                    "LEFTJOIN_BUYER_COUNTRY_ABBR", "LEFTJOIN_SELLER_COUNTRY_ABBR")));
        }
    }

    @Test
    public void testExposedColumnsWhenPushdownDisabled() throws Exception {

        Pair<Set<String>, Set<String>> schemasAndTables;
        Set<String> tableSchemas, tableNames;
        List<ColumnMeta> factColumns;

        getTestConfig().setProperty("kylin.query.pushdown-enabled", "false");

        //we have two projects: testproject2 and testproject1. different projects exposes different views of
        //table, depending on what ready cube it has.
        {
            //check the default project
            final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");

            schemasAndTables = getSchemasAndTables(tableMetas);
            tableSchemas = schemasAndTables.getFirst();
            tableNames = schemasAndTables.getSecond();

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(20, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
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

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(20, tableNames.size());
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

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(20, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas);
            Assert.assertEquals(12, factColumns.size());
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

            Assert.assertEquals(3, tableSchemas.size());
            //make sure the schema "metadata" is not exposed
            Assert.assertTrue(!tableSchemas.contains("metadata"));
            Assert.assertEquals(20, tableNames.size());
            Assert.assertTrue(tableNames.contains("TEST_KYLIN_FACT"));

            //make sure test_kylin_fact contains all computed columns
            factColumns = getFactColumns(tableMetas4default);
            Assert.assertEquals(12, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
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
            Assert.assertEquals(columnDescs.length, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
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
            Assert.assertEquals(columnDescs.length, factColumns.size());
            Assert.assertFalse(getColumnNames(factColumns).containsAll(Arrays.asList("_CC_DEAL_YEAR", "_CC_DEAL_AMOUNT",
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
    public void testQueryWithConstants() throws Exception {
        String sql = "select price from test_kylin_fact where 1 <> 1";
        stubQueryConnection(sql, "default");

        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertEquals("CONSTANTS", response.getEngineType());
    }

    @Test
    public void testQueryWithEmptyLayout() throws Exception {
        String sql = "select price*item_count from test_kylin_fact where cal_dt = '2020-01-01' limit 100";
        stubQueryConnection(sql, "default");
        mockOLAPContextForEmptyLayout();

        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(sql);
        SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(1, response.getNativeRealizations().size());
        NativeQueryRealization realization = response.getNativeRealizations().get(0);
        Assert.assertEquals("mock_model_alias1", realization.getModelAlias());
        Assert.assertNull(realization.getLayoutId());
        Assert.assertNull(realization.getIndexType());
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
            Assert.assertEquals(KylinException.class, ex.getClass());
            Assert.assertEquals("Query named \"test\" already exists. Please check and try again.", ex.getMessage());
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
        Assert.assertEquals(
                String.valueOf(
                        dataflowManager.getDataflow(modelId).getLastSegment().getLayout(layoutId).getCreateTime()),
                signature.split(";")[0]);
        response.setSignature(signature);
        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCacheSignatureWhenModelOffline() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(
                Lists.newArrayList(new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX)));
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));

        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(response, project));
        //let model offline
        dataflowManager.updateDataflowStatus("89af4ee2-2cdb-4b07-b39e-4c29856309aa", RealizationStatusEnum.OFFLINE);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCacheSignatureWhenTableModified() {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val layoutId = 1000001L;
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        SQLResponse response = new SQLResponse();
        response.setNativeRealizations(
                Lists.newArrayList(new NativeQueryRealization(modelId, layoutId, QueryMetricsContext.AGG_INDEX)));
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));

        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(response, project));
        //modify table
        dataflowManager.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getModel().getRootFactTable().getTableDesc()
                .setLastModified(1);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testQueryWithCacheSignatureNotExpired() throws Exception {

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelAlias = "nmodel_basic";
        long layoutId = 1000001L;
        final String project = "default";
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        final String sql = "select * from success_table_1";
        stubQueryConnection(sql, project);
        mockOLAPContextWithOneModelInfo(modelId, modelAlias, layoutId);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        final SQLResponse firstSuccess = queryService.doQueryWithCache(request, false);

        // case of hitting cache
        final SQLResponse secondSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(secondSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, secondSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, secondSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals("nmodel_basic", secondSuccess.getNativeRealizations().get(0).getModelAlias());
    }

    @Test
    public void testQueryWithCacheSignatureExpired() throws Exception {

        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelAlias = "nmodel_basic";
        long layoutId = 1000001L;
        final String project = "default";
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);

        final String sql = "select * from success_table_2";
        stubQueryConnection(sql, project);
        mockOLAPContextWithOneModelInfo(modelId, modelAlias, layoutId);

        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);

        // case of not hitting cache
        final SQLResponse firstSuccess = queryService.doQueryWithCache(request, false);

        dataflowManager.updateDataflow(modelId, copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });
        // case of cache expired
        final SQLResponse thirdSuccess = queryService.doQueryWithCache(request, false);
        Assert.assertFalse(thirdSuccess.isStorageCacheUsed());
        Assert.assertEquals(1, thirdSuccess.getNativeRealizations().size());
        Assert.assertEquals(QueryMetricsContext.AGG_INDEX, thirdSuccess.getNativeRealizations().get(0).getIndexType());
        Assert.assertEquals(modelAlias, thirdSuccess.getNativeRealizations().get(0).getModelAlias());
    }

    @Test
    @Ignore
    public void testQueryWithResultRowCountBreaker() {
        final String sql = "select * from success_table_2";
        final String project = "default";
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        final SQLResponse response = Mockito.mock(SQLResponse.class);
        Mockito.doReturn(2L).when(response).getResultRowCount();

        getTestConfig().setProperty("kylin.circuit-breaker.threshold.query-result-row-count", "1");

        Mockito.doReturn(response).when(queryService).queryAndUpdateCache(Mockito.any(SQLRequest.class),
                Mockito.anyLong(), Mockito.anyBoolean());
        try {
            getTestConfig().setProperty("kylin.server.mode", "job");
            NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
            queryService.queryWithCache(request, false);
        } catch (KylinException e) {
            Assert.assertEquals("Job node is unavailable for queries. Please select a query node.", e.getMessage());
        }

        try {
            getTestConfig().setProperty("kylin.server.mode", "query");
            NCircuitBreaker.start(KapConfig.wrap(getTestConfig()));
            val queryWithCache = queryService.queryWithCache(request, false);
            Assert.assertTrue(queryWithCache.isException());
        } catch (Exception e) {
            Assert.fail();
        } finally {
            NCircuitBreaker.stop();
        }
    }

    @Test
    public void testQueryWithSpecificQueryId() throws Exception {
        final String sql = "select * from test";
        final String project = "default";
        final String queryId = UUID.randomUUID().toString();
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        request.setQueryId(queryId);

        final SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertEquals(queryId, response.getQueryId());
    }

    @Test
    public void testQueryLogMatch() {
        final String sql = "-- This is comment" + '\n' + "select * from test";
        final String project = "default";
        final String tag = "tagss";
        final String pushDownForced = "false";
        final SQLRequest request = new SQLRequest();
        request.setProject(project);
        request.setSql(sql);
        request.setUser_defined_tag(tag);
        HashMap<String, String> backdoorToggles = Maps.newHashMap();
        backdoorToggles.put("DEBUG_TOGGLE_HTRACE_ENABLED", "false");
        request.setBackdoorToggles(backdoorToggles);
        request.setUserAgent("Chrome/89.0.4389.82 Safari/537.36");

        final SQLResponse response = queryService.doQueryWithCache(request, false);

        // Current QueryContext will be reset in doQueryWithCache
        QueryContext.current().setUserSQL(sql);
        String log = queryService.logQuery(request, response);
        //
        final int groupCnt = 30;
        String matchNewLine = "\\n";
        String s = "(?s)[=]+\\[QUERY\\][=]+.*Query Id:\\s(.*?)" + matchNewLine + "SQL:\\s(.*?)" + matchNewLine
                + "User:\\s(.*?)" + matchNewLine + "Success:\\s(.*?)" + matchNewLine + "Duration:\\s(.*?)"
                + matchNewLine + "Project:\\s(.*?)" + matchNewLine + "Realization Names:\\s\\[(.*?)\\]" + matchNewLine
                + "Index Layout Ids:\\s\\[(.*?)\\]" + matchNewLine + "Is Partial Match Model:\\s\\[(.*?)\\]"
                + matchNewLine + "Scan rows:\\s(.*?)" + matchNewLine + "Total Scan rows:\\s(.*?)" + matchNewLine
                + "Scan bytes:\\s(.*?)" + matchNewLine + "Total Scan Bytes:\\s(.*?)" + matchNewLine
                + "Result Row Count:\\s(.*?)" + matchNewLine + "Shuffle partitions:\\s(.*?)" + matchNewLine
                + "Accept Partial:\\s(.*?)" + matchNewLine + "Is Partial Result:\\s(.*?)" + matchNewLine
                + "Hit Exception Cache:\\s(.*?)" + matchNewLine + "Storage Cache Used:\\s(.*?)" + matchNewLine
                + "Is Query Push-Down:\\s(.*?)" + matchNewLine + "Is Prepare:\\s(.*?)" + matchNewLine
                + "Is Timeout:\\s(.*?)" + matchNewLine + "Trace URL:\\s(.*?)" + matchNewLine
                + "Time Line Schema:\\s(.*?)" + matchNewLine + "Time Line:\\s(.*?)" + matchNewLine + "Message:\\s(.*?)"
                + matchNewLine + "User Defined Tag:\\s(.*?)" + matchNewLine + "Is forced to Push-Down:\\s(.*?)"
                + matchNewLine + "User Agent:\\s(.*?)" + matchNewLine + "Back door toggles:\\s(.*?)" + matchNewLine
                + "[=]+\\[QUERY\\][=]+.*";
        Pattern pattern = Pattern.compile(s);
        Matcher matcher = pattern.matcher(log);

        Assert.assertTrue(matcher.find());
        for (int i = 0; i < groupCnt; i++)
            Assert.assertNotNull(matcher.group(i));
        Assert.assertEquals(groupCnt, matcher.groupCount());

        Assert.assertEquals(QueryContext.current().getQueryId(), matcher.group(1));
        Assert.assertEquals(sql, matcher.group(2));
        Assert.assertEquals(project, matcher.group(6));
        Assert.assertFalse(Boolean.parseBoolean(matcher.group(4)));
        Assert.assertEquals("null", matcher.group(23)); //Trace URL
        Assert.assertEquals(tag, matcher.group(27));
        Assert.assertEquals(pushDownForced, matcher.group(28));
        Assert.assertEquals("Chrome/89.0.4389.82 Safari/537.36", matcher.group(29));
        Assert.assertEquals("{DEBUG_TOGGLE_HTRACE_ENABLED=false}", matcher.group(30));
    }

    @Test
    public void testQueryWithParam() {
        final String sql = "select * from test where col1 = ?";
        final String project = "default";
        final PrepareSqlRequest request = new PrepareSqlRequest();
        request.setProject(project);
        request.setSql(sql);
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[1];
        params[0] = new PrepareSqlStateParam(String.class.getCanonicalName(), "value1");
        request.setParams(params);

        final SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        response.setEngineType("NATIVE");

        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "true");
        queryService.queryWithCache(request, false);

        overwriteSystemProp("kylin.query.replace-dynamic-params-enabled", "false");
        queryService.queryWithCache(request, false);

        final QueryContext queryContext = QueryContext.current();
        String filledSql = "select * from test where col1 = 'value1'";
        Assert.assertEquals(queryContext.getUserSQL(), filledSql);
        Assert.assertEquals(queryContext.getMetrics().getCorrectedSql(), filledSql);
        queryContext.getMetrics().setCorrectedSql(filledSql);
        QueryMetricsContext.start(queryContext.getQueryId(), "localhost:7070");
        Assert.assertTrue(QueryMetricsContext.isStarted());

        final QueryMetricsContext metricsContext = QueryMetricsContext.collect(queryContext);

        final Map<String, Object> influxdbFields = getInfluxdbFields(metricsContext);
        Assert.assertEquals(filledSql, influxdbFields.get(QueryHistory.SQL_TEXT));
        QueryMetricsContext.reset();
    }

    @Test
    public void testQueryIDShouldBeDifferentAfterReset() {
        QueryContext curOld = QueryContext.current();
        QueryContext.reset();
        QueryContext curNew = QueryContext.current();
        Pattern uuid_p = Pattern
                .compile("([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}){1}");
        Assert.assertNotNull(curNew);
        Assert.assertTrue(StringUtils.isNotEmpty(curNew.getQueryId()));

        Matcher matcher = uuid_p.matcher(curNew.getQueryId());
        Assert.assertTrue(matcher.find());

        Assert.assertNotEquals(curOld.getQueryId(), curNew.getQueryId());
    }

    @Test
    public void testMetaData() throws IOException {
        final List<TableMeta> tableMetas = queryService.getMetadata("default");
        // TEST_MEASURE table has basically all possible column types
        String metaString = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_MEASURE")).findFirst().get().toString();

        File expectedMetaFile = new File("src/test/resources/ut_table_meta/defaultTableMetas");
        String expectedMetaString = FileUtils.readFileToString(expectedMetaFile);
        Assert.assertEquals(expectedMetaString, metaString);
    }

    @Test
    public void testMetaDataV2() throws IOException {
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");
        // TEST_MEASURE table has basically all possible column types
        String metaString = tableMetas.stream().filter(t -> t.getTABLE_SCHEM().equalsIgnoreCase("DEFAULT"))
                .filter(t -> t.getTABLE_NAME().equalsIgnoreCase("TEST_MEASURE")).findFirst().get().toString();

        File expectedMetaFile = new File("src/test/resources/ut_table_meta/defaultTableMetasV2");
        String expectedMetaString = FileUtils.readFileToString(expectedMetaFile);
        Assert.assertEquals(expectedMetaString, metaString);
    }

    @Test
    //ref KE-12803
    public void testDeepCopy() {
        final List<TableMetaWithType> tableMetas = queryService.getMetadataV2("default");
        tableMetas.stream()
                .map(tableMetaWithType -> JsonUtil.deepCopyQuietly(tableMetaWithType, TableMetaWithType.class))
                .collect(Collectors.toList());
    }

    @Test
    //reference KE-8052
    public void testQueryWithConstant() throws SQLException {
        doTestQueryWithConstant("select current_timestamp");
        doTestQueryWithConstant("select 1,2,3,4,5");

    }

    private void doTestQueryWithConstant(String testSql) {
        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(testSql);
        request.setQueryId(UUID.randomUUID().toString());

        Predicate<SQLResponse> scannedRows = (s -> s.getTotalScanRows() == 0);
        Predicate<SQLResponse> scannedBytes = (s -> s.getTotalScanBytes() == 0);

        final SQLResponse response = queryService.doQueryWithCache(request, false);
        Assert.assertTrue(scannedRows.and(scannedBytes).test(response));
    }

    @Test
    //reference KE-8052
    public void testQueryWithScanBytesAndRows() {
        long defaultValue = QueryContext.DEFAULT_NULL_SCANNED_DATA;

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setScanRows(Arrays.asList(1L, 2L));
        sqlResponse.setScanBytes(Arrays.asList(2L, 3L));
        sqlResponse.setTotalScanBytes(QueryContext.calScannedValueWithDefault(sqlResponse.getScanBytes()));
        sqlResponse.setTotalScanRows(QueryContext.calScannedValueWithDefault(sqlResponse.getScanRows()));

        Assert.assertEquals(3L, sqlResponse.getTotalScanRows());
        Assert.assertEquals(5L, sqlResponse.getTotalScanBytes());

        SQLResponse sqlResponseNull = new SQLResponse();
        sqlResponseNull.setScanRows(null);
        sqlResponseNull.setScanBytes(null);
        sqlResponseNull.setTotalScanBytes(QueryContext.calScannedValueWithDefault(sqlResponseNull.getScanBytes()));
        sqlResponseNull.setTotalScanRows(QueryContext.calScannedValueWithDefault(sqlResponseNull.getScanRows()));

        Assert.assertEquals(sqlResponseNull.getTotalScanRows(), defaultValue);
        Assert.assertEquals(sqlResponseNull.getTotalScanBytes(), defaultValue);

        SQLResponse sqlResponseEmpty = new SQLResponse();

        sqlResponseEmpty.setScanRows(Collections.emptyList());
        sqlResponseEmpty.setScanBytes(Collections.emptyList());
        sqlResponseEmpty.setTotalScanBytes(QueryContext.calScannedValueWithDefault(sqlResponseEmpty.getScanBytes()));
        sqlResponseEmpty.setTotalScanRows(QueryContext.calScannedValueWithDefault(sqlResponseEmpty.getScanRows()));

        Assert.assertEquals(0, sqlResponseEmpty.getTotalScanRows());
        Assert.assertEquals(0, sqlResponseEmpty.getTotalScanBytes());

        QueryContext queryContext = QueryContext.current();
        queryContext.getMetrics().updateAndCalScanRows(Arrays.asList(1L, 2L));
        queryContext.getMetrics().updateAndCalScanBytes(Arrays.asList(2L, 3L));

        Assert.assertEquals(3L, queryContext.getMetrics().getScannedRows());
        Assert.assertEquals(5L, queryContext.getMetrics().getScannedBytes());

    }

    @Test
    public void testGetMetadataV2WithBrokenModels() {
        String modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        List<TableMetaWithType> metaWithTypeList = queryService.getMetadataV2("default");
        boolean noFactTableType = metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME())).findFirst().get()
                .getTYPE().isEmpty();
        Assert.assertFalse(noFactTableType);

        // fact table is broken
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel brokenModel = modelManager.getDataModelDesc(modelId);
        brokenModel.setBroken(true);
        brokenModel.setBrokenReason(NDataModel.BrokenReason.SCHEMA);
        modelManager.updateDataBrokenModelDesc(brokenModel);

        metaWithTypeList = queryService.getMetadataV2("default");
        noFactTableType = metaWithTypeList.stream()
                .filter(tableMetaWithType -> "TEST_MEASURE".equals(tableMetaWithType.getTABLE_NAME())).findFirst().get()
                .getTYPE().isEmpty();
        Assert.assertTrue(noFactTableType);
    }

    @Test
    public void testExecuteAsUserSwitchOff() {
        overwriteSystemProp("kylin.query.query-with-execute-as", "false");
        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setExecuteAs("unknown");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Configuration item \"kylin.query.query-with-execute-as\" "
                + "is not enabled. So you cannot use the \"executeAs\" parameter now");
        queryService.doQueryWithCache(request, false);
    }

    @Test
    public void testExecuteAsUserServiceAccountAccessDenied() {
        try {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
            final SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql("select 2");
            request.setExecuteAs("ADMIN");
            getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
            request.setQueryId(UUID.randomUUID().toString());
            thrown.expect(KylinException.class);
            thrown.expectMessage("User [testuser] does not have permissions for all tables, rows, "
                    + "and columns in the project [default] and cannot use the executeAs parameter");
            queryService.doQueryWithCache(request, false);
        } finally {
            SecurityContextHolder.getContext()
                    .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        }
    }

    @Test
    public void testExecuteAsUserAccessDenied() {
        final SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql("select 2");
        getTestConfig().setProperty("kylin.query.query-with-execute-as", "true");
        request.setQueryId(UUID.randomUUID().toString());
        userService.createUser(new ManagedUser("testuser", "KYLIN", false, Arrays.asList()));
        userService.userExists("testuser");
        request.setExecuteAs("testuser");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Access is denied.");
        queryService.doQueryWithCache(request, false);
    }

    @Test
    public void testQueryWithAdminPermission() {
        QueryService queryService = Mockito.spy(new QueryService());
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setExecuteAs("ADMIN");
        sqlRequest.setProject("default");
        sqlRequest.setSql("select 1");

        overwriteSystemProp("kylin.query.security.acl-tcr-enabled", "true");
        // role admin
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("ROLE_ADMIN"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        // project admin permission
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), true)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        // normal user
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertFalse(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }

        overwriteSystemProp("kylin.query.security.acl-tcr-enabled", "false");
        // acl disabled
        {
            Mockito.doReturn(new QueryContext.AclInfo("ADMIN", Sets.newHashSet("FOO"), false)).when(queryService)
                    .getExecuteAclInfo("default", "ADMIN");
            Assert.assertTrue(
                    queryService.isACLDisabledOrAdmin("default", queryService.getExecuteAclInfo("default", "ADMIN")));
        }
    }

    @Test
    public void testQuerySelectStar() {
        overwriteSystemProp("kylin.query.return-empty-result-on-select-star", "true");
        String[] select_star_sqls = { "select * from TEST_KYLIN_FACT", "select * from TEST_ACCOUNT",
                "select * from TEST_KYLIN_FACT inner join TEST_ACCOUNT on TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID"
        };
        for (String sql : select_star_sqls) {
            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(sql);
            request.setQueryId(UUID.randomUUID().toString());

            final SQLResponse response = queryService.doQueryWithCache(request, false);
            Assert.assertEquals(0, response.getResults().size());
        }
    }
}
