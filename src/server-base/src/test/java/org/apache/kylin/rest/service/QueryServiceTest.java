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


import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.service.QueryHistoryService;
import net.sf.ehcache.CacheManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;


/**
 * @author xduo
 */
public class QueryServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private CacheManager cacheManager = Mockito.spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache-test.xml")));

    @Mock
    private QueryHistoryService queryHistoryService = Mockito.spy(QueryHistoryService.class);

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
        ReflectionTestUtils.setField(queryService, "queryHistoryService", queryHistoryService);
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
}
