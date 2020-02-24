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

import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.metric.InfluxDBWriter;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.cluster.DefaultClusterManager;
import io.kyligence.kap.rest.config.AppConfig;
import io.kyligence.kap.rest.service.KapQueryService;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import io.kyligence.kap.shaded.influxdb.okhttp3.Interceptor;
import io.kyligence.kap.shaded.influxdb.okhttp3.MediaType;
import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.okhttp3.Protocol;
import io.kyligence.kap.shaded.influxdb.okhttp3.Request;
import io.kyligence.kap.shaded.influxdb.okhttp3.Response;
import io.kyligence.kap.shaded.influxdb.okhttp3.ResponseBody;
import io.kyligence.kap.shaded.influxdb.okio.Buffer;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FavoriteQueryTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    private final String SHOW_DATABASES = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"KE_HISTORY\"]]}]}]}\n";

    private KapQueryService queryService;
    private List<QueryHistory> queryHistories = new ArrayList<>();
    private CountDownLatch countDown;

    @Before
    public void setup() {
        createTestMetadata();
        System.setProperty("kylin.query.cache-enabled", "false");

        queryService = Mockito.spy(KapQueryService.class);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(queryService, "aclEvaluate", Mockito.mock(AclEvaluate.class));

        AppConfig appConfig = Mockito.spy(new AppConfig());
        ClusterManager clusterManager = new DefaultClusterManager(7070);

        ReflectionTestUtils.setField(queryService, "clusterManager", clusterManager);

        Mockito.when(appConfig.getPort()).thenReturn(7070);
        ReflectionTestUtils.setField(queryService, "appConfig", appConfig);

        ReflectionTestUtils.setField(InfluxDBWriter.class, "influxDB", mockInfluxDB());
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
        System.clearProperty("kylin.query.cache-enabled");
    }

    private void mockQueryException(QueryService queryService, String sql, Throwable throwable) throws Exception {
        final QueryExec queryExec = Mockito.mock(QueryExec.class);
        Mockito.when(queryExec.executeQuery(sql)).thenThrow(throwable);
        Mockito.when(queryService.newQueryExec(PROJECT)).thenReturn(queryExec);
    }


    private InfluxDB mockInfluxDB() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                final Request request = chain.request();
                final URL url = request.url().url();
                if ("/ping".equals(url.getPath())) {
                    return mockPingSuccess(request);
                }

                if (url.toString().contains("SHOW+DATABASES")) {
                    return mockShowDatabases(request);
                }

                if ("/write".equals(url.getPath())) {
                    Buffer buffer = new Buffer();
                    if (request.body() != null) {
                        request.body().writeTo(buffer);
                    }
                    byte[] bytes = buffer.readByteArray();
                    String content = new String(bytes);
                    log.info("content: {}", content);
                    parseAndeRecord(content);
                    countDown.countDown();
                    return mockWriteSuccess(request);
                }

                return chain.proceed(request);
            }
        });

        return InfluxDBFactory.connect("http://localhost:8086", "root", "root", client);
    }

    private Response mockPingSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }

    private Response mockShowDatabases(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), SHOW_DATABASES)).build();
    }

    private Response mockWriteSuccess(final Request request) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), "")).build();
    }

    private void parseAndeRecord(String content) {
        String[] props = content.split(",");
        QueryHistory queryHistory = new QueryHistory();
        for (String prop : props) {
            if (prop.startsWith("error_type")) {
                String errorType = prop.substring(prop.indexOf('=') + 1).replace("\\", "");
                queryHistory.setErrorType(errorType);
            }
            if (prop.startsWith("query_status")) {
                String queryStatus = prop.substring(prop.indexOf('=') + 2, prop.length() - 1);
                queryHistory.setQueryStatus(queryStatus);
            }
            if (prop.startsWith("sql_text")) {
                String sqlText = prop.substring(prop.indexOf('"') + 1, prop.length() - 1);
                queryHistory.setSql(sqlText);
                queryHistory.setSqlPattern(sqlText);
            }
        }
        queryHistories.add(queryHistory);
    }

    @Test
    public void testSyntaxErrorQuery() throws Exception {
        countDown = new CountDownLatch(1);
        SQLException exception = new SQLException(new SqlValidatorException("sql syntax error", new RuntimeException()));
        String sql1 = "select * from test_kylin_fact";
        mockQueryException(queryService, sql1, exception);
        SQLRequest request = new SQLRequest();
        request.setProject(PROJECT);
        request.setSql(sql1);
        queryService.doQueryWithCache(request, false);

        countDown.await();

        countDown = new CountDownLatch(1);
        exception = new SQLException(new NoRealizationFoundException("time out", new RuntimeException()));
        String sql2 = "select * from test_account";
        mockQueryException(queryService, sql2, exception);
        request = new SQLRequest();
        request.setProject(PROJECT);
        request.setSql("select * from test_account");
        Mockito.when(queryService.tryPushDownSelectQuery(request, null, exception, false))
                .thenThrow(new KylinTimeoutException("time out"));
        queryService.doQueryWithCache(request, false);

        countDown.await();


        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals(QueryHistory.SYNTAX_ERROR, queryHistories.get(0).getErrorType());
        Assert.assertEquals(QueryHistory.NO_REALIZATION_FOUND_ERROR, queryHistories.get(1).getErrorType());


        NFavoriteScheduler favoriteScheduler = Mockito.spy(new NFavoriteScheduler(PROJECT));
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(queryHistories).when(queryHistoryDAO).getQueryHistoriesByTime(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(queryHistoryDAO).when(favoriteScheduler).getQueryHistoryDao();

        ReflectionTestUtils.invokeMethod(favoriteScheduler, "initFrequencyStatus");

        Assert.assertEquals(1, favoriteScheduler.getOverAllStatus().getSqlPatternFreqMap().size());

    }

}

