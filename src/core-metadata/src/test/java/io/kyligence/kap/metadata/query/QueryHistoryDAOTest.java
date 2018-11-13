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

package io.kyligence.kap.metadata.query;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.shaded.influxdb.okhttp3.Interceptor;
import io.kyligence.kap.shaded.influxdb.okhttp3.MediaType;
import io.kyligence.kap.shaded.influxdb.okhttp3.OkHttpClient;
import io.kyligence.kap.shaded.influxdb.okhttp3.Protocol;
import io.kyligence.kap.shaded.influxdb.okhttp3.Request;
import io.kyligence.kap.shaded.influxdb.okhttp3.Response;
import io.kyligence.kap.shaded.influxdb.okhttp3.ResponseBody;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;
import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDBFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class QueryHistoryDAOTest extends NLocalFileMetadataTestCase {
    private final String SHOW_DATABASES = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"KE_METRIC\"]]}]}]}\n";
    private final String SHOW_DATABASES_NOT_EXIST = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"]]}]}]}\n";

    private static final String PROJECT = "default";
    private QueryHistoryDAO queryHistoryDAO;

    final String mockedHostname = "192.168.1.1";
    final String mockedSubmitter = "ADMIN";
    final String mockedAnsweredBy = "RDBMS";
    final String mockedProject = PROJECT;
    final String mockedSql1 = "select count(*) from table_1 where price > 10";
    final String mockedSql2 = "select count(*) from table_2 where price > 10";
    final String mockedSqlPattern1 = "select count(*) from table_1 where price > 1";
    final String mockedSqlPattern2 = "select count(*) from table_2 where price > 1";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        queryHistoryDAO = QueryHistoryDAO.getInstance(getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        QueryHistoryDAO.influxDB = mockInfluxDB();

        final String testSql = String.format("select * from %s", QueryHistory.QUERY_MEASUREMENT);
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesBySql(testSql, QueryHistory.class);
        Assert.assertEquals(2, queryHistories.size());
        QueryHistory queryHistory1 = queryHistories.get(0);
        QueryHistory queryHistory2 = queryHistories.get(1);
        Assert.assertEquals(mockedSql1, queryHistory1.getSql());
        Assert.assertEquals(mockedSql2, queryHistory2.getSql());
        Assert.assertEquals(mockedSqlPattern1, queryHistory1.getSqlPattern());
        Assert.assertEquals(mockedSqlPattern2, queryHistory2.getSqlPattern());
        Assert.assertEquals(mockedSubmitter, queryHistory1.getQuerySubmitter());
        Assert.assertEquals(mockedSubmitter, queryHistory2.getQuerySubmitter());
        Assert.assertEquals(mockedHostname, queryHistory1.getHostName());
        Assert.assertEquals(mockedHostname, queryHistory2.getHostName());
    }

    @Test
    public void testDatabaseNotExist() {
        QueryHistoryDAO.influxDB = mockInfluxDBWhenDatabaseNotExist();
        final String testSql = String.format("select * from %s", QueryHistory.QUERY_MEASUREMENT);
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesBySql(testSql, QueryHistory.class);
        Assert.assertEquals(0, queryHistories.size());
    }

    private InfluxDB mockInfluxDBWhenDatabaseNotExist() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                final Request request = chain.request();
                return mockShowDatabases(request, SHOW_DATABASES_NOT_EXIST);
            }
        });

        return InfluxDBFactory.connect("http://localhost:8096", "username", "password", client);
    }

    private InfluxDB mockInfluxDB() {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) {
                final Request request = chain.request();
                final URL url = request.url().url();

                if (url.toString().contains("SHOW+DATABASES")) {
                    return mockShowDatabases(request, SHOW_DATABASES);
                }

                return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                        .addHeader("Content-Type", "application/json").message("ok")
                        .body(ResponseBody.create(MediaType.parse("application/json"), getMockData())).build();
            }
        });

        return InfluxDBFactory.connect("http://localhost:8096", "username", "password", client);
    }

    private Response mockShowDatabases(final Request request, String result) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), result)).build();
    }

    private String getMockData() {

        StringBuilder sb = new StringBuilder();
        sb.append("{\"results\":[{\"series\":[{\"name\":\"query_metric\",");
        // columns
        sb.append(String.format("\"columns\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"],",
                QueryHistory.SQL_TEXT, QueryHistory.SQL_PATTERN, QueryHistory.QUERY_HOSTNAME, QueryHistory.SUBMITTER, QueryHistory.ANSWERED_BY, QueryHistory.PROJECT));
        // row 1
        sb.append(String.format("\"values\":[[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"],",
                mockedSql1, mockedSqlPattern1, mockedHostname, mockedSubmitter, mockedAnsweredBy, mockedProject));
        // row 2
        sb.append(String.format("[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]]}]}]}",
                mockedSql2, mockedSqlPattern2, mockedHostname, mockedSubmitter, mockedAnsweredBy, mockedProject));

        return sb.toString();
    }
}
