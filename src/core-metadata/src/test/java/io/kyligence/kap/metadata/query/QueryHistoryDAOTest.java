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

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

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

public class QueryHistoryDAOTest extends NLocalFileMetadataTestCase {
    private final String SHOW_DATABASES = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"],[\"KE_METRIC\"]]}]}]}\n";
    private final String SHOW_DATABASES_NOT_EXIST = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"databases\",\"columns\":[\"name\"],\"values\":[[\"_internal\"]]}]}]}\n";

    private static final String PROJECT = "default";
    private String queryMeasurement;
    private String realizationMeasurement;
    private QueryHistoryDAO queryHistoryDAO;

    final String mockedHostname = "192.168.1.1";
    final String mockedSubmitter = "ADMIN";
    final String mockedAnsweredBy = "RDBMS";
    final String mockedSql1 = "select count(*) from table_1 where price > 10";
    final String mockedSql2 = "select count(*) from table_2 where price > 10";
    final String mockedSqlPattern1 = "select count(*) from table_1 where price > 1";
    final String mockedSqlPattern2 = "select count(*) from table_2 where price > 1";

    @Before
    public void setUp() {
        this.createTestMetadata();
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        queryHistoryDAO = QueryHistoryDAO.getInstance(getTestConfig(), PROJECT);
        queryMeasurement = queryHistoryDAO.getQueryMetricMeasurement();
        realizationMeasurement = queryHistoryDAO.getRealizationMetricMeasurement();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        QueryHistoryDAO.influxDB = mockInfluxDB(getMockData());

        final String testSql = String.format("select * from %s", queryMeasurement);
        List<QueryHistory> queryHistories = queryHistoryDAO.getResultBySql(testSql, QueryHistory.class, queryMeasurement);
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
    public void testGetQueryHistoriesByTime() {
        QueryHistoryDAO.influxDB = mockInfluxDB(getMockData());
        long startTime = 0;
        long endTime = 1000;

        String expectedSql = String.format("SELECT * FROM %s WHERE time >= 0ms AND time < 1000ms", queryMeasurement);
        String actualSql = queryHistoryDAO.getQueryHistoriesByTimeSql(startTime, endTime);

        Assert.assertEquals(expectedSql, actualSql);

        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesByTime(startTime, endTime);
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
        QueryHistoryDAO.influxDB = mockInfluxDB(SHOW_DATABASES_NOT_EXIST);
        final String testSql = String.format("select * from %s", queryMeasurement);
        List<QueryHistory> queryHistories = queryHistoryDAO.getResultBySql(testSql, QueryHistory.class, queryMeasurement);
        Assert.assertEquals(0, queryHistories.size());
    }

    @Test
    public void testGetFilteredQueryHistories() {
        QueryHistoryDAO.influxDB = mockInfluxDB(getMockData());

        int limit = 2;
        int offset = 0;

        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom(0);
        request.setStartTimeTo(Long.MAX_VALUE);
        request.setLatencyFrom(0);
        request.setLatencyTo(Integer.MAX_VALUE);

        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesByConditions(request, limit, offset);
        Assert.assertEquals(2, queryHistories.size());

        QueryHistoryDAO.influxDB = mockInfluxDB(getMockedZeroSize());
        int size = queryHistoryDAO.getQueryHistoriesSize(request);
        Assert.assertEquals(0, size);
    }

    @Test
    public void testSqls() {
        int limit = 10;
        int offset = 1;

        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom(0);
        request.setStartTimeTo(Long.MAX_VALUE);
        request.setLatencyFrom(0);
        request.setLatencyTo(Integer.MAX_VALUE);

        String filterSql = queryHistoryDAO.getQueryHistoryFilterSql(request);
        String getQueryHistoriesSql = queryHistoryDAO.getQueryHistoriesSql(filterSql, limit, offset);
        String getTotalSizeSql = queryHistoryDAO.getQueryHistoriesSizeSql(filterSql);

        String expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE (query_time >= %d AND query_time < %d) " +
                        "AND (\"duration\" >= %d AND \"duration\" <= %d) ORDER BY time DESC LIMIT %d OFFSET %d", queryMeasurement,
                0, Long.MAX_VALUE, 0, Integer.MAX_VALUE*1000L, limit, offset * limit);
        String expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE (query_time >= %d AND query_time < %d) " +
                "AND (\"duration\" >= %d AND \"duration\" <= %d) ", queryMeasurement, 0, Long.MAX_VALUE, 0, Integer.MAX_VALUE*1000L);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        request.setStartTimeFrom(0);
        request.setStartTimeTo(1);
        request.setLatencyFrom(0);
        request.setLatencyTo(10);

        // when there is a filter condition for sql
        request.setSql("select * from test_table");
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ ORDER BY time DESC LIMIT %d OFFSET %d",
                queryMeasurement, request.getSql(), limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ ", queryMeasurement, request.getSql());

        filterSql = queryHistoryDAO.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryDAO.getQueryHistoriesSql(filterSql, limit, offset);
        getTotalSizeSql = queryHistoryDAO.getQueryHistoriesSizeSql(filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        // when there is a filter condition for accelerate status
        request.setAccelerateStatuses(Lists.newArrayList(QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED));
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (accelerate_status = '%s' OR accelerate_status = '%s') ORDER BY time DESC LIMIT %d OFFSET %d", queryMeasurement,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED, limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (accelerate_status = '%s' OR accelerate_status = '%s') ", queryMeasurement,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED);

        filterSql = queryHistoryDAO.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryDAO.getQueryHistoriesSql(filterSql, limit, offset);
        getTotalSizeSql = queryHistoryDAO.getQueryHistoriesSizeSql(filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        // when there is a condition that filters answered by
        request.setRealizations(Lists.newArrayList("pushdown", "modelId"));
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (cube_hit = 'false' OR cube_hit = 'true') AND (accelerate_status = '%s' OR accelerate_status = '%s') ORDER BY time DESC LIMIT %d OFFSET %d", queryMeasurement,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED, limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (cube_hit = 'false' OR cube_hit = 'true') AND (accelerate_status = '%s' OR accelerate_status = '%s') ", queryMeasurement,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED);

        filterSql = queryHistoryDAO.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryDAO.getQueryHistoriesSql(filterSql, limit, offset);
        getTotalSizeSql = queryHistoryDAO.getQueryHistoriesSizeSql(filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);
    }

    @Test
    public void testGetQueryStatisticsByEngine() {
        QueryHistoryDAO.influxDB = mockInfluxDB(getQueryStatisticsByEngineResult());
        List<QueryStatistics> queryStatistics = queryHistoryDAO.getQueryEngineStatistics(0L, Long.MAX_VALUE);
        Assert.assertEquals(1, queryStatistics.size());
        Assert.assertEquals("RDBMS", queryStatistics.get(0).getEngineType());
        Assert.assertEquals(7, queryStatistics.get(0).getCount());
        Assert.assertEquals(1108.71, queryStatistics.get(0).getMeanDuration(), 0.01);
    }

    @Test
    public void testGetQueryStatistics() {
        QueryHistoryDAO.influxDB = mockInfluxDB(getQueryStatisticsResult());
        QueryStatistics queryStatistics = queryHistoryDAO.getQueryCountAndAvgDuration(0L, Long.MAX_VALUE);

        Assert.assertNotNull(queryStatistics);
        Assert.assertEquals(1000, queryStatistics.getCount());
        Assert.assertEquals(3000, queryStatistics.getMeanDuration(), 0.1);

        // case of no data
        QueryHistoryDAO.influxDB = mockInfluxDB(getEmptyQueryStatistics());
        queryStatistics = queryHistoryDAO.getQueryCountAndAvgDuration(0L, Long.MAX_VALUE);
        Assert.assertNotNull(queryStatistics);
        Assert.assertEquals(0, queryStatistics.getCount());
        Assert.assertEquals(0, queryStatistics.getMeanDuration(), 0.1);
    }

    @Test
    public void testGetQueryCount() {
        // query count by model
        QueryHistoryDAO.influxDB = mockInfluxDB(getQueryCountByModelResult());
        List<QueryStatistics> queryStatistics = queryHistoryDAO.getQueryCountByModel(0, Long.MAX_VALUE);
        Assert.assertEquals(1, queryStatistics.size());
        Assert.assertEquals("model1", queryStatistics.get(0).getModel());
        Assert.assertEquals(1000, queryStatistics.get(0).getCount());

        // query count by day
        QueryHistoryDAO.influxDB = mockInfluxDB(getQueryCountByTimeResult());
        queryStatistics = queryHistoryDAO.getQueryCountByTime(0, Long.MAX_VALUE, "day");
        Assert.assertEquals(2, queryStatistics.size());

        Assert.assertEquals(2000, queryStatistics.get(0).getCount());
        Date date = new Date(queryStatistics.get(0).getTime().toEpochMilli());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Assert.assertEquals("2018-11-30", sdf.format(date));

        Assert.assertEquals(3000, queryStatistics.get(1).getCount());
        date = new Date(queryStatistics.get(1).getTime().toEpochMilli());
        Assert.assertEquals("2018-12-01", sdf.format(date));

        // query count by week
        String sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_count group by ", 1000, 2000, "week");
        Assert.assertEquals("sql_prefix_for_count group by time(1w, 4d)", sql);

        // query count by month
        sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_count group by ", 1000, 2000, "month");
        Assert.assertEquals("sql_prefix_for_count group by month", sql);

        // query count by default
        sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_count group by ", 1000, 2000, "");
        Assert.assertEquals("sql_prefix_for_count group by time(1d)", sql);
    }

    @Test
    public void testGetAvgDuration() {
        // avg duration by model
        QueryHistoryDAO.influxDB = mockInfluxDB(getAvgDurationByModelResult());
        List<QueryStatistics> queryStatistics = queryHistoryDAO.getQueryCountByModel(0, Long.MAX_VALUE);
        Assert.assertEquals(1, queryStatistics.size());
        Assert.assertEquals("model1", queryStatistics.get(0).getModel());
        Assert.assertEquals(500, queryStatistics.get(0).getMeanDuration(), 0.1);

        // avg duration by time
        QueryHistoryDAO.influxDB = mockInfluxDB(getAvgDurationByTimeResult());
        queryStatistics = queryHistoryDAO.getQueryCountByTime(0, Long.MAX_VALUE, "day");
        Assert.assertEquals(2, queryStatistics.size());

        Assert.assertEquals(500, queryStatistics.get(0).getMeanDuration(), 0.1);
        Date date = new Date(queryStatistics.get(0).getTime().toEpochMilli());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Assert.assertEquals("2018-11-30", sdf.format(date));

        Assert.assertEquals(600, queryStatistics.get(1).getMeanDuration(), 0.1);
        date = new Date(queryStatistics.get(1).getTime().toEpochMilli());
        Assert.assertEquals("2018-12-01", sdf.format(date));

        // query count by week
        String sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_duration group by ", 1000, 2000, "week");
        Assert.assertEquals("sql_prefix_for_duration group by time(1w, 4d)", sql);

        // query count by month
        sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_duration group by ", 1000, 2000, "month");
        Assert.assertEquals("sql_prefix_for_duration group by month", sql);

        // query count by default
        sql = queryHistoryDAO.getQueryStatsByTimeSql("sql_prefix_for_duration group by ", 1000, 2000, "");
        Assert.assertEquals("sql_prefix_for_duration group by time(1d)", sql);
    }

    private InfluxDB mockInfluxDB(final String mockedResult) {
        final OkHttpClient.Builder client = new OkHttpClient.Builder();
        client.addInterceptor(new Interceptor() {
            @Override
            public Response intercept(Chain chain) {
                final Request request = chain.request();
                final URL url = request.url().url();


                if (url.toString().contains("SHOW+DATABASES")) {
                    return mockResponse(request, SHOW_DATABASES);
                }

                return mockResponse(request, mockedResult);
            }
        });

        return InfluxDBFactory.connect("http://localhost:8096", "username", "password", client);
    }

    private Response mockResponse(final Request request, String result) {
        return new Response.Builder().request(request).protocol(Protocol.HTTP_2).code(200)
                .addHeader("Content-Type", "application/json").message("ok").addHeader("X-Influxdb-Version", "mock")
                .body(ResponseBody.create(MediaType.parse("application/json"), result)).build();
    }

    private String getMockData() {

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",", queryMeasurement));
        // columns
        sb.append(String.format("\"columns\":[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"],",
                QueryHistory.SQL_TEXT, QueryHistory.SQL_PATTERN, QueryHistory.QUERY_HOSTNAME, QueryHistory.SUBMITTER, QueryHistory.ANSWERED_BY));
        // row 1
        sb.append(String.format("\"values\":[[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"],",
                mockedSql1, mockedSqlPattern1, mockedHostname, mockedSubmitter, mockedAnsweredBy));
        // row 2
        sb.append(String.format("[\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"]]}]}]}",
                mockedSql2, mockedSqlPattern2, mockedHostname, mockedSubmitter, mockedAnsweredBy));

        return sb.toString();
    }

    private String getQueryStatisticsResult() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",", queryMeasurement));
        // column
        sb.append("\"columns\":[\"count\",\"mean\"],");
        // row
        sb.append("\"values\":[[1000,3000]]}]}]}");
        return sb.toString();
    }

    private String getEmptyQueryStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",", queryMeasurement));
        // column
        sb.append("\"columns\":[\"count\",\"mean\"],");
        // row
        sb.append("\"values\":[]}]}]}");
        return sb.toString();
    }

    private String getQueryCountByModelResult() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",\"tags\":{\"model\":\"model1\"}, ", realizationMeasurement));
        // column
        sb.append("\"columns\":[\"count\"],");
        // row
        sb.append("\"values\":[[1000]]}]}]}");
        return sb.toString();
    }

    private String getQueryCountByTimeResult() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\", ", queryMeasurement));
        // column
        sb.append("\"columns\":[\"count\", \"time\"],");
        // row
        sb.append("\"values\":[[2000, \"2018-11-30T00:00:00Z\"], [3000, \"2018-12-01T00:00:00Z\"]]}]}]}");
        return sb.toString();
    }

    private String getAvgDurationByModelResult() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",\"tags\":{\"model\":\"model1\"}, ", realizationMeasurement));
        // column
        sb.append("\"columns\":[\"mean\"],");
        // row
        sb.append("\"values\":[[500]]}]}]}");
        return sb.toString();
    }

    private String getAvgDurationByTimeResult() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{\"results\":[{\"series\":[{\"name\":\"%s\", ", queryMeasurement));
        // column
        sb.append("\"columns\":[\"mean\", \"time\"],");
        // row
        sb.append("\"values\":[[500, \"2018-11-30T00:00:00Z\"], [600, \"2018-12-01T00:00:00Z\"]]}]}]}");
        return sb.toString();
    }

    private String getMockedZeroSize() {

        StringBuilder sb = new StringBuilder();
        sb.append("{\"results\":[{\"series\":[]}]}");
        return sb.toString();
    }

    private String getQueryStatisticsByEngineResult() {
        return String.format("{\"results\":[{\"series\":[{\"name\":\"%s\",\"tags\":{\"engine_type\":\"RDBMS\"}," +
                "\"columns\":[\"time\",\"count\",\"mean\"]," +
                "\"values\":[[\"1970-01-01T00:00:00Z\",7.0,1108.7142857142858]]}]," +
                "\"error\":null}],\"error\":null}\n", queryMeasurement);
    }
}
