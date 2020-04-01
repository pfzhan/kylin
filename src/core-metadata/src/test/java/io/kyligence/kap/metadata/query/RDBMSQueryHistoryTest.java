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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;
import static io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.base.Joiner;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import lombok.val;
import lombok.var;

@RunWith(TimeZoneTestRunner.class)
public class RDBMSQueryHistoryTest extends NLocalFileMetadataTestCase {

    String PROJECT = "default";
    public static final String WEEK = "week";
    public static final String DAY = "day";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        writeToQueryHistory();
    }

    @After
    public void destroy() throws Exception {
        val jdbcTemplate = getJdbcTemplate();
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        cleanupTestMetadata();
    }

    private void writeToQueryHistory() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        Properties properties = getProperties();
        var createQueryHistorSql = properties.getProperty("create.queryhistory.store.table");
        getJdbcTemplate().execute(String.format(createQueryHistorSql, url.getIdentifier() + "_query_history"));
        String sql = "INSERT INTO " + url.getIdentifier() + "_query_history" + " ("
                + Joiner.on(",").join(QueryHistory.QUERY_ID, QueryHistory.SQL_TEXT, QueryHistory.SQL_PATTERN,
                        QueryHistory.QUERY_DURATION, QueryHistory.TOTAL_SCAN_BYTES, QueryHistory.TOTAL_SCAN_COUNT,
                        QueryHistory.RESULT_ROW_COUNT, QueryHistory.SUBMITTER, QueryHistory.REALIZATIONS,
                        QueryHistory.QUERY_SERVER, QueryHistory.ERROR_TYPE, QueryHistory.ENGINE_TYPE,
                        QueryHistory.IS_CACHE_HIT, QueryHistory.QUERY_STATUS, QueryHistory.IS_INDEX_HIT,
                        QueryHistory.QUERY_TIME, QueryHistory.MONTH, QueryHistory.QUERY_FIRST_DAY_OF_MONTH,
                        QueryHistory.QUERY_FIRST_DAY_OF_WEEK, QueryHistory.QUERY_DAY, QueryHistory.IS_TABLE_INDEX_USED,
                        QueryHistory.IS_AGG_INDEX_USED, QueryHistory.IS_TABLE_SNAPSHOT_USED, QueryHistory.PROJECT_NAME)
                + ")  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        // 2020-01-29 23:25:12
        Long queryTime1 = 1580311512000L;
        // 2020-01-30 23:25:12
        Long queryTime2 = 1580397912000L;
        // 2020-01-31 23:25:12
        Long queryTime3 = 1580484312000L;
        // 2021-01-29 23:25:12
        Long queryTime4 = 1611933912000L;
        getJdbcTemplate().update(sql, "121bbebf-3d82-4b18-8bae-a3b668930141", "select 1", "select 1", 1, 5045, 4096,
                500, "ADMIN", "", "", "", "", false, "", true, queryTime1, "2020-03",
                TimeUtil.getMonthStart(queryTime1), TimeUtil.getWeekStart(queryTime1), TimeUtil.getDayStart(queryTime1),
                true, false, false, PROJECT);
        getJdbcTemplate().update(sql, "121bbebf-3d82-4b18-8bae-a3b668930142", "select 2", "select 2", 2, 5045, 4096,
                500, "ADMIN", "", "", "", "", false, "", true, queryTime2, "2020-03",
                TimeUtil.getMonthStart(queryTime2), TimeUtil.getWeekStart(queryTime2), TimeUtil.getDayStart(queryTime2),
                true, false, false, PROJECT);
        getJdbcTemplate().update(sql, "121bbebf-3d82-4b18-8bae-a3b668930143", "select 3", "select 3", 3, 5045, 4096,
                500, "ADMIN", "", "", "", "", false, "", true, queryTime3, "2020-03",
                TimeUtil.getMonthStart(queryTime3), TimeUtil.getWeekStart(queryTime3), TimeUtil.getDayStart(queryTime3),
                true, false, false, PROJECT);
        getJdbcTemplate().update(sql, "121bbebf-3d82-4b18-8bae-a3b668930144", "select 4", "select 4", 4, 5045, 4096,
                500, "ADMIN", "", "", "", "", false, "", true, queryTime4, "2020-03",
                TimeUtil.getMonthStart(queryTime4), TimeUtil.getWeekStart(queryTime4), TimeUtil.getDayStart(queryTime4),
                true, false, false, "other_project");
    }

    @Test
    public void testGetQueryHistoriesByTime() throws Exception {
        val url = getTestConfig().getMetadataUrl();

        // filter from 2020-01-29 23:25:11 to 2020-01-31 23:25:13
        List<QueryHistory> queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.QUERY_HISTORY_BY_TIME_SQL_FORMAT,
                        url.getIdentifier() + "_query_history"), 1580311511000L, 1580484313000L, PROJECT));
        Assert.assertEquals(3, queryHistoryList.size());

        // filter from 2020-01-29 23:25:11 to 2020-01-30 23:00:13
        queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.QUERY_HISTORY_BY_TIME_SQL_FORMAT,
                        url.getIdentifier() + "_query_history"), 1580311511000L, 1580396413000L, PROJECT));
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1580311512000L, queryHistoryList.get(0).getQueryTime());
    }

    @Test
    public void testGetQueryCountByTime() throws Exception {
        val url = getTestConfig().getMetadataUrl();

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = JDBCResultMapper
                .queryStatisticsResultMapper(getJdbcTemplate().queryForList(
                        String.format(RDBMSQueryHistoryDAO.QUERY_COUNT_BY_TIME_SQL_FORMAT, QueryHistory.QUERY_DAY,
                                url.getIdentifier() + "_query_history", QueryHistory.QUERY_DAY),
                        1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(3, dayQueryStatistics.size());
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(0).getCount());
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getCount());
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(2).getCount());
        fillZeroForQueryStatistics(dayQueryStatistics, 1580052311000L, 1580484313000L, DAY);
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(0).getCount());
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getCount());
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(2).getCount());
        Assert.assertEquals("2020-01-26T00:00:00Z", dayQueryStatistics.get(3).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(3).getCount());
        Assert.assertEquals("2020-01-27T00:00:00Z", dayQueryStatistics.get(4).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(4).getCount());
        Assert.assertEquals("2020-01-28T00:00:00Z", dayQueryStatistics.get(5).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(5).getCount());

        List<QueryStatistics> weekQueryStatistics = JDBCResultMapper.queryStatisticsResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.QUERY_COUNT_BY_TIME_SQL_FORMAT,
                        QueryHistory.QUERY_FIRST_DAY_OF_WEEK, url.getIdentifier() + "_query_history",
                        QueryHistory.QUERY_FIRST_DAY_OF_WEEK), 1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());

        List<QueryStatistics> monthQueryStatistics = JDBCResultMapper.queryStatisticsResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.QUERY_COUNT_BY_TIME_SQL_FORMAT,
                        QueryHistory.QUERY_FIRST_DAY_OF_MONTH, url.getIdentifier() + "_query_history",
                        QueryHistory.QUERY_FIRST_DAY_OF_MONTH), 1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
    }

    @Test
    public void testGetAvgDurationByTime() throws Exception {
        val url = getTestConfig().getMetadataUrl();

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = JDBCResultMapper
                .queryStatisticsResultMapper(getJdbcTemplate().queryForList(
                        String.format(RDBMSQueryHistoryDAO.AVG_DURATION_BY_TIME_SQL_FORMAT, QueryHistory.QUERY_DAY,
                                url.getIdentifier() + "_query_history", QueryHistory.QUERY_DAY),
                        1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(3, dayQueryStatistics.size());
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, dayQueryStatistics.get(0).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(2, dayQueryStatistics.get(2).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(dayQueryStatistics, 1580052311000L, 1580484313000L, DAY);
        Assert.assertEquals("2020-01-31T00:00:00Z", dayQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, dayQueryStatistics.get(0).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-29T00:00:00Z", dayQueryStatistics.get(1).getTime().toString());
        Assert.assertEquals(1, dayQueryStatistics.get(1).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-30T00:00:00Z", dayQueryStatistics.get(2).getTime().toString());
        Assert.assertEquals(2, dayQueryStatistics.get(2).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-26T00:00:00Z", dayQueryStatistics.get(3).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(3).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-27T00:00:00Z", dayQueryStatistics.get(4).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(4).getMeanDuration(), 0.1);
        Assert.assertEquals("2020-01-28T00:00:00Z", dayQueryStatistics.get(5).getTime().toString());
        Assert.assertEquals(0, dayQueryStatistics.get(5).getMeanDuration(), 0.1);

        List<QueryStatistics> weekQueryStatistics = JDBCResultMapper.queryStatisticsResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.AVG_DURATION_BY_TIME_SQL_FORMAT,
                        QueryHistory.QUERY_FIRST_DAY_OF_WEEK, url.getIdentifier() + "_query_history",
                        QueryHistory.QUERY_FIRST_DAY_OF_WEEK), 1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);

        List<QueryStatistics> monthQueryStatistics = JDBCResultMapper.queryStatisticsResultMapper(
                getJdbcTemplate().queryForList(String.format(RDBMSQueryHistoryDAO.AVG_DURATION_BY_TIME_SQL_FORMAT,
                        QueryHistory.QUERY_FIRST_DAY_OF_MONTH, url.getIdentifier() + "_query_history",
                        QueryHistory.QUERY_FIRST_DAY_OF_MONTH), 1580052311000L, 1580484313000L, PROJECT));
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
    }

    @Test
    public void testDropProjectMeasurement() throws Exception {
        val url = getTestConfig().getMetadataUrl();

        // before delete
        List<QueryHistory> queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(getJdbcTemplate()
                .queryForList(String.format("select * from %s", url.getIdentifier() + "_query_history")));
        Assert.assertEquals(4, queryHistoryList.size());

        String deleteQueryHistoryForProjectSql = "delete from %s where project_name = ?";
        getJdbcTemplate().update(String.format(deleteQueryHistoryForProjectSql, url.getIdentifier() + "_query_history"),
                PROJECT);

        // after delete
        queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(getJdbcTemplate()
                .queryForList(String.format("select * from %s", url.getIdentifier() + "_query_history")));
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals("other_project", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testDeleteQueryHistoriesIfMaxSizeReached() throws Exception {
        val url = getTestConfig().getMetadataUrl();

        // before delete
        List<QueryHistory> queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(getJdbcTemplate()
                .queryForList(String.format("select * from %s", url.getIdentifier() + "_query_history")));
        Assert.assertEquals(4, queryHistoryList.size());

        // delete before 2020-01-31 08:25:13
        String deleteQueryHistoryForProjectSql = "delete from %s where query_time < ?";
        getJdbcTemplate().update(String.format(deleteQueryHistoryForProjectSql, url.getIdentifier() + "_query_history"),
                1585614313000L);

        // after delete
        queryHistoryList = JDBCResultMapper.queryHistoryResultMapper(getJdbcTemplate()
                .queryForList(String.format("select * from %s", url.getIdentifier() + "_query_history")));
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1611933912000L, queryHistoryList.get(0).getQueryTime());
    }

    @Test
    public void testQueryTimeInMaxSize() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        List<QueryStatistics> statistics = JDBCResultMapper.queryStatisticsResultMapper(getJdbcTemplate().queryForList(
                String.format(RDBMSQueryHistoryDAO.QUERY_TIME_IN_MAX_SIZE, url.getIdentifier() + "_query_history")));
        Assert.assertEquals(0, statistics.size());

        statistics = JDBCResultMapper.queryStatisticsResultMapper(getJdbcTemplate()
                .queryForList(String.format("SELECT query_time as time, id FROM %s ORDER BY id DESC limit 1 OFFSET 1",
                        url.getIdentifier() + "_query_history")));
        Assert.assertEquals(1, statistics.size());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    private Properties getProperties() throws Exception {
        String fileName = "metadata-jdbc-mysql.properties";
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }
}
