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

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.rest.request.QueryHistoryRequest;
import org.apache.kylin.rest.msg.MsgPicker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;

public class QueryHistoryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @BeforeClass
    public static void setUpBeforeClass() {
        staticCreateTestMetadata();
    }

    @Before
    public void setUp() {
        createTestMetadata();
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetFilteredQueryHistories() {
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

        // mock sqls
        String getQueryHistoriesSql = "select * from query_metric where project = 'default' ";
        String filterSql = "and (query_time >= 0 and query_time < 1) and (\"duration\" >= 0 and \"duration\" <= 10)";
        String getTotalSizeSql = "select count(query_id) from query_metric where project = 'default' ";
        Mockito.doReturn(filterSql).when(queryHistoryService).getQueryHistoryFilterSql(request);
        Mockito.doReturn(getQueryHistoriesSql).when(queryHistoryService).getQueryHistoriesSql(request.getProject(), filterSql, limit, offset);
        Mockito.doReturn(getTotalSizeSql).when(queryHistoryService).getQueryHistoriesTotalSizeSql(request.getProject(), filterSql);

        // mock query history
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select * from test_table_1");
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select * from test_table_2");

        // mock query history total size
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setSize(10);

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory1, queryHistory2)).when(queryHistoryDAO).getQueryHistoriesBySql(getQueryHistoriesSql, QueryHistory.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory)).when(queryHistoryDAO).getQueryHistoriesBySql(getTotalSizeSql, QueryHistory.class);
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryManager();

        HashMap<String, Object> result = queryHistoryService.getQueryHistories(request, limit, offset);
        List<QueryHistory> queryHistories = (List<QueryHistory>) result.get("query_histories");
        int size = (int) result.get("size");

        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals(queryHistory1.getSql(), queryHistories.get(0).getSql());
        Assert.assertEquals(queryHistory2.getSql(), queryHistories.get(1).getSql());
        Assert.assertEquals(10, size);

        Mockito.doReturn(Lists.newArrayList()).when(queryHistoryDAO).getQueryHistoriesBySql(getTotalSizeSql, QueryHistory.class);
        result = queryHistoryService.getQueryHistories(request, limit, offset);
        size = (int) result.get("size");

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

        String filterSql = queryHistoryService.getQueryHistoryFilterSql(request);
        String getQueryHistoriesSql = queryHistoryService.getQueryHistoriesSql(request.getProject(), filterSql, limit, offset);
        String getTotalSizeSql = queryHistoryService.getQueryHistoriesTotalSizeSql(request.getProject(), filterSql);

        String expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE project = '%s' AND (query_time >= %d AND query_time < %d) " +
                        "AND (\"duration\" >= %d AND \"duration\" <= %d) ORDER BY time DESC LIMIT %d OFFSET %d", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                0, Long.MAX_VALUE, 0, Integer.MAX_VALUE*1000L, limit, offset * limit);
        String expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE project = '%s' AND (query_time >= %d AND query_time < %d) " +
                        "AND (\"duration\" >= %d AND \"duration\" <= %d) ", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                0, Long.MAX_VALUE, 0, Integer.MAX_VALUE*1000L);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        request.setStartTimeFrom(0);
        request.setStartTimeTo(1);
        request.setLatencyFrom(0);
        request.setLatencyTo(10);

        // when there is a filter condition for sql
        request.setSql("select * from test_table");
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ ORDER BY time DESC LIMIT %d OFFSET %d", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql(), limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ ", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql());

        filterSql = queryHistoryService.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryService.getQueryHistoriesSql(request.getProject(), filterSql, limit, offset);
        getTotalSizeSql = queryHistoryService.getQueryHistoriesTotalSizeSql(request.getProject(), filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        // when there is a filter condition for accelerate status
        request.setAccelerateStatuses(Lists.newArrayList(QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED));
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (accelerate_status = '%s' OR accelerate_status = '%s') ORDER BY time DESC LIMIT %d OFFSET %d", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED, limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (accelerate_status = '%s' OR accelerate_status = '%s') ", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED);

        filterSql = queryHistoryService.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryService.getQueryHistoriesSql(request.getProject(), filterSql, limit, offset);
        getTotalSizeSql = queryHistoryService.getQueryHistoriesTotalSizeSql(request.getProject(), filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);

        // when there is a condition that filters answered by
        request.setRealizations(Lists.newArrayList("pushdown", "modelName"));
        expectedQueryHistoriesSql = String.format("SELECT * FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (cube_hit = 'false' OR cube_hit = 'true') AND (accelerate_status = '%s' OR accelerate_status = '%s') ORDER BY time DESC LIMIT %d OFFSET %d", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED, limit, offset*limit);
        expectedGetTotalSizeSql = String.format("SELECT count(query_id) FROM %s WHERE project = '%s' AND (query_time >= 0 AND query_time < 1) " +
                        "AND (\"duration\" >= 0 AND \"duration\" <= 10000) AND sql_text =~ /%s/ AND (cube_hit = 'false' OR cube_hit = 'true') AND (accelerate_status = '%s' OR accelerate_status = '%s') ", QueryHistory.QUERY_MEASUREMENT, PROJECT,
                request.getSql(), QueryHistory.QUERY_HISTORY_ACCELERATED, QueryHistory.QUERY_HISTORY_UNACCELERATED);

        filterSql = queryHistoryService.getQueryHistoryFilterSql(request);
        getQueryHistoriesSql = queryHistoryService.getQueryHistoriesSql(request.getProject(), filterSql, limit, offset);
        getTotalSizeSql = queryHistoryService.getQueryHistoriesTotalSizeSql(request.getProject(), filterSql);

        Assert.assertEquals(expectedQueryHistoriesSql, getQueryHistoriesSql);
        Assert.assertEquals(expectedGetTotalSizeSql, getTotalSizeSql);
    }

    @Test
    public void testGetQueryHistoriesByTime() {
        long startTime = 0;
        long endTime = 1000;

        String expectedSql = String.format("SELECT * FROM %s WHERE time >= 0ms AND time < 1000ms", QueryHistory.QUERY_MEASUREMENT);
        String actualSql = queryHistoryService.getQueryHistoriesByTimeSql(startTime, endTime);

        Assert.assertEquals(expectedSql, actualSql);

        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select * from test_table_1");
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select * from test_table_2");

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory1, queryHistory2)).when(queryHistoryDAO).getQueryHistoriesBySql(actualSql, QueryHistory.class);
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryManager();

        List<QueryHistory> queryHistories = queryHistoryService.getQueryHistories(startTime, endTime);
        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals(queryHistory1.getSql(), queryHistories.get(0).getSql());
        Assert.assertEquals(queryHistory2.getSql(), queryHistories.get(1).getSql());
    }

    @Test
    public void testCheckMetricTypeError() {
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "");

        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);

        try {
            queryHistoryService.getQueryHistories(request, 10, 0);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalStateException.class, ex.getClass());
            Assert.assertEquals(MsgPicker.getMsg().getNOT_SET_INFLUXDB(), ex.getMessage());
        }
    }
}
