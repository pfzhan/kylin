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
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.metadata.query.QueryStatistics;
import io.kyligence.kap.rest.response.QueryHistoryResponse;
import io.kyligence.kap.rest.response.QueryStatisticsResponse;
import org.apache.kylin.rest.exception.BadRequestException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetFilteredQueryHistories() throws InvocationTargetException, IllegalAccessException {
        // when there is no filter conditions
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        // set default values
        request.setStartTimeFrom(0);
        request.setStartTimeTo(Long.MAX_VALUE);
        request.setLatencyFrom(0);
        request.setLatencyTo(Integer.MAX_VALUE);

        // mock query history
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSql("select * from test_table_1");
        queryHistory1.setAnsweredBy("741ca86a-1f13-46da-a59f-95fb68615e3a,89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSql("select * from test_table_2");

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(queryHistory1, queryHistory2)).when(queryHistoryDAO).getQueryHistoriesByConditions(Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(10).when(queryHistoryDAO).getQueryHistoriesSize(Mockito.any());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao(PROJECT);

        HashMap<String, Object> result = queryHistoryService.getQueryHistories(request, 10, 0);
        List<QueryHistoryResponse> queryHistories = (List<QueryHistoryResponse>) result.get("query_histories");
        int size = (int) result.get("size");

        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals(queryHistory1.getSql(), queryHistories.get(0).getQueryHistory().getSql());
        Assert.assertEquals(queryHistory2.getSql(), queryHistories.get(1).getQueryHistory().getSql());
        Assert.assertEquals(10, size);
        Assert.assertEquals(2, queryHistories.get(0).getModelAliasMapping().size());
        Assert.assertEquals("741ca86a-1f13-46da-a59f-95fb68615e3a", queryHistories.get(0).getModelAliasMapping().get("nmodel_basic_inner"));
        Assert.assertEquals("89af4ee2-2cdb-4b07-b39e-4c29856309aa", queryHistories.get(0).getModelAliasMapping().get("nmodel_basic"));
    }

    @Test
    public void testGetQueryStatistics() {
        QueryStatistics queryStatistics = new QueryStatistics();
        queryStatistics.setCount(100);
        queryStatistics.setMeanDuration(500);

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(queryStatistics).when(queryHistoryDAO).getQueryCountAndAvgDuration(0, Long.MAX_VALUE);
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao(PROJECT);

        QueryStatisticsResponse result = queryHistoryService.getQueryStatistics(PROJECT, 0, Long.MAX_VALUE);
        Assert.assertEquals(100, result.getCount());
        Assert.assertEquals(500, result.getMean(), 0.1);
    }

    @Test
    public void testGetQueryCount() throws ParseException {
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountByModel(0, Long.MAX_VALUE);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getQueryCountByTime(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao(PROJECT);

        // query count by model
        Map<String, Object> result = queryHistoryService.getQueryCount(PROJECT, 0, Long.MAX_VALUE, "model");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(10, result.get("nmodel_basic"));
        Assert.assertEquals(11, result.get("all_fixed_length"));
        Assert.assertEquals(12, result.get("test_encoding"));

        // query count by day
        result = queryHistoryService.getQueryCount(PROJECT, 0, Long.MAX_VALUE, "day");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(10, result.get("2018-01-01"));
        Assert.assertEquals(11, result.get("2018-01-02"));
        Assert.assertEquals(12, result.get("2018-01-03"));

        // query count by week
        result = queryHistoryService.getQueryCount(PROJECT, 0, Long.MAX_VALUE, "week");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(10, result.get("2018-01-01"));
        Assert.assertEquals(11, result.get("2018-01-02"));
        Assert.assertEquals(12, result.get("2018-01-03"));

        // query count by month
        result = queryHistoryService.getQueryCount(PROJECT, 0, Long.MAX_VALUE, "month");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(10, result.get("2018-01-01"));
        Assert.assertEquals(11, result.get("2018-01-02"));
        Assert.assertEquals(12, result.get("2018-01-03"));
    }

    @Test
    public void testGetAvgDuration() throws ParseException {
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByModel(0, Long.MAX_VALUE);
        Mockito.doReturn(getTestStatistics()).when(queryHistoryDAO).getAvgDurationByTime(Mockito.anyLong(), Mockito.anyLong(), Mockito.anyString());
        Mockito.doReturn(queryHistoryDAO).when(queryHistoryService).getQueryHistoryDao(PROJECT);

        // avg duration by model
        Map<String, Object> result = queryHistoryService.getAvgDuration(PROJECT, 0, Long.MAX_VALUE, "model");
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(500, (double) result.get("nmodel_basic"), 0.1);
        Assert.assertEquals(600, (double) result.get("all_fixed_length"), 0.1);
        Assert.assertEquals(700, (double) result.get("test_encoding"), 0.1);

        // avg duration by day
        result = queryHistoryService.getAvgDuration(PROJECT, 0, Long.MAX_VALUE, "day");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(500, (double) result.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) result.get("2018-01-02"), 0.1);
        Assert.assertEquals(700, (double) result.get("2018-01-03"), 0.1);

        // avg duration by week
        result = queryHistoryService.getAvgDuration(PROJECT, 0, Long.MAX_VALUE, "week");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(500, (double) result.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) result.get("2018-01-02"), 0.1);
        Assert.assertEquals(700, (double) result.get("2018-01-03"), 0.1);

        // avg duration by month
        result = queryHistoryService.getAvgDuration(PROJECT, 0, Long.MAX_VALUE, "month");
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(500, (double) result.get("2018-01-01"), 0.1);
        Assert.assertEquals(600, (double) result.get("2018-01-02"), 0.1);
        Assert.assertEquals(700, (double) result.get("2018-01-03"), 0.1);
    }

    private List<QueryStatistics> getTestStatistics() throws ParseException {
        String date = "2018-01-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        long time = format.parse(date).getTime();

        QueryStatistics queryStatistics1 = new QueryStatistics();
        queryStatistics1.setCount(10);
        queryStatistics1.setMeanDuration(500);
        queryStatistics1.setModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        queryStatistics1.setTime(Instant.ofEpochMilli(time));
        queryStatistics1.setMonth(date);

        date = "2018-01-02";
        time = format.parse(date).getTime();

        QueryStatistics queryStatistics2 = new QueryStatistics();
        queryStatistics2.setCount(11);
        queryStatistics2.setMeanDuration(600);
        queryStatistics2.setModel("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        queryStatistics2.setTime(Instant.ofEpochMilli(time));
        queryStatistics2.setMonth(date);

        date = "2018-01-03";
        time = format.parse(date).getTime();

        QueryStatistics queryStatistics3 = new QueryStatistics();
        queryStatistics3.setCount(12);
        queryStatistics3.setMeanDuration(700);
        queryStatistics3.setModel("a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94");
        queryStatistics3.setTime(Instant.ofEpochMilli(time));
        queryStatistics3.setMonth(date);

        date = "2018-01-04";
        time = format.parse(date).getTime();
        QueryStatistics queryStatistics4 = new QueryStatistics();
        queryStatistics4.setCount(12);
        queryStatistics4.setMeanDuration(800);
        queryStatistics4.setModel("not_existing_model");
        queryStatistics4.setTime(Instant.ofEpochMilli(time));
        queryStatistics4.setMonth(date);

        return Lists.newArrayList(queryStatistics1, queryStatistics2, queryStatistics3, queryStatistics4);
    }

    @Test
    public void testGetQueryHistoryTableNames() {
        List<String> projects = Lists.newArrayList(PROJECT, "newten");
        Map<String, String> tableMap = queryHistoryService.getQueryHistoryTableMap(projects);
        Assert.assertEquals(2, tableMap.size());
        Assert.assertEquals("_examples_test_metadata_metadata_query_metric_5cabc32a_a33e_4b69_83dd_1bb8b1f8c92b", tableMap.get("newten"));
        Assert.assertEquals("_examples_test_metadata_metadata_query_metric_1eaca32a_a33e_4b69_83dd_0bb8b1f8c91b", tableMap.get(PROJECT));

        // get all tables
        tableMap = queryHistoryService.getQueryHistoryTableMap(null);
        Assert.assertEquals(8, tableMap.size());

        // not existing project
        try {
            tableMap = queryHistoryService.getQueryHistoryTableMap(Lists.newArrayList("not_existing_project"));
        } catch (Exception ex) {
            Assert.assertEquals(BadRequestException.class, ex.getClass());
            Assert.assertEquals("Cannot find project 'not_existing_project'.", ex.getMessage());
        }
    }
}
