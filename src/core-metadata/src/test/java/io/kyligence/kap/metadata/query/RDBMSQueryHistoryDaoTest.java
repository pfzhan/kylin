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

import static io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;

@RunWith(TimeZoneTestRunner.class)
public class RDBMSQueryHistoryDaoTest extends NLocalFileMetadataTestCase {

    String PROJECT = "default";
    public static final String WEEK = "week";
    public static final String DAY = "day";
    public static final String NORMAL_USER = "normal_user";
    public static final String ADMIN = "ADMIN";

    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void destroy() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        cleanupTestMetadata();
    }

    @Test
    public void testInsert() {
        List<QueryMetrics> queryMetricsList = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            queryMetricsList.add(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        }
        queryHistoryDAO.insert(queryMetricsList);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 200, "default");
        Assert.assertEquals(100, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesfilterByIsIndexHit() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, false, "otherProject"));

        // filter all
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        // filter hit index
        queryHistoryRequest.setRealizations(Lists.newArrayList("modelName"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());

        // filter not hit index
        queryHistoryRequest.setRealizations(Lists.newArrayList("pushdown"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(2, queryHistoryList.size());

        // filter all
        queryHistoryRequest.setRealizations(Lists.newArrayList("modelName", "pushdown"));
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesfilterByQueryTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 1L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 1L, false, PROJECT));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT));

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setStartTimeFrom("1580397912000");
        queryHistoryRequest.setStartTimeTo("1580484312000");
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesfilterByDuration() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1000L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 2000L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 3000L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 4000L, false, PROJECT));

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setLatencyFrom("1");
        queryHistoryRequest.setLatencyTo("4");
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        queryHistoryRequest.setLatencyFrom("2");
        queryHistoryRequest.setLatencyTo("3");
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testGetQueryHistoriesfilterBySql() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics1.setSql("select 2 LIMIT 500\n");
        queryHistoryDAO.insert(queryMetrics1);

        QueryMetrics queryMetrics2 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics2.setSql("select 1 LIMIT 500\n");
        queryHistoryDAO.insert(queryMetrics2);

        QueryMetrics queryMetrics3 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics3.setSql("select count(*) from KYLIN_SALES group by BUYER_ID LIMIT 500");
        queryHistoryDAO.insert(queryMetrics3);

        QueryMetrics queryMetrics4 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics4.setSql("select count(*) from KYLIN_SALES");
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setProject(PROJECT);
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);

        queryHistoryRequest.setSql("count");
        List<QueryHistory> queryHistoryList1 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(2, queryHistoryList1.size());

        queryHistoryRequest.setSql("LIMIT");
        List<QueryHistory> queryHistoryList2 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(3, queryHistoryList2.size());

        queryHistoryRequest.setSql("select 1");
        List<QueryHistory> queryHistoryList3 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(1, queryHistoryList3.size());

        queryHistoryRequest.setSql("6a9a151f");
        List<QueryHistory> queryHistoryList4 = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10,
                0);
        Assert.assertEquals(4, queryHistoryList4.size());

        for (int i = 0; i < 30; i++) {
            queryHistoryDAO.insert(queryMetrics1);
        }
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0).size());
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 1).size());
        Assert.assertEquals(10, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 2).size());
        Assert.assertEquals(4, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 3).size());
        Assert.assertEquals(20, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 20, 0).size());
        Assert.assertEquals(14, queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 20, 1).size());
    }

    @Test
    public void getQueryHistoriesById() {
        Assert.assertEquals(1, queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT)));
        Assert.assertEquals(1, queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT)));
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 10, "default");
        Assert.assertEquals(2, queryHistoryList.size());
        Assert.assertEquals("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", queryHistoryList.get(0).getQueryId());
        Assert.assertNotNull(queryHistoryList.get(0).getQueryHistoryInfo());
    }

    @Test
    public void testGetQueryHistoriesSize() throws Exception {
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);

        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        long queryHistoriesSize = queryHistoryDAO.getQueryHistoriesSize(queryHistoryRequest, PROJECT);
        Assert.assertEquals(2, queryHistoriesSize);

        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoriesSize = queryHistoryDAO.getQueryHistoriesSize(queryHistoryRequest, PROJECT);
        Assert.assertEquals(5, queryHistoriesSize);
    }

    @Test
    public void testGetQueryCountByTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 1L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 1L, false, PROJECT));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT));

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "day", PROJECT);
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

        List<QueryStatistics> weekQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "week", PROJECT);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(3, weekQueryStatistics.get(0).getCount());

        List<QueryStatistics> monthQueryStatistics = queryHistoryDAO.getQueryCountByTime(1580052311000L, 1580484313000L,
                "month", PROJECT);
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(3, monthQueryStatistics.get(0).getCount());
    }

    @Test
    public void testGetAvgDurationByTime() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        // 2021-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1611933912000L, 1L, false, PROJECT));

        // filter from 2020-01-26 23:25:11 to 2020-01-31 23:25:13
        List<QueryStatistics> dayQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L, 1580484313000L,
                "day", PROJECT);
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

        List<QueryStatistics> weekQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L, 1580484313000L,
                "week", PROJECT);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(weekQueryStatistics, 1580052311000L, 1580484313000L, WEEK);
        Assert.assertEquals(1, weekQueryStatistics.size());
        Assert.assertEquals("2020-01-26T00:00:00Z", weekQueryStatistics.get(0).getTime().toString());
        Assert.assertEquals(2, weekQueryStatistics.get(0).getMeanDuration(), 0.1);

        List<QueryStatistics> monthQueryStatistics = queryHistoryDAO.getAvgDurationByTime(1580052311000L,
                1580484313000L, "month", PROJECT);
        Assert.assertEquals(1, monthQueryStatistics.size());
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
        fillZeroForQueryStatistics(monthQueryStatistics, 1580052311000L, 1580484313000L, "month");
        Assert.assertEquals(2, monthQueryStatistics.get(0).getMeanDuration(), 0.1);
    }

    @Test
    public void testDeleteQueryHistoriesIfRetainTimeReached() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoriesIfRetainTimeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals(1895930712000L, queryHistoryList.get(0).getQueryTime());
    }

    @Test
    public void testDeleteQueryHistoriesIfMaxSizeReached() throws Exception {
        overwriteSystemProp("kylin.query.queryhistory.max-size", "2");

        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoriesIfMaxSizeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(2, queryHistoryList.size());

        // test delete empty
        queryHistoryDAO.deleteQueryHistoriesIfMaxSizeReached();
        queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(0, 100, PROJECT);
        Assert.assertEquals(2, queryHistoryList.size());
    }

    @Test
    public void testDeleteQueryHistoriesIfProjectMaxSizeReached() throws Exception {
        overwriteSystemProp("kylin.query.queryhistory.project-max-size", "2");

        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, PROJECT));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoriesIfProjectMaxSizeReached(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, queryHistoryList.size());

        // test delete empty
        queryHistoryDAO.deleteQueryHistoriesIfProjectMaxSizeReached(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(2, queryHistoryList.size());
    }

    @Test
    public void testDropProjectMeasurement() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, "other"));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.dropProjectMeasurement(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals("other", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testDeleteQueryHistoryForProject() throws Exception {
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1L, true, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3L, false, PROJECT));
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 1L, false, "other"));

        // before delete
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(4, queryHistoryList.size());

        // after delete
        queryHistoryDAO.deleteQueryHistoryForProject(PROJECT);
        queryHistoryList = queryHistoryDAO.getAllQueryHistories();
        Assert.assertEquals(1, queryHistoryList.size());
        Assert.assertEquals("other", queryHistoryList.get(0).getProjectName());
    }

    @Test
    public void testUpdateQueryHistoryInfo() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT);
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other");
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        List<Pair<Long, QueryHistoryInfo>> qhInfoList = Lists.newArrayList();
        QueryHistoryInfo queryHistoryInfo1 = new QueryHistoryInfo(true, 3, true);
        queryHistoryInfo1.setState(QueryHistoryInfo.HistoryState.SUCCESS);
        qhInfoList.add(new Pair<>(queryMetrics1.id, queryHistoryInfo1));
        QueryHistoryInfo queryHistoryInfo2 = new QueryHistoryInfo(true, 3, true);
        queryHistoryInfo2.setState(QueryHistoryInfo.HistoryState.FAILED);
        qhInfoList.add(new Pair<>(queryMetrics2.id, queryHistoryInfo2));

        queryHistoryDAO.batchUpdataQueryHistorieInfo(qhInfoList);

        // after update
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getAllQueryHistories();

        Assert.assertEquals(queryMetrics1.id, queryHistoryList.get(2).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.SUCCESS,
                queryHistoryList.get(2).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics2.id, queryHistoryList.get(3).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.FAILED,
                queryHistoryList.get(3).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics3.id, queryHistoryList.get(0).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.PENDING,
                queryHistoryList.get(0).getQueryHistoryInfo().getState());

        Assert.assertEquals(queryMetrics4.id, queryHistoryList.get(1).getId());
        Assert.assertEquals(QueryHistoryInfo.HistoryState.PENDING,
                queryHistoryList.get(1).getQueryHistoryInfo().getState());
    }

    @Test
    public void testGetRetainTime() throws Exception {
        long retainTime = RDBMSQueryHistoryDAO.getRetainTime();
        long currentTime = System.currentTimeMillis();
        Assert.assertEquals(30, (currentTime - retainTime) / (24 * 60 * 60 * 1000L));
    }

    @Test
    public void testNonAdminUserGetQueryHistories() throws Exception {
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics1.setSubmitter(ADMIN);
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT);
        queryMetrics2.setSubmitter(ADMIN);
        QueryMetrics queryMetrics3 = createQueryMetrics(1580484312000L, 3L, false, PROJECT);
        queryMetrics3.setSubmitter(NORMAL_USER);
        QueryMetrics queryMetrics4 = createQueryMetrics(1895930712000L, 1L, false, "other");
        queryMetrics4.setSubmitter(NORMAL_USER);
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);
        queryHistoryDAO.insert(queryMetrics3);
        queryHistoryDAO.insert(queryMetrics4);

        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();

        // system-admin and project-admin can get all query history on current project
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(NORMAL_USER);
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(3, queryHistoryList.size());

        // non-admin can only get self query history on current project
        queryHistoryRequest.setAdmin(false);
        queryHistoryRequest.setUsername(NORMAL_USER);
        queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);
        Assert.assertEquals(1, queryHistoryList.size());
    }

    @Test
    public void testReadWriteJsonForQueryHistory() throws Exception {
        // write
        QueryMetrics queryMetrics1 = createQueryMetrics(1580311512000L, 1L, true, PROJECT);
        queryMetrics1.setQueryHistoryInfo(new QueryHistoryInfo(true, 3, true));
        QueryMetrics queryMetrics2 = createQueryMetrics(1580397912000L, 2L, false, PROJECT);
        queryMetrics2.setQueryHistoryInfo(new QueryHistoryInfo(false, 5, false));
        queryHistoryDAO.insert(queryMetrics1);
        queryHistoryDAO.insert(queryMetrics2);

        // read
        QueryHistoryRequest queryHistoryRequest = new QueryHistoryRequest();
        queryHistoryRequest.setAdmin(true);
        queryHistoryRequest.setUsername(ADMIN);
        queryHistoryRequest.setProject(PROJECT);
        List<QueryHistory> queryHistoryList = queryHistoryDAO.getQueryHistoriesByConditions(queryHistoryRequest, 10, 0);

        Assert.assertEquals(2, queryHistoryList.size());

        Assert.assertEquals(false, queryHistoryList.get(0).getQueryHistoryInfo().isExactlyMatch());
        Assert.assertEquals(5, queryHistoryList.get(0).getQueryHistoryInfo().getScanSegmentNum());
        Assert.assertEquals("PENDING", queryHistoryList.get(0).getQueryHistoryInfo().getState().toString());
        Assert.assertEquals(false, queryHistoryList.get(0).getQueryHistoryInfo().isExecutionError());

        Assert.assertEquals(true, queryHistoryList.get(1).getQueryHistoryInfo().isExactlyMatch());
        Assert.assertEquals(3, queryHistoryList.get(1).getQueryHistoryInfo().getScanSegmentNum());
        Assert.assertEquals("PENDING", queryHistoryList.get(1).getQueryHistoryInfo().getState().toString());
        Assert.assertEquals(true, queryHistoryList.get(1).getQueryHistoryInfo().isExecutionError());
    }

    @Test
    public void testGetQueryCountAndAvgDuration() throws Exception {
        // 2020-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580311512000L, 1000L, true, PROJECT));
        // 2020-01-30 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580397912000L, 2000L, false, PROJECT));
        // 2020-01-31 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1580484312000L, 3000L, false, PROJECT));
        // 2030-01-29 23:25:12
        queryHistoryDAO.insert(createQueryMetrics(1895930712000L, 4000L, false, PROJECT));

        // happy pass
        QueryStatistics statistics = queryHistoryDAO.getQueryCountAndAvgDuration(1580311512000L, 1580484312000L,
                PROJECT);
        Assert.assertEquals(2, statistics.getCount());
        Assert.assertEquals(1500, statistics.getMeanDuration(), 0.1);

        // no query history for this time period
        statistics = queryHistoryDAO.getQueryCountAndAvgDuration(1560311512000L, 1570311512000L, PROJECT);
        Assert.assertEquals(0, statistics.getCount());
        Assert.assertEquals(0, statistics.getMeanDuration(), 0.1);
    }

    private QueryMetrics createQueryMetrics(long queryTime, long duration, boolean indexHit, String project) {
        QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
        queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
        queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
        queryMetrics.setQueryDuration(duration);
        queryMetrics.setTotalScanBytes(863L);
        queryMetrics.setTotalScanCount(4096L);
        queryMetrics.setResultRowCount(500L);
        queryMetrics.setSubmitter("ADMIN");
        queryMetrics.setRealizations("0ad44339-f066-42e9-b6a0-ffdfa5aea48e#20000000001#Table Index");
        queryMetrics.setErrorType("");
        queryMetrics.setCacheHit(true);
        queryMetrics.setIndexHit(indexHit);
        queryMetrics.setQueryTime(queryTime);
        queryMetrics.setQueryFirstDayOfMonth(TimeUtil.getMonthStart(queryTime));
        queryMetrics.setQueryFirstDayOfWeek(TimeUtil.getWeekStart(queryTime));
        queryMetrics.setQueryDay(TimeUtil.getDayStart(queryTime));
        queryMetrics.setProjectName(project);
        queryMetrics.setQueryStatus("SUCCEEDED");
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);

        QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea");
        realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
        realizationMetrics.setDuration(4591L);
        realizationMetrics.setQueryTime(1586405449387L);
        realizationMetrics.setProjectName(project);

        List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
        realizationMetricsList.add(realizationMetrics);
        realizationMetricsList.add(realizationMetrics);
        queryMetrics.setRealizationMetrics(realizationMetricsList);
        return queryMetrics;
    }
}
