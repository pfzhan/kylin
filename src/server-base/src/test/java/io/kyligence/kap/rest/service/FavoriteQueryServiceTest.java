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
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryJDBCDao;
import io.kyligence.kap.metadata.favorite.FavoriteQueryResponse;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.smart.NSmartMaster;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.QueryFilterRequest;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FavoriteQueryServiceTest extends ServiceTestBase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";

    private final String[] sqlsToAnalyze = new String[] { //
        "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where cal_dt = '2012-01-03' group by cal_dt, lstg_format_name", //
                "select cal_dt, lstg_format_name, sum(price) from test_kylin_fact where lstg_format_name = 'ABIN' group by cal_dt, lstg_format_name", //
                "select sum(price) from test_kylin_fact where cal_dt = '2012-01-03'", //
                "select lstg_format_name, sum(item_count), count(*) from test_kylin_fact group by lstg_format_name" //
    };

    FavoriteQueryJDBCDao favoriteQueryJDBCDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());

    @Mock
    private MockedQueryHistoryService queryHistoryService = Mockito.spy(new MockedQueryHistoryService());

    @InjectMocks
    private FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
    }

    private static void loadTestDataToH2() {
        FavoriteQueryJDBCDao favoriteQueryJDBCDao = FavoriteQueryJDBCDao.getInstance(getTestConfig());
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1", "sql1".hashCode(), PROJECT);
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setSuccessCount(1);
        favoriteQuery1.setLastQueryTime(10001);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2", "sql2".hashCode(), PROJECT);
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setSuccessCount(1);
        favoriteQuery2.setLastQueryTime(10002);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3", "sql3".hashCode(), PROJECT);
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setSuccessCount(1);
        favoriteQuery3.setLastQueryTime(10003);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.ACCELERATING);

        favoriteQueryJDBCDao.batchInsert(Lists.newArrayList(favoriteQuery1, favoriteQuery2, favoriteQuery3));
    }

    @Before
    public void setup() {
        createTestMetadata();
        ReflectionTestUtils.setField(favoriteQueryService, "queryHistoryService", queryHistoryService);
        getTestConfig().setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        getTestConfig().setProperty("kylin.favorite.storage-url", "kylin_favorite@jdbc,url=jdbc:h2:mem:db_default;MODE=MySQL,username=sa,password=,driverClassName=org.h2.Driver");
        loadTestDataToH2();
        favoriteQueryJDBCDao.initializeSqlPatternSet();
    }

    @After
    public void after() {
        favoriteQueryJDBCDao.dropTable();
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testInitFrequencyStatus() throws IOException {
        Mockito.when(favoriteQueryService.getQHTimeOffsetManager()).thenReturn(Mockito.mock(QueryHistoryTimeOffsetManager.class));
        Mockito.doReturn(queriesForTest()).when(queryHistoryService).getQueryHistories(Mockito.anyLong(), Mockito.anyLong());
        favoriteQueryService.initFrequencyStatus();

        Assert.assertEquals(24 * 60, favoriteQueryService.getFrequencyStatuses().size());
        Assert.assertEquals(3, favoriteQueryService.getOverAllStatus().getSqlPatternFreqMap().get(PROJECT).size());
        Assert.assertEquals(24 * 60, (int) favoriteQueryService.getOverAllStatus().getSqlPatternFreqMap().get(PROJECT).get("sql1"));

        FavoriteQueryService.FrequencyStatus firstStatus = favoriteQueryService.getFrequencyStatuses().pollFirst();
        FavoriteQueryService.FrequencyStatus lastStatus = favoriteQueryService.getFrequencyStatuses().pollLast();

        Assert.assertEquals(23, (lastStatus.getAddTime() - firstStatus.getAddTime()) / 1000 / 60 / 60);
    }

    @Test
    public void testGetFilterRulesAndUpdate() throws IOException {
        Map<String, Object> frequencyRuleResult = favoriteQueryService.getFrequencyRule(PROJECT);
        Assert.assertTrue((boolean) frequencyRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(0.1, (float) frequencyRuleResult.get("freqValue"), 0.1);

        Map<String, Object> submitterRuleResult = favoriteQueryService.getSubmitterRule(PROJECT);
        List<String> users = (ArrayList<String>) submitterRuleResult.get("users");
        Assert.assertTrue((boolean) submitterRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(3, users.size());

        Map<String, Object> durationRuleResult = favoriteQueryService.getDurationRule(PROJECT);
        List<Long> durationValues = (ArrayList<Long>) durationRuleResult.get("durationValue");
        Assert.assertTrue((boolean) durationRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(5, (long) durationValues.get(0));
        Assert.assertEquals(8, (long) durationValues.get(1));

        // the request of updating frequency rule
        QueryFilterRequest request = new QueryFilterRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        request.setFreqValue("0.2");

        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.FREQUENCY_RULE_NAME);
        frequencyRuleResult = favoriteQueryService.getFrequencyRule(PROJECT);
        Assert.assertFalse((boolean) frequencyRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(0.2, (float) frequencyRuleResult.get("freqValue"), 0.1);

        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));

        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.SUBMITTER_RULE_NAME);
        submitterRuleResult = favoriteQueryService.getSubmitterRule(PROJECT);
        users = (ArrayList<String>) submitterRuleResult.get("users");
        Assert.assertFalse((boolean) submitterRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(4, users.size());

        request.setDurationValue(new String[]{"0", "10"});

        favoriteQueryService.updateQueryFilterRule(request, QueryFilterRule.DURATION_RULE_NAME);
        durationRuleResult = favoriteQueryService.getDurationRule(PROJECT);
        durationValues = (ArrayList<Long>) durationRuleResult.get("durationValue");
        Assert.assertFalse((boolean) durationRuleResult.get(QueryFilterRule.ENABLE));
        Assert.assertEquals(0, (long) durationValues.get(0));
        Assert.assertEquals(10, (long) durationValues.get(1));
    }

    @Test
    public void testGetRulesWithError() {
        // assert get rule error
        try {
            favoriteQueryService.getFrequencyRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    QueryFilterRule.FREQUENCY_RULE_NAME), ex.getMessage());
        }

        try {
            favoriteQueryService.getSubmitterRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    QueryFilterRule.SUBMITTER_RULE_NAME), ex.getMessage());
        }

        try {
            favoriteQueryService.getDurationRule(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(),
                    QueryFilterRule.DURATION_RULE_NAME), ex.getMessage());
        }
    }

    private static List<QueryHistory> queriesForTest() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setProject(PROJECT);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setProject(PROJECT);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setProject(PROJECT);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setProject(PROJECT);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);

        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4);
    }

    private void stubUnAcceleratedSqlPatterns(List<String> sqls, String project) {
        Mockito.doReturn(sqls).when(favoriteQueryService).getUnAcceleratedSqlPattern(project);
    }

    @Test
    public void testGetAccelerateTips() throws IOException {
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);

        // case of no model
        Map<String, Object> newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(false, newten_data.get("reach_threshold"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        NSmartMaster smartMaster = new NSmartMaster(getTestConfig(), PROJECT_NEWTEN, sqlsToAnalyze);
        smartMaster.runAll();

        String[] sqlsForAddCuboidTest = new String[] {
                "select order_id from test_kylin_fact"
        };

        // case of adding a new cuboid
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsForAddCuboidTest), PROJECT_NEWTEN);
        newten_data = favoriteQueryService.getAccelerateTips(PROJECT_NEWTEN);
        Assert.assertEquals(1, newten_data.get("size"));
        Assert.assertEquals(1, newten_data.get("optimized_model_num"));

        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);

        System.setProperty("kylin.favorite.query-accelerate-threshold", "1");
        newten_data = favoriteQueryService.getAccelerateTips("newten");
        Assert.assertEquals(4, newten_data.get("size"));
        Assert.assertEquals(true, newten_data.get("reach_threshold"));
        Assert.assertEquals(0, newten_data.get("optimized_model_num"));
        System.clearProperty("kylin.favorite.query-accelerate-threshold");

        // when unaccelerated sql patterns list is empty
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(), PROJECT);
        Map<String, Object> data = favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertEquals(0, data.get("size"));
        Assert.assertEquals(false, data.get("reach_threshold"));
        Assert.assertEquals(0, data.get("optimized_model_num"));
    }

    @Test
    public void testAcceptAccelerate() throws PersistentException, IOException {
        getTestConfig().setProperty("kylin.server.mode", "query");

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
        } catch (Throwable ex) {
            Assert.assertEquals(ex.getMessage(), String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10));
        }

        // when there is no origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT_NEWTEN);
        favoriteQueryService.acceptAccelerate(PROJECT_NEWTEN, 4);
        Mockito.verify(favoriteQueryService).post(PROJECT_NEWTEN, Lists.newArrayList(sqlsToAnalyze), true);

        // when there is origin model
        stubUnAcceleratedSqlPatterns(Lists.newArrayList(sqlsToAnalyze), PROJECT);
        favoriteQueryService.acceptAccelerate(PROJECT, 4);
        Mockito.verify(favoriteQueryService).post(PROJECT, Lists.newArrayList(sqlsToAnalyze), true);

        try {
            favoriteQueryService.acceptAccelerate(PROJECT, 10);
            Mockito.verify(favoriteQueryService).post(PROJECT, Lists.newArrayList(sqlsToAnalyze), true);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals(String.format(MsgPicker.getMsg().getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH(), 10), ex.getMessage());
        }

        // when models are reconstructing
        try {
            getTestConfig().setProperty("kylin.favorite.batch-accelerate-size", "4");
            favoriteQueryService.acceptAccelerate(PROJECT, 4);
            Mockito.verify(favoriteQueryService).post(PROJECT, Lists.newArrayList(sqlsToAnalyze), true);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalStateException.class, ex.getClass());
            Assert.assertEquals("model all_fixed_length is reconstructing", ex.getMessage());
        }

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testIgnoreAccelerateTips() {
        Assert.assertFalse(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
        Mockito.when(favoriteQueryService.getUnAcceleratedSqlPattern(PROJECT)).thenReturn(Lists.newArrayList());
        favoriteQueryService.getAccelerateTips(PROJECT);
        Assert.assertTrue(favoriteQueryService.getIgnoreCountMap().containsKey(PROJECT));
        favoriteQueryService.ignoreAccelerate(PROJECT);
        Assert.assertEquals(2, (int) favoriteQueryService.getIgnoreCountMap().get(PROJECT));
    }

    @Test
    public void testManualFavorite() throws IOException, PersistentException {
        getTestConfig().setProperty("kylin.server.mode", "query");

        // sql pattern not exists
        FavoriteRequest request = new FavoriteRequest(PROJECT, "sql_pattern_not_exists", 1000);
        favoriteQueryService.manualFavorite(request);
        Mockito.verify(favoriteQueryService).post(PROJECT, Lists.newArrayList("sql_pattern_not_exists"), true);

        List<FavoriteQueryResponse> favoriteQueries = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0);
        FavoriteQuery newInsertedRow = favoriteQueries.get(favoriteQueries.size() - 1);
        Assert.assertEquals("sql_pattern_not_exists", newInsertedRow.getSqlPattern());
        Assert.assertEquals(favoriteQueries.size(), favoriteQueryService.getFavoriteQuerySize(PROJECT));

        // sql pattern exists but not accelerating
        request.setSqlPattern("sql1");
        favoriteQueryService.manualFavorite(request);
        Mockito.verify(favoriteQueryService).post(PROJECT, Lists.newArrayList("sql1"), true);

        favoriteQueries = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0);
        FavoriteQuery updatedRow = favoriteQueries.get(favoriteQueries.size() - 1);
        Assert.assertEquals(FavoriteQueryStatusEnum.ACCELERATING, updatedRow.getStatus());

        // sql pattern exists and is accelerating
        request.setSqlPattern("sql3");
        favoriteQueryService.manualFavorite(request);
        // assert this sql pattern did not post out
        Mockito.verify(favoriteQueryService, Mockito.never()).post(PROJECT, Lists.newArrayList("sql3"), true);

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testFilteredByFrequencyRule() {
        Map<String, Map<String, Integer>> sqlPatternFreqMap = Maps.newHashMap();
        Map<String, Integer> sqlPatternFreqInProj = Maps.newHashMap();
        sqlPatternFreqInProj.put("sql1", 1);
        sqlPatternFreqInProj.put("sql2", 2);
        sqlPatternFreqInProj.put("sql3", 3);
        sqlPatternFreqInProj.put("sql4", 4);
        sqlPatternFreqInProj.put("sql5", 5);
        sqlPatternFreqInProj.put("sql6", 6);
        sqlPatternFreqInProj.put("sql7", 7);
        sqlPatternFreqInProj.put("sql8", 8);
        sqlPatternFreqInProj.put("sql9", 9);
        sqlPatternFreqInProj.put("sql10", 9);
        sqlPatternFreqInProj.put("sql11", 10);
        sqlPatternFreqInProj.put("sql12", 10);

        sqlPatternFreqMap.put(PROJECT, sqlPatternFreqInProj);

        FavoriteQueryService.FrequencyStatus frequencyStatus = favoriteQueryService.new FrequencyStatus(System.currentTimeMillis());
        frequencyStatus.setSqlPatternFreqMap(sqlPatternFreqMap);

        Set<FavoriteQuery> candidates = new HashSet<>();
        Mockito.doReturn(frequencyStatus).when(favoriteQueryService).getOverAllStatus();

        favoriteQueryService.addCandidatesByFrequencyRule(candidates);

        Assert.assertEquals(2, candidates.size());
    }

    @Test
    public void testMatchRule() {
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setProject(PROJECT);

        // matches submitter rule
        queryHistory.setQuerySubmitter("userA");
        Assert.assertTrue(favoriteQueryService.matchRuleBySingleRecord(queryHistory));

        // matches duration rule
        queryHistory.setQuerySubmitter("not_matches_rule_submitter");
        queryHistory.setDuration(6*1000L);
        Assert.assertTrue(favoriteQueryService.matchRuleBySingleRecord(queryHistory));

        // matches no rules
        queryHistory.setDuration(0);
        Assert.assertFalse(favoriteQueryService.matchRuleBySingleRecord(queryHistory));

        QueryFilterRuleManager queryFilterRuleManager = Mockito.mock(QueryFilterRuleManager.class);
        Mockito.when(queryFilterRuleManager.getAllEnabled()).thenReturn(Lists.newArrayList(new QueryFilterRule()));
        Mockito.when(queryHistoryService.getQueryFilterRuleManager(PROJECT)).thenReturn(queryFilterRuleManager);

        try {
            favoriteQueryService.matchRuleBySingleRecord(queryHistory);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
        }
    }

    @Test
    public void testUpdateFrequencyStatus() {
        Mockito.doReturn(Lists.newArrayList()).when(queryHistoryService).getQueryHistories(Mockito.anyLong(), Mockito.anyLong());
        Map<String, Map<String, Integer>> freqMap = Maps.newHashMap();

        freqMap.put(PROJECT, new HashMap<String, Integer>(){{put("sql1", 1);put("sql2", 1);put("sql3", 1);}});
        freqMap.put(PROJECT_NEWTEN, new HashMap<String, Integer>(){{put("sql1", 1);put("sql2", 1);put("sql3", 1);}});

        FavoriteQueryService.FrequencyStatus frequencyStatus = favoriteQueryService.new FrequencyStatus(1000);
        frequencyStatus.setSqlPatternFreqMap(freqMap);

        // update frequency for an existing sql pattern
        frequencyStatus.updateFrequency(PROJECT, "sql1");
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT).get("sql1"));

        // update frequency for a new project
        frequencyStatus.updateFrequency("new_project", "sql1");
        Assert.assertEquals(3, frequencyStatus.getSqlPatternFreqMap().size());

        // update frequency for a new sql pattern
        frequencyStatus.updateFrequency(PROJECT, "sql4");
        Assert.assertEquals(4, frequencyStatus.getSqlPatternFreqMap().get(PROJECT).size());

        // add new status
        FavoriteQueryService.FrequencyStatus newStatus = favoriteQueryService.new FrequencyStatus(1000);
        Map<String, Map<String, Integer>> newFreqMap = Maps.newHashMap();
        newFreqMap.put("new_project_2", new HashMap<String, Integer>(){{put("sql1", 1);put("sql2", 1);put("sql3", 1);}});
        newFreqMap.put(PROJECT_NEWTEN, new HashMap<String, Integer>(){{put("sql1", 2);put("sql2", 1);put("sql3", 1);put("sql4", 10);}});
        newStatus.setSqlPatternFreqMap(newFreqMap);
        frequencyStatus.addStatus(newStatus);

        Assert.assertEquals(4, frequencyStatus.getSqlPatternFreqMap().size());
        Assert.assertEquals(3, frequencyStatus.getSqlPatternFreqMap().get("new_project_2").size());
        Assert.assertEquals(1, (int) frequencyStatus.getSqlPatternFreqMap().get("new_project_2").get("sql1"));
        Assert.assertEquals(4, frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).size());
        Assert.assertEquals(10, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql4"));
        Assert.assertEquals(3, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql1"));
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql2"));
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql3"));

        // remove status
        FavoriteQueryService.FrequencyStatus removedStatus = favoriteQueryService.new FrequencyStatus(1000);
        newFreqMap = Maps.newHashMap();
        newFreqMap.put(PROJECT_NEWTEN, new HashMap<String, Integer>(){{put("sql1", 2);put("sql2", 1);put("sql3", 1);put("sql4", 5);}});
        removedStatus.setSqlPatternFreqMap(newFreqMap);
        frequencyStatus.removeStatus(removedStatus);

        Assert.assertEquals(4, frequencyStatus.getSqlPatternFreqMap().size());
        Assert.assertEquals(5, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql4"));
        Assert.assertEquals(1, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql1"));
        Assert.assertEquals(1, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql2"));
        Assert.assertEquals(1, (int) frequencyStatus.getSqlPatternFreqMap().get(PROJECT_NEWTEN).get("sql3"));
    }

    @Test
    public void testUpdateFavoriteQueryStatistics() {
        // already loaded three favorite queries whose sql patterns are "sql1", "sql2", "sql3"
        long systemTime = queryHistoryService.getCurrentTime();
        int originFavoriteQuerySize = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size();
        FavoriteQueryService.CollectFavoriteStatisticsRunner updateRunner = favoriteQueryService.new CollectFavoriteStatisticsRunner();

        // first round, updated no favorite query
        Mockito.doReturn(systemTime).when(favoriteQueryService).getSystemTime();
        updateRunner.run();

        List<FavoriteQueryResponse> favoriteQueriesInDB = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0);
        Assert.assertEquals(originFavoriteQuerySize, favoriteQueriesInDB.size());

        // second round, updated two query histories
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod()).when(favoriteQueryService).getSystemTime();
        updateRunner.run();
        favoriteQueriesInDB = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0);

        for (FavoriteQuery favoriteQuery : favoriteQueriesInDB) {
            switch (favoriteQuery.getSqlPattern()) {
                case "sql1":
                    Assert.assertEquals(2, favoriteQuery.getTotalCount());
                    Assert.assertEquals(2, favoriteQuery.getSuccessCount());
                    break;
                case "sql2":
                    Assert.assertEquals(2, favoriteQuery.getTotalCount());
                    Assert.assertEquals(2, favoriteQuery.getSuccessCount());
                    break;
                case "sql3":
                    Assert.assertEquals(1, favoriteQuery.getTotalCount());
                    Assert.assertEquals(1, favoriteQuery.getSuccessCount());
                    break;
                default:
                    break;
            }
        }

        // third round, insert a query at 59s, so we get three query histories
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 2).when(favoriteQueryService).getSystemTime();

        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setSqlPattern("sql2");
        queryHistory.setProject(PROJECT);
        queryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory.setInsertTime(queryHistoryService.getCurrentTime() + 59 * 1000L);
        queryHistoryService.insert(queryHistory);

        updateRunner.run();
        favoriteQueriesInDB = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0);

        for (FavoriteQuery favoriteQuery : favoriteQueriesInDB) {
            switch (favoriteQuery.getSqlPattern()) {
                case "sql1":
                    Assert.assertEquals(2, favoriteQuery.getTotalCount());
                    Assert.assertEquals(2, favoriteQuery.getSuccessCount());
                    break;
                case "sql2":
                    Assert.assertEquals(3, favoriteQuery.getTotalCount());
                    Assert.assertEquals(2, favoriteQuery.getSuccessCount());
                    break;
                case "sql3":
                    Assert.assertEquals(2, favoriteQuery.getTotalCount());
                    Assert.assertEquals(2, favoriteQuery.getSuccessCount());
                    break;
                default:
                    break;
            }
        }
    }

    @Test
    public void testAutoMark() {
        /*
        There already have three favorite queries loaded in database, whose sql patterns are "sql1", "sql2", "sql3",
        so these three sql patterns will not be marked as favorite queries.

        The mocked query history service will be generating test data from 2018-02-01 00:00:00 to 2018-02-01 00:02:30 every 30 seconds,
        and the last auto mark time is 2018-01-01 00:00:00
         */
        long systemTime = queryHistoryService.getCurrentTime();
        int originFavoriteQuerySize = favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size();
        FavoriteQueryService.AutoMarkFavoriteRunner autoMarkFavoriteRunner = favoriteQueryService.new AutoMarkFavoriteRunner();

        // when current time is 00:00, auto mark runner scanned from 2018-01-01 00:00 to 2018-01-31 23:59:00
        Mockito.doReturn(systemTime).when(favoriteQueryService).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize, favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size());

        // current time is 02-01 00:01:00, triggered next round, runner scanned from 2018-01-31 23:59:00 to 2018-02-01 00:00:00, still get nothing
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod()).when(favoriteQueryService).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize, favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size());

        // at time 02-01 00:01:03, a query history is inserted into influxdb but with insert time as 00:00:59
        QueryHistory queryHistory = new QueryHistory("sql_pattern7", PROJECT,
                QueryHistory.QUERY_HISTORY_SUCCEEDED, "ADMIN", System.currentTimeMillis(), 6000L);
        queryHistory.setInsertTime(queryHistoryService.getCurrentTime() + 59 * 1000L);
        queryHistoryService.insert(queryHistory);

        // current time is 02-01 00:02:00, triggered next round, runner scanned from 2018-02-01 00:00:00 to 2018-02-01 00:01:00,
        // scanned three new queries, and inserted them into database
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 2).when(favoriteQueryService).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 3, favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size());

        // current time is 02-01 00:03:00, triggered next round, runner scanned from 2018-02-01 00:01:00 to 2018-02-01 00:02:00
        // scanned two new queries
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 3).when(favoriteQueryService).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 5, favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size());

        // current time is 02-01 00:04:00, runner scanned from 2018-02-01 00:02:00 to 2018-02-01 00:03:00
        // scanned two new queries, but one is failed, which is not expected to be marked as favorite query
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 4).when(favoriteQueryService).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 6, favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0).size());
    }
}
