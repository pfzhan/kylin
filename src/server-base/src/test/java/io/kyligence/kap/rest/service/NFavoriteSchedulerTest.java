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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.AccelerateRatio;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;

public class NFavoriteSchedulerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private NFavoriteScheduler favoriteScheduler;

    @Before
    public void setUp() {
        createTestMetadata();
        createTestFavoriteQuery();
        setUpTimeOffset();
        favoriteScheduler = Mockito.spy(new NFavoriteScheduler(PROJECT));
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    private List<QueryHistory> queriesForTest() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setSqlPattern("sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory1.setDuration(1000L);
        queryHistory1.setQueryTime(1001);
        queryHistory1.setAnsweredBy("CONSTANTS");

        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setSqlPattern("sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setDuration(1000L);
        queryHistory2.setQueryTime(1002);
        queryHistory2.setAnsweredBy("HIVE");

        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setSqlPattern("sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setDuration(1000L);
        queryHistory3.setQueryTime(1003);
        queryHistory3.setAnsweredBy("HIVE");

        QueryHistory queryHistory4 = new QueryHistory();
        queryHistory4.setSqlPattern("sql3");
        queryHistory4.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory4.setDuration(1000L);
        queryHistory4.setQueryTime(1004);
        queryHistory4.setAnsweredBy("HIVE");

        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3, queryHistory4);
    }

    private void createTestFavoriteQuery() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql1");
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setSuccessCount(1);
        favoriteQuery1.setLastQueryTime(10001);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql2");
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setSuccessCount(1);
        favoriteQuery2.setLastQueryTime(10002);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql3");
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setSuccessCount(1);
        favoriteQuery3.setLastQueryTime(10003);
        favoriteQuery3.setStatus(FavoriteQueryStatusEnum.ACCELERATING);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        favoriteQueryManager.create(new HashSet(){{add(favoriteQuery1);add(favoriteQuery2);add(favoriteQuery3);}});
    }

    private void createUnacceleratedFavoriteQueries() {
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQuery favoriteQuery1 = new FavoriteQuery("sql4");
        favoriteQuery1.setTotalCount(1);
        favoriteQuery1.setSuccessCount(1);
        favoriteQuery1.setLastQueryTime(10001);
        favoriteQuery1.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery2 = new FavoriteQuery("sql5");
        favoriteQuery2.setTotalCount(1);
        favoriteQuery2.setSuccessCount(1);
        favoriteQuery2.setLastQueryTime(10002);
        favoriteQuery2.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        FavoriteQuery favoriteQuery3 = new FavoriteQuery("sql6");
        favoriteQuery3.setTotalCount(1);
        favoriteQuery3.setSuccessCount(1);
        favoriteQuery3.setLastQueryTime(10003);
        favoriteQuery3.setChannel(FavoriteQuery.CHANNEL_FROM_RULE);

        favoriteQueryManager.create(new HashSet(){{add(favoriteQuery1);add(favoriteQuery2);add(favoriteQuery3);}});
    }

    @Test
    public void testInitFrequencyStatus() {
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(queriesForTest()).when(queryHistoryDAO).getQueryHistoriesByTime(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(queryHistoryDAO).when(favoriteScheduler).getQueryHistoryDao();

        favoriteScheduler.initFrequencyStatus();
        Assert.assertEquals(24 * 60, favoriteScheduler.getFrequencyStatuses().size());
        Assert.assertEquals(2, favoriteScheduler.getOverAllStatus().getSqlPatternFreqMap().size());
        Assert.assertNull(favoriteScheduler.getOverAllStatus().getSqlPatternFreqMap().get("sql1"));
        Assert.assertEquals(24 * 60,
                (int) favoriteScheduler.getOverAllStatus().getSqlPatternFreqMap().get("sql2"));

        NFavoriteScheduler.FrequencyStatus firstStatus = favoriteScheduler.getFrequencyStatuses().pollFirst();
        NFavoriteScheduler.FrequencyStatus lastStatus = favoriteScheduler.getFrequencyStatuses().pollLast();

        Assert.assertEquals(23, (lastStatus.getAddTime() - firstStatus.getAddTime()) / 1000 / 60 / 60);
    }

    @Test
    public void testFilteredByFrequencyRule() {
        Map<String, Integer> sqlPatternFreqMap = Maps.newHashMap();
        sqlPatternFreqMap.put("sql1", 1);
        sqlPatternFreqMap.put("sql2", 2);
        sqlPatternFreqMap.put("sql3", 3);
        sqlPatternFreqMap.put("sql4", 4);
        sqlPatternFreqMap.put("sql5", 5);
        sqlPatternFreqMap.put("sql6", 6);
        sqlPatternFreqMap.put("sql7", 7);
        sqlPatternFreqMap.put("sql8", 8);
        sqlPatternFreqMap.put("sql9", 9);
        sqlPatternFreqMap.put("sql10", 9);
        sqlPatternFreqMap.put("sql11", 10);
        sqlPatternFreqMap.put("sql12", 10);

        NFavoriteScheduler.FrequencyStatus frequencyStatus = favoriteScheduler.new FrequencyStatus(
                System.currentTimeMillis());
        frequencyStatus.setSqlPatternFreqMap(sqlPatternFreqMap);

        Set<FavoriteQuery> candidates = new HashSet<>();
        Mockito.doReturn(frequencyStatus).when(favoriteScheduler).getOverAllStatus();

        favoriteScheduler.addCandidatesByFrequencyRule(candidates);

        Assert.assertEquals(2, candidates.size());
    }

    @Test
    public void testFailedQueryHistoryNotAutoMarkedByFrequencyRule() {
        NFavoriteScheduler.AutoFavoriteRunner autoMarkRunner = favoriteScheduler.new AutoFavoriteRunner();
        QueryHistoryTimeOffsetManager timeOffsetManager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), PROJECT);
        long startTime = timeOffsetManager.get().getAutoMarkTimeOffset();
        Mockito.doReturn(startTime + getTestConfig().getQueryHistoryScanPeriod() * 2).when(favoriteScheduler).getSystemTime();

        QueryHistory succeededQueryHistory = new QueryHistory();
        succeededQueryHistory.setSqlPattern("succeeded_query");
        succeededQueryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        succeededQueryHistory.setQueryTime(1001);
        succeededQueryHistory.setQuerySubmitter("ADMIN");
        succeededQueryHistory.setAnsweredBy("Agg Index");

        QueryHistory failedQueryHistory = new QueryHistory();
        failedQueryHistory.setSqlPattern("failed_query");
        failedQueryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        failedQueryHistory.setQueryTime(1001);
        failedQueryHistory.setQuerySubmitter("ADMIN");
        failedQueryHistory.setAnsweredBy("Unknown");

        // queries with constants will not be recorded down
        QueryHistory queryWithConstants = new QueryHistory();
        queryWithConstants.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryWithConstants.setSqlPattern("select * from table where 1 <> 1");
        queryWithConstants.setAnsweredBy("CONSTANTS");

        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList(succeededQueryHistory, failedQueryHistory, queryWithConstants)).when(queryHistoryDAO).getQueryHistoriesByTime(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(queryHistoryDAO).when(favoriteScheduler).getQueryHistoryDao();

        autoMarkRunner.run();

        NFavoriteScheduler.FrequencyStatus overallStatus = favoriteScheduler.getOverAllStatus();
        Map<String, Integer> sqlPatternFreqInProj = overallStatus.getSqlPatternFreqMap();

        Assert.assertEquals(1, sqlPatternFreqInProj.size());
        Assert.assertEquals(1, (int) sqlPatternFreqInProj.get("succeeded_query"));
    }

    @Test
    public void testMatchRule() {
        QueryHistory queryHistory = new QueryHistory();

        // matches submitter rule
        queryHistory.setQuerySubmitter("userA");
        Assert.assertTrue(favoriteScheduler.matchRuleBySingleRecord(queryHistory));

        // matches duration rule
        queryHistory.setQuerySubmitter("not_matches_rule_submitter");
        queryHistory.setDuration(6 * 1000L);
        Assert.assertTrue(favoriteScheduler.matchRuleBySingleRecord(queryHistory));

        // matches no rules
        queryHistory.setDuration(0);
        Assert.assertFalse(favoriteScheduler.matchRuleBySingleRecord(queryHistory));
    }

    @Test
    public void testUpdateFrequencyStatus() {
        QueryHistoryDAO queryHistoryDAO = Mockito.mock(QueryHistoryDAO.class);
        Mockito.doReturn(Lists.newArrayList()).when(queryHistoryDAO).getQueryHistoriesByTime(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(queryHistoryDAO).when(favoriteScheduler).getQueryHistoryDao();
        Map<String, Integer> freqMap = Maps.newHashMap();

        freqMap.put("sql1", 1);
        freqMap.put("sql2", 1);
        freqMap.put("sql3", 1);

        NFavoriteScheduler.FrequencyStatus frequencyStatus = favoriteScheduler.new FrequencyStatus(1000);
        frequencyStatus.setSqlPatternFreqMap(freqMap);

        // update frequency for an existing sql pattern
        frequencyStatus.updateFrequency("sql1");
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get("sql1"));

        // update frequency for a new sql pattern
        frequencyStatus.updateFrequency("sql4");
        Assert.assertEquals(4, frequencyStatus.getSqlPatternFreqMap().size());

        // add new status
        NFavoriteScheduler.FrequencyStatus newStatus = favoriteScheduler.new FrequencyStatus(1000);
        Map<String, Integer> newFreqMap = Maps.newHashMap();
        newFreqMap.put("sql1", 2);
        newFreqMap.put("sql2", 1);
        newFreqMap.put("sql3", 1);
        newFreqMap.put("sql5", 10);

        newStatus.setSqlPatternFreqMap(newFreqMap);
        frequencyStatus.addStatus(newStatus);

        Assert.assertEquals(5, frequencyStatus.getSqlPatternFreqMap().size());
        Assert.assertEquals(4, (int) frequencyStatus.getSqlPatternFreqMap().get("sql1"));
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get("sql2"));
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get("sql3"));
        Assert.assertEquals(1, (int) frequencyStatus.getSqlPatternFreqMap().get("sql4"));
        Assert.assertEquals(10, (int) frequencyStatus.getSqlPatternFreqMap().get("sql5"));

        // remove status
        NFavoriteScheduler.FrequencyStatus removedStatus = favoriteScheduler.new FrequencyStatus(1000);
        newFreqMap = Maps.newHashMap();
        newFreqMap.put("sql1", 2);
        newFreqMap.put("sql2", 2);
        newFreqMap.put("sql3", 2);
        newFreqMap.put("sql5", 5);

        removedStatus.setSqlPatternFreqMap(newFreqMap);
        frequencyStatus.removeStatus(removedStatus);

        Assert.assertEquals(3, frequencyStatus.getSqlPatternFreqMap().size());
        Assert.assertEquals(5, (int) frequencyStatus.getSqlPatternFreqMap().get("sql5"));
        Assert.assertEquals(2, (int) frequencyStatus.getSqlPatternFreqMap().get("sql1"));
    }

    @Test
    public void testUpdateFavoriteQueryStatistics() {
        MockedQueryHistoryDao mockedQueryHistoryDao = new MockedQueryHistoryDao(getTestConfig(), PROJECT);
        Mockito.doReturn(mockedQueryHistoryDao).when(favoriteScheduler).getQueryHistoryDao();
        // already loaded three favorite queries whose sql patterns are "sql1", "sql2", "sql3"
        long systemTime = mockedQueryHistoryDao.getCurrentTime();
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        int originFavoriteQuerySize = favoriteQueryManager.getAll().size();
        NFavoriteScheduler.UpdateFavoriteStatisticsRunner updateRunner = favoriteScheduler.new UpdateFavoriteStatisticsRunner();

        // first round, updated no favorite query
        Mockito.doReturn(systemTime).when(favoriteScheduler).getSystemTime();
        updateRunner.run();

        List<FavoriteQuery> currentFavoriteQueries = favoriteQueryManager.getAll();
        Assert.assertEquals(originFavoriteQuerySize, currentFavoriteQueries.size());

        // second round, updated two query histories
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod()).when(favoriteScheduler)
                .getSystemTime();
        updateRunner.run();
        currentFavoriteQueries = favoriteQueryManager.getAll();

        for (FavoriteQuery favoriteQuery : currentFavoriteQueries) {
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
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 2).when(favoriteScheduler)
                .getSystemTime();

        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setSqlPattern("sql2");
        queryHistory.setQueryStatus(QueryHistory.QUERY_HISTORY_FAILED);
        queryHistory.setInsertTime(mockedQueryHistoryDao.getCurrentTime() + 59 * 1000L);
        mockedQueryHistoryDao.insert(queryHistory);

        updateRunner.run();
        currentFavoriteQueries = favoriteQueryManager.getAll();

        for (FavoriteQuery favoriteQuery : currentFavoriteQueries) {
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

    private void setUpTimeOffset() {
        QueryHistoryTimeOffsetManager timeOffsetManager = QueryHistoryTimeOffsetManager.getInstance(getTestConfig(), PROJECT);
        String date = "2018-01-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        long autoFavoriteTimeOffset = 0;
        long updateFavoriteTimeOffset = 0;
        try {
            autoFavoriteTimeOffset = format.parse(date).getTime();
            updateFavoriteTimeOffset = format.parse(date).getTime();
        } catch (ParseException e) {
            // ignore
        }
        QueryHistoryTimeOffset queryHistoryTimeOffset = new QueryHistoryTimeOffset(autoFavoriteTimeOffset, updateFavoriteTimeOffset);
        timeOffsetManager.save(queryHistoryTimeOffset);
    }

    @Test
    public void testAutoMark() {
        /*
        There already have three favorite queries loaded in database, whose sql patterns are "sql1", "sql2", "sql3",
        so these three sql patterns will not be marked as favorite queries.

        The mocked query history service will be generating test data from 2018-02-01 00:00:00 to 2018-02-01 00:02:30 every 30 seconds,
        and the last auto mark time is 2018-01-01 00:00:00
         */
        MockedQueryHistoryDao mockedQueryHistoryDao = new MockedQueryHistoryDao(getTestConfig(), PROJECT);
        Mockito.doReturn(mockedQueryHistoryDao).when(favoriteScheduler).getQueryHistoryDao();
        long systemTime = mockedQueryHistoryDao.getCurrentTime();
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);
        int originFavoriteQuerySize = favoriteQueryManager.getAll().size();
        NFavoriteScheduler.AutoFavoriteRunner autoMarkFavoriteRunner = favoriteScheduler.new AutoFavoriteRunner();

        // when current time is 00:00, auto mark runner scanned from 2018-01-01 00:00 to 2018-01-31 23:59:00
        Mockito.doReturn(systemTime).when(favoriteScheduler).getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize, favoriteQueryManager.getAll().size());

        // current time is 02-01 00:01:00, triggered next round, runner scanned from 2018-01-31 23:59:00 to 2018-02-01 00:00:00, still get nothing
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod()).when(favoriteScheduler)
                .getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize, favoriteQueryManager.getAll().size());

        // at time 02-01 00:01:03, a query history is inserted into influxdb but with insert time as 00:00:59
        QueryHistory queryHistory = new QueryHistory("sql_pattern7", QueryHistory.QUERY_HISTORY_SUCCEEDED,
                "ADMIN", System.currentTimeMillis(), 6000L);
        queryHistory.setInsertTime(mockedQueryHistoryDao.getCurrentTime() + 59 * 1000L);
        queryHistory.setAnsweredBy("HIVE");
        mockedQueryHistoryDao.insert(queryHistory);

        // current time is 02-01 00:02:00, triggered next round, runner scanned from 2018-02-01 00:00:00 to 2018-02-01 00:01:00,
        // scanned three new queries, and inserted them into database
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 2).when(favoriteScheduler)
                .getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 3, favoriteQueryManager.getAll().size());

        // current time is 02-01 00:03:00, triggered next round, runner scanned from 2018-02-01 00:01:00 to 2018-02-01 00:02:00
        // scanned two new queries
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 3).when(favoriteScheduler)
                .getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 5, favoriteQueryManager.getAll().size());

        // current time is 02-01 00:04:00, runner scanned from 2018-02-01 00:02:00 to 2018-02-01 00:03:00
        // scanned two new queries, but one is failed, which is not expected to be marked as favorite query, and the other is in blacklist
        // so no new query will be inserted to database
        Mockito.doReturn(systemTime + getTestConfig().getQueryHistoryScanPeriod() * 4).when(favoriteScheduler)
                .getSystemTime();
        autoMarkFavoriteRunner.run();
        Assert.assertEquals(originFavoriteQuerySize + 5, favoriteQueryManager.getAll().size());

        AccelerateRatioManager ratioManager = AccelerateRatioManager.getInstance(getTestConfig(), PROJECT);
        AccelerateRatio ratio = ratioManager.get();
        Assert.assertEquals(9, ratio.getOverallQueryNum());
        Assert.assertEquals(3, ratio.getQueryNumOfMarkedAsFavorite());
    }

    @Test
    public void testAutoMark_AutoApply_AutoAccelerate() throws IOException {
        // load some unacceleratd favorite queries
        createUnacceleratedFavoriteQueries();
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-auto-apply",
                "true");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold", "2");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-batch-enable",
                "true");
        projectManager.updateProject(projectInstanceUpdate);

        MockedQueryHistoryDao mockedQueryHistoryDao = new MockedQueryHistoryDao(getTestConfig(), PROJECT);
        Mockito.doReturn(mockedQueryHistoryDao).when(favoriteScheduler).getQueryHistoryDao();
        Mockito.doReturn(mockedQueryHistoryDao.getCurrentTime()).when(favoriteScheduler).getSystemTime();

        NFavoriteScheduler.AutoFavoriteRunner autoMarkFavoriteRunner = favoriteScheduler.new AutoFavoriteRunner();

        EventDao eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        eventDao.deleteAllEvents();
        autoMarkFavoriteRunner.run();
//        List<Event> events = eventDao.getEvents();
//        Assert.assertEquals(1, events.size());
//        AccelerateEvent accelerateEvent = (AccelerateEvent) events.get(0);
//        //sql1,sql2,sql4,sql5,sql6
//        Assert.assertEquals(5, accelerateEvent.getSqlPatterns().size());

    }

    @Test
    public void testAutoMark_AutoApply_NotReachThreshold() throws IOException {
        // load some unaccelerated favorite queries
        createUnacceleratedFavoriteQueries();
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-auto-apply",
                "true");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold", "6");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-batch-enable",
                "true");
        projectManager.updateProject(projectInstanceUpdate);

        Mockito.doReturn(new MockedQueryHistoryDao(getTestConfig(), PROJECT).getCurrentTime()).when(favoriteScheduler).getSystemTime();

        NFavoriteScheduler.AutoFavoriteRunner autoMarkFavoriteRunner = favoriteScheduler.new AutoFavoriteRunner();

        EventDao eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        eventDao.deleteAllEvents();
        autoMarkFavoriteRunner.run();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(0, events.size());
    }

    @Test
    public void testAutoMark_UserApply_ReachThreshold() throws IOException {
        // load some unaccelerated favorite queries
        createUnacceleratedFavoriteQueries();
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-auto-apply",
                "false");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold", "3");
        projectInstanceUpdate.getOverrideKylinProps().put("kylin.favorite.query-accelerate-threshold-batch-enable",
                "true");
        projectManager.updateProject(projectInstanceUpdate);

        Mockito.doReturn(new MockedQueryHistoryDao(getTestConfig(), PROJECT).getCurrentTime()).when(favoriteScheduler).getSystemTime();

        NFavoriteScheduler.AutoFavoriteRunner autoMarkFavoriteRunner = favoriteScheduler.new AutoFavoriteRunner();

        EventDao eventDao = EventDao.getInstance(getTestConfig(), PROJECT);
        eventDao.deleteAllEvents();
        autoMarkFavoriteRunner.run();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(0, events.size());
    }
}
