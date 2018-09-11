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
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryFilterRuleManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
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
import java.util.HashMap;
import java.util.List;

public class FavoriteQueryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static long now = System.currentTimeMillis();

    @Mock
    private QueryHistoryService queryHistoryService = Mockito.spy(QueryHistoryService.class);
    @InjectMocks
    private final FavoriteQueryService favoriteQueryService = Mockito.spy(new FavoriteQueryService());

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
        ReflectionTestUtils.setField(favoriteQueryService, "queryHistoryService", queryHistoryService);
    }

    @AfterClass
    public static void tearDown() {
        System.clearProperty("HADOOP_USER_NAME");
        staticCleanupTestMetadata();
    }

    private List<QueryHistory> stubUnFavoriteQuery(final String favoriteQueryName, final String favoriteSql) throws IOException {
        final QueryHistory queryHistory1 = new QueryHistory();
        final QueryHistory queryHistory2 = new QueryHistory();
        queryHistory1.setFavorite(favoriteQueryName);
        queryHistory2.setFavorite(favoriteQueryName);

        final FavoriteQuery favoriteQuery = Mockito.mock(FavoriteQuery.class);
        Mockito.when(favoriteQuery.getSql()).thenReturn(favoriteSql);

        final FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.when(favoriteQueryManager.get(favoriteQueryName)).thenReturn(favoriteQuery);

        final QueryHistoryManager queryHistoryManager = Mockito.mock(QueryHistoryManager.class);
        Mockito.when(queryHistoryManager.findQueryHistoryByFavorite(favoriteQueryName)).thenReturn(Lists.newArrayList(queryHistory1, queryHistory2));

        Mockito.when(favoriteQueryService.getFavoriteQueryManager(PROJECT)).thenReturn(favoriteQueryManager);
        Mockito.when(favoriteQueryService.getQueryHistoryManager(PROJECT)).thenReturn(queryHistoryManager);

        return Lists.newArrayList(queryHistory1, queryHistory2);
    }

    private void stubFavoriteQueries(List<QueryHistory> queryHistories) throws IOException {
        final QueryHistoryManager queryHistoryManager = Mockito.mock(QueryHistoryManager.class);
        final FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);

        final FavoriteQuery favoriteQuery1 = Mockito.mock(FavoriteQuery.class);
        Mockito.when(favoriteQuery1.getSql()).thenReturn(queryHistories.get(0).getSql());
        Mockito.when(favoriteQuery1.getUuid()).thenReturn("test_uuid");
        Mockito.when(favoriteQuery1.getStatus()).thenReturn(FavoriteQueryStatusEnum.ACCELERATING);

        for (QueryHistory queryHistory : queryHistories) {
            Mockito.when(queryHistoryManager.findQueryHistory(queryHistory.getUuid())).thenReturn(queryHistory);
        }

        Mockito.when(favoriteQueryManager.findFavoriteQueryBySql(queryHistories.get(0).getSql())).thenReturn(favoriteQuery1);
        Mockito.when(favoriteQueryManager.findFavoriteQueryBySql(queryHistories.get(1).getSql())).thenReturn(null);

        Mockito.when(favoriteQueryService.getQueryHistoryManager(PROJECT)).thenReturn(queryHistoryManager);
        Mockito.when(favoriteQueryService.getFavoriteQueryManager(PROJECT)).thenReturn(favoriteQueryManager);
    }

    private void stubQueryFilterRule(List<QueryFilterRule> rules) {
        final QueryFilterRuleManager manager = Mockito.mock(QueryFilterRuleManager.class);
        Mockito.when(manager.getAll()).thenReturn(rules);

        Mockito.when(favoriteQueryService.getQueryFilterRuleManager(PROJECT)).thenReturn(manager);
    }

    private void stubUnAcceleratedFavorites() {
        final FavoriteQueryManager manager = Mockito.mock(FavoriteQueryManager.class);
        Mockito.when(manager.getAll()).thenReturn(favoritesForTest());

        Mockito.when(favoriteQueryService.getFavoriteQueryManager(PROJECT)).thenReturn(manager);
    }

    private List<QueryHistory> queriesForTest() {
        QueryHistory queryHistory1 = new QueryHistory();
        queryHistory1.setUuid("query-0");
        queryHistory1.setSql("select * from existing_table_1");
        queryHistory1.setStartTime(now);
        queryHistory1.setLatency(200);
        queryHistory1.setQueryStatus(QueryHistoryStatusEnum.FAILED);
        QueryHistory queryHistory2 = new QueryHistory();
        queryHistory2.setUuid("query-1");
        queryHistory2.setSql("select * from existing_table_2");
        queryHistory2.setStartTime(now);
        queryHistory2.setLatency(30);
        queryHistory2.setQueryStatus(QueryHistoryStatusEnum.FAILED);
        QueryHistory queryHistory3 = new QueryHistory();
        queryHistory3.setStartTime(now + 1);
        queryHistory3.setUuid("query-2");
        queryHistory3.setLatency(70);
        queryHistory3.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        queryHistory3.setSql("select * from existing_table_2");

        return Lists.newArrayList(queryHistory1, queryHistory2, queryHistory3);
    }

    private List<QueryFilterRule> rulesForTest() {
        QueryFilterRule queryFilterRule1 = new QueryFilterRule();
        queryFilterRule1.setUuid("rule-0");
        queryFilterRule1.setEnabled(true);

        QueryFilterRule queryFilterRule2 = new QueryFilterRule();
        queryFilterRule2.setUuid("rule-1");
        queryFilterRule2.setEnabled(false);

        return Lists.newArrayList(queryFilterRule1, queryFilterRule2);
    }

    private List<FavoriteQuery> favoritesForTest() {
        FavoriteQuery favoriteQuery1 = new FavoriteQuery();
        favoriteQuery1.setUuid("favorite-0");
        favoriteQuery1.setSql("select * from existing_table_1");
        favoriteQuery1.setStatus(FavoriteQueryStatusEnum.WAITING);
        FavoriteQuery favoriteQuery2 = new FavoriteQuery();
        favoriteQuery2.setUuid("favorite-1");
        favoriteQuery2.setSql("select * from existing_table_2");
        favoriteQuery2.setStatus(FavoriteQueryStatusEnum.WAITING);

        return Lists.newArrayList(favoriteQuery1, favoriteQuery2);
    }

    @Test
    public void testUnFavorite() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "query");
        final String favoriteQuery = "favorite-query-0";
        final String favoriteSql = "select * from existing_table";
        List<QueryHistory> queryHistories = stubUnFavoriteQuery(favoriteQuery, favoriteSql);

        favoriteQueryService.unFavorite(PROJECT, Lists.newArrayList(favoriteQuery));
        Assert.assertFalse(queryHistories.get(0).isFavorite());
        Assert.assertFalse(queryHistories.get(1).isFavorite());
        Mockito.verify(favoriteQueryService).post(PROJECT, new HashMap<String, String>(){{put("select * from existing_table", "favorite-query-0");}}, false);
        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testFavorite() throws IOException {

        stubFavoriteQueries(queriesForTest());

        final List<FavoriteQuery> favoriteQueries = favoriteQueryService.favorite(PROJECT, Lists.newArrayList("query-0", "query-1", "query-2"));
        // suppose to have only two favorite queries returned because there are only two distinct sqls
        Assert.assertEquals(2, favoriteQueries.size());
        Assert.assertEquals("select * from existing_table_2", favoriteQueries.get(0).getSql());
        Assert.assertEquals("select * from existing_table_1", favoriteQueries.get(1).getSql());
        Assert.assertEquals("test_uuid", favoriteQueries.get(1).getUuid());
    }

    @Test
    public void testGetAll() throws IOException {
        final QueryHistoryManager queryHistoryManager = Mockito.mock(QueryHistoryManager.class);
        final FavoriteQueryManager favoriteQueryManager = Mockito.mock(FavoriteQueryManager.class);

        Mockito.when(queryHistoryManager.getAllQueryHistories()).thenReturn(queriesForTest());
        List<FavoriteQuery> favoriteQueries = favoritesForTest();
        Mockito.when(favoriteQueryManager.getAll()).thenReturn(favoriteQueries);

        Mockito.when(favoriteQueryService.getQueryHistoryManager(PROJECT)).thenReturn(queryHistoryManager);
        Mockito.when(favoriteQueryService.getFavoriteQueryManager(PROJECT)).thenReturn(favoriteQueryManager);

        favoriteQueryService.getAllFavoriteQueries(PROJECT);
        Assert.assertEquals(1, favoriteQueries.get(0).getFrequency());
        Assert.assertEquals(0f, favoriteQueries.get(0).getSuccessRate(), 0.1);
        Assert.assertEquals(200f, favoriteQueries.get(0).getAverageDuration(), 0.1);

        Assert.assertEquals(2, favoriteQueries.get(1).getFrequency());
        Assert.assertEquals(0.5f, favoriteQueries.get(1).getSuccessRate(), 0.1);
        Assert.assertEquals(50f, favoriteQueries.get(1).getAverageDuration(), 0.1);
        Assert.assertEquals(now + 1, favoriteQueries.get(1).getLastQueryTime());
    }

    @Test
    public void testMarkAutomatic() throws IOException {
        favoriteQueryService.markAutomatic(PROJECT);
        ProjectInstance projectInstance = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT);
        Assert.assertTrue(projectInstance.getConfig().isAutoMarkFavorite());
        favoriteQueryService.markAutomatic(PROJECT);
    }

    @Test
    public void testApplyAll() throws IOException {
        List<QueryFilterRule> rules = rulesForTest();
        stubQueryFilterRule(rules);

        favoriteQueryService.applyAll(PROJECT);
        for (QueryFilterRule rule : rules) {
            Assert.assertTrue(rule.isEnabled());
        }
    }

    @Test
    public void testTimeToAccelerate() {
        stubUnAcceleratedFavorites();

        HashMap<String, Object> data = favoriteQueryService.isTimeToAccelerate(PROJECT);
        Assert.assertEquals(2, data.get("size"));
        Assert.assertEquals(false, data.get("reach_threshold"));
    }

    @Test
    public void testAutoMarkFavorite() throws Exception {
        FavoriteQueryService.AutoMarkFavorite autoMarkFavorite = favoriteQueryService.new AutoMarkFavorite();
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.updateRandomUuid();
        queryHistory.setQueryId("auto-mark-favorite-query-id");
        QueryHistoryManager queryHistoryManager = QueryHistoryManager.getInstance(getTestConfig(), PROJECT);
        queryHistoryManager.save(queryHistory);

        QueryFilterRule.QueryHistoryCond cond = new QueryFilterRule.QueryHistoryCond();
        cond.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond.setField("queryId");
        cond.setRightThreshold("auto-mark-favorite-query-id");
        QueryFilterRule rule = new QueryFilterRule(Lists.newArrayList(cond), "test_rule", true);
        favoriteQueryService.saveQueryFilterRule(PROJECT, Lists.newArrayList(rule));
        System.setProperty("kylin.favorite.auto-mark-detection-interval", "1");
        favoriteQueryService.markAutomatic(PROJECT);

        autoMarkFavorite.start();

        Thread.sleep(1000 * 2);

        Assert.assertNotNull(queryHistoryManager.findQueryHistory(queryHistory.getUuid()).getFavorite());
        autoMarkFavorite.interrupt();
        favoriteQueryService.deleteQueryFilterRule(PROJECT, Lists.newArrayList(rule.getUuid()));
        favoriteQueryService.markAutomatic(PROJECT);
        System.clearProperty("kylin.favorite.auto-mark-detection-interval");
    }
}
