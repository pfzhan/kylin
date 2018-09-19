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
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

public class QueryHistoryServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static long now = System.currentTimeMillis();

    @InjectMocks
    private QueryHistoryService queryHistoryService = Mockito.spy(new QueryHistoryService());

    @Before
    public void setUp() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private List<QueryHistory> mockQueryHistories() {
        QueryHistory query1 = new QueryHistory("query-1", "select * from test_table_1", now, 1000, "", "", "test_user_1");
        QueryHistory query2 = new QueryHistory("query-2", "select * from test_table_2", now + 1, 8000, "", "", "test_user_2");
        query2.setRealization(Lists.newArrayList(QueryHistory.ADJ_PUSHDOWN));
        query2.setAccelerateStatus(QueryHistory.QUERY_HISTORY_UNACCELERATED);
        QueryHistory query3 = new QueryHistory("query-3", "select * from test_table_1", now + 2, 1000, "", "", "test_user_1");
        QueryHistory query4 = new QueryHistory("query-4", "select * from test_table_2", now + 3, 8000, "", "", "test_user_3");
        query4.setAccelerateStatus(QueryHistory.QUERY_HISTORY_ACCELERATED);
        query4.setRealization(Lists.newArrayList(QueryHistory.ADJ_PUSHDOWN));
        QueryHistory query5 = new QueryHistory("query-5", "select * from test_table_1", now + 4, 8000, "", "", "test_user_1");
        QueryHistory query6 = new QueryHistory("query-6", "select * from test_table_1", now + 5, 8000, "", "", "test_user_2");
        QueryHistory query7 = new QueryHistory("query-7", "select * from test_table_1", now + 6, 8000, "", "", "test_user_3");

        return Lists.newArrayList(query1, query2, query3, query4, query5, query6, query7);
    }

    private List<QueryFilterRule> prepareRules() {
        QueryFilterRule.QueryHistoryCond cond1 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.START_TIME, String.valueOf(now), String.valueOf(now+5));
        QueryFilterRule.QueryHistoryCond cond2 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.LATENCY, "5", "10");
        QueryFilterRule.QueryHistoryCond cond3 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.SQL, null, "select * from test_table_2");
        QueryFilterRule.QueryHistoryCond cond4 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.ANSWERED_BY, null, "model");
        QueryFilterRule.QueryHistoryCond cond5 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.ANSWERED_BY, null, "pushdown");
        QueryFilterRule.QueryHistoryCond cond6 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.ACCELERATE_STATUS, null, QueryHistory.QUERY_HISTORY_ACCELERATED);
        QueryFilterRule.QueryHistoryCond cond7 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.ACCELERATE_STATUS, null, QueryHistory.QUERY_HISTORY_UNACCELERATED);
        QueryFilterRule.QueryHistoryCond cond8 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.LATENCY, null, "1");
        QueryFilterRule.QueryHistoryCond cond9 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.USER, null, "test_user_1");
        QueryFilterRule.QueryHistoryCond cond10 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.USER, null, "test_user_2");
        QueryFilterRule.QueryHistoryCond cond11 = new QueryFilterRule.QueryHistoryCond(QueryFilterRule.FREQUENCY, null,  "4");

        QueryFilterRule rule1 =  new QueryFilterRule(Lists.newArrayList(cond1, cond2, cond3, cond4, cond5, cond6, cond7), "test_rule_1", false);
        QueryFilterRule rule2 = new QueryFilterRule(Lists.newArrayList(cond8, cond9, cond10, cond11), "test_rule_2", false);

        return Lists.newArrayList(rule1, rule2);
    }

    @Test
    public void testFilterRule() throws IOException {
        List<QueryHistory> queryHistories = queryHistoryService.getQueryHistoriesByRules(prepareRules(), mockQueryHistories());

        Assert.assertEquals(4, queryHistories.size());
        Assert.assertEquals("query-2", queryHistories.get(0).getQueryId());
        Assert.assertEquals("query-4", queryHistories.get(1).getQueryId());
        Assert.assertEquals("query-5", queryHistories.get(2).getQueryId());
        Assert.assertEquals("query-6", queryHistories.get(3).getQueryId());

        // case of no rule
        queryHistories = queryHistoryService.getQueryHistoriesByRules(Lists.<QueryFilterRule>newArrayList(), mockQueryHistories());
        Assert.assertEquals(7, queryHistories.size());

        // case of not supported condition
        QueryFilterRule.QueryHistoryCond illegalCond = new QueryFilterRule.QueryHistoryCond("illegalField", "", "");
        QueryFilterRule illegalRule = new QueryFilterRule(Lists.newArrayList(illegalCond), "illegal_rule", false);
        try {
            queryHistoryService.getQueryHistoriesByRules(Lists.newArrayList(illegalRule), mockQueryHistories());
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals("The field of illegalField is not yet supported.", ex.getMessage());
        }
    }

    @Test
    public void testUpsertQueryHistory() throws IOException {
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setProject(PROJECT);
        sqlRequest.setSql("select * from existing_table_1");
        sqlRequest.setUsername("ADMIN");

        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setDuration(100L);
        sqlResponse.setTotalScanBytes(0);
        sqlResponse.setTotalScanCount(0);

        // exception case
        sqlResponse.setIsException(true);

        queryHistoryService.upsertQueryHistory(sqlRequest, sqlResponse, 0);
        QueryHistoryManager queryHistoryManager = QueryHistoryManager.getInstance(getTestConfig(), PROJECT);
        List<QueryHistory> queryHistories = queryHistoryManager.getAllQueryHistories();
        Assert.assertEquals(5, queryHistories.size());
        Assert.assertEquals(0L, queryHistories.get(queryHistories.size() - 1).getStartTime());
        Assert.assertEquals(QueryHistoryStatusEnum.FAILED, queryHistories.get(queryHistories.size() - 1).getQueryStatus());

        // push down case
        sqlResponse = new SQLResponse(null, null, null, 0, false, null, true, true);
        queryHistoryService.upsertQueryHistory(sqlRequest, sqlResponse, 1);
        queryHistories = queryHistoryManager.getAllQueryHistories();
        Assert.assertEquals(6, queryHistories.size());
        Assert.assertEquals(1L, queryHistories.get(queryHistories.size() - 2).getStartTime());
        Assert.assertEquals("[pushdown]", queryHistories.get(queryHistories.size() - 2).getRealization().toString());

        sqlResponse = new SQLResponse(null, null, null, 0, false, null, true, false);
        queryHistoryService.upsertQueryHistory(sqlRequest, sqlResponse, 2);
        queryHistories = queryHistoryManager.getAllQueryHistories();
        Assert.assertEquals(7, queryHistories.size());
        Assert.assertEquals(2L, queryHistories.get(queryHistories.size() - 3).getStartTime());
        Assert.assertTrue(queryHistories.get(queryHistories.size() - 3).isCubeHit());
    }

    @Test
    public void testParseFilterRequest() {
        // when there is any no filter rules
        QueryFilterRule rule = queryHistoryService.parseQueryFilterRuleRequest(-1 , -1 , -1, -1, "", null, null);
        Assert.assertNull(rule);

        // when there are some rules
        rule = queryHistoryService.parseQueryFilterRuleRequest(0, 1000, 100, 1000, "select * from test_country",
                Lists.newArrayList("pushdown", "modelName"), Lists.newArrayList(QueryHistory.QUERY_HISTORY_UNACCELERATED, QueryHistory.QUERY_HISTORY_ACCELERATED));
        Assert.assertNotNull(rule);
        Assert.assertEquals(7, rule.getConds().size());

        // illegal arguments
        try {
            queryHistoryService.parseQueryFilterRuleRequest(-1, -1, -1, -1, "", Lists.newArrayList("pushdown", "modelName", "illegal_arg"), null);
        } catch (Throwable ex) {
            Assert.assertEquals(IllegalArgumentException.class, ex.getClass());
            Assert.assertEquals("Not supported filter condition: illegal_arg", ex.getMessage());
        }
    }
}
