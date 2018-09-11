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
        QueryHistory query1 = new QueryHistory("query-1", "select * from test_table_1", now, 1000, "", "", "");
        QueryHistory query2 = new QueryHistory("query-2", "select * from test_table_1", now + 1, 1000, "", "", "");
        QueryHistory query3 = new QueryHistory("query-3", "select * from test_table_1", now + 2, 100, "", "", "");
        query3.setQueryStatus(QueryHistoryStatusEnum.SUCCEEDED);
        QueryHistory query4 = new QueryHistory("query-4", "select * from test_table_2", now + 3, 100, "", "", "");
        query4.setQueryStatus(QueryHistoryStatusEnum.FAILED);
        QueryHistory query5 = new QueryHistory("query-5", "select * from test_table_1", now + 4, 100, "", "", "");
        query5.setQueryStatus(QueryHistoryStatusEnum.FAILED);
        QueryHistory query6 = new QueryHistory("query-6", "select * from test_table_1", 5, 100, "", "", "");
        query5.setQueryStatus(QueryHistoryStatusEnum.FAILED);

        return Lists.newArrayList(query1, query2, query3, query4, query5, query6);
    }

    private List<QueryFilterRule> prepareRules() {
        QueryFilterRule.QueryHistoryCond cond1 = new QueryFilterRule.QueryHistoryCond();
        cond1.setOp(QueryFilterRule.QueryHistoryCond.Operation.TO);
        cond1.setField("startTime");
        cond1.setLeftThreshold(String.valueOf(now));
        cond1.setRightThreshold(String.valueOf(now + 5));

        QueryFilterRule.QueryHistoryCond cond2 = new QueryFilterRule.QueryHistoryCond();
        cond2.setOp(QueryFilterRule.QueryHistoryCond.Operation.GREATER);
        cond2.setField("latency");
        cond2.setRightThreshold("100");

        QueryFilterRule.QueryHistoryCond cond3 = new QueryFilterRule.QueryHistoryCond();
        cond3.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond3.setField("queryStatus");
        cond3.setRightThreshold("FAILED");

        QueryFilterRule.QueryHistoryCond cond4 = new QueryFilterRule.QueryHistoryCond();
        cond4.setOp(QueryFilterRule.QueryHistoryCond.Operation.CONTAIN);
        cond4.setField("sql");
        cond4.setRightThreshold("test_table_1");

        QueryFilterRule.QueryHistoryCond cond5 = new QueryFilterRule.QueryHistoryCond();
        cond5.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond5.setField("frequency");
        cond5.setRightThreshold("4");

        QueryFilterRule rule1 =  new QueryFilterRule(Lists.newArrayList(cond1, cond2), "test_rule_1", false);
        QueryFilterRule rule2 = new QueryFilterRule(Lists.newArrayList(cond3, cond4, cond5), "test_rule_2", false);

        return Lists.newArrayList(rule1, rule2);
    }

    @Test
    public void testFilterRule() throws IOException {
        List<QueryHistory> queryHistories = queryHistoryService.getQueryHistoriesByRules(prepareRules(), mockQueryHistories());

        Assert.assertEquals(2, queryHistories.size());
        Assert.assertEquals("query-2", queryHistories.get(0).getQueryId());
        Assert.assertEquals("query-5", queryHistories.get(1).getQueryId());
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
        Assert.assertEquals(0L, queryHistories.get(0).getStartTime());
        Assert.assertEquals(QueryHistoryStatusEnum.FAILED, queryHistories.get(0).getQueryStatus());

        // push down case
        sqlResponse = new SQLResponse(null, null, null, 0, false, null, true, true);
        queryHistoryService.upsertQueryHistory(sqlRequest, sqlResponse, 1);
        queryHistories = queryHistoryManager.getAllQueryHistories();
        Assert.assertEquals(6, queryHistories.size());
        Assert.assertEquals(1L, queryHistories.get(1).getStartTime());
        Assert.assertEquals("[Pushdown]", queryHistories.get(1).getRealization().toString());
    }
}
