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

package io.kyligence.kap.server;


import com.google.common.collect.Lists;
import com.jayway.jsonpath.JsonPath;
import io.kyligence.kap.metadata.query.QueryFilterRule;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import io.kyligence.kap.metadata.query.QueryHistoryStatusEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.QueryFilterRequest;
import org.apache.kylin.source.jdbc.H2Database;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class NQueryControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");
        sqlRequest.setSql("SELECT * FROM test_country");

        final MvcResult result = mockMvc.perform(MockMvcRequestBuilders
                .post("/api/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andReturn();

        Assert.assertTrue(result.getResolvedException() instanceof InternalErrorException);
        Assert.assertTrue(StringUtils.contains(result.getResolvedException().getMessage(), "No realization found for OLAPContext"));

        QueryHistoryManager manager = QueryHistoryManager.getInstance(getTestConfig(), "default");
        List<QueryHistory> queryHistories = manager.getAllQueryHistories();
        QueryHistory newRecordedQuery = queryHistories.get(queryHistories.size() - 1);

        // assert if query history was saved
        Assert.assertEquals(5, queryHistories.size());
        Assert.assertEquals(sqlRequest.getSql(), newRecordedQuery.getSql());
        Assert.assertEquals(QueryHistoryStatusEnum.FAILED, newRecordedQuery.getQueryStatus());
    }

    @Test
    public void testPushDownQuery() throws Exception {
        System.setProperty("kylin.query.pushdown.runner-class-name", "org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl");
        System.setProperty("kylin.query.pushdown.converter-class-names", "org.apache.kylin.source.adhocquery.HivePushDownConverter");

        // Load H2 Tables (inner join)
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa",
                "");
        H2Database h2DB = new H2Database(h2Connection, getTestConfig(), "default");
        h2DB.loadAllTables();

        System.setProperty("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default;SCHEMA=DEFAULT");
        System.setProperty("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        System.setProperty("kylin.query.pushdown.jdbc.username", "sa");
        System.setProperty("kylin.query.pushdown.jdbc.password", "");

        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");

        String queryFileName = "src/test/resources/query/sql_pushdown/query04.sql";
        File sqlFile = new File(queryFileName);
        String sql = KylinTestBase.getTextFromFile(sqlFile);
        sqlRequest.setSql(sql);

        // get h2 query result for comparison
        Statement statement = h2Connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders
                .post("/api/query")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.results[0].length()")
                        .value(resultSet.getMetaData().getColumnCount()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.pushDown").value(true))
                .andDo(MockMvcResultHandlers.print())
                .andReturn();

        final long totalScanCount = JsonPath.compile("$.data.totalScanCount")
                .<Integer> read(mvcResult.getResponse().getContentAsString());
        final long totalScanBytes = JsonPath.compile("$.data.totalScanBytes")
                .<Integer> read(mvcResult.getResponse().getContentAsString());

        QueryHistoryManager manager = QueryHistoryManager.getInstance(getTestConfig(), "default");
        List<QueryHistory> queryHistories = manager.getAllQueryHistories();
        QueryHistory newRecordedQuery = queryHistories.get(queryHistories.size() - 1);

        // assert if query history was saved
        Assert.assertEquals(5, queryHistories.size());
        Assert.assertEquals(sqlRequest.getSql(), newRecordedQuery.getSql());
        Assert.assertEquals(QueryHistory.ADJ_PUSHDOWN, newRecordedQuery.getRealization().get(0));
        Assert.assertEquals(totalScanCount, newRecordedQuery.getTotalScanCount());
        Assert.assertEquals(totalScanBytes, newRecordedQuery.getTotalScanBytes());

        h2Connection.close();
        System.clearProperty("kylin.query.pushdown.runner-class-name");
        System.clearProperty("kylin.query.pushdown.converter-class-names");
        System.clearProperty("kylin.query.pushdown.jdbc.url");
        System.clearProperty("kylin.query.pushdown.jdbc.driver");
        System.clearProperty("kylin.query.pushdown.jdbc.username");
        System.clearProperty("kylin.query.pushdown.jdbc.password");
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject("default");
        sqlRequest.setSql("SELECT * FROM test_country");

        mockMvc.perform(MockMvcRequestBuilders
                .post("/api/query/prestate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());

    }

    @Test
    public void testGetMetadata() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/query/tables_and_columns")
                .param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print());

    }

    @Test
    public void testGetQueryHistory() throws Exception {
        QueryHistoryManager manager = QueryHistoryManager.getInstance(getTestConfig(), "default");
        List<QueryHistory> queryHistories = manager.getAllQueryHistories();

        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(queryHistories.size()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[0].uuid").value(queryHistories.get(0).getUuid()));
    }

    @Test
    public void testGetFilterQueryHistory() throws Exception {
        QueryFilterRequest request = new QueryFilterRequest();
        request.setProject("default");
        QueryFilterRule.QueryHistoryCond cond1 = new QueryFilterRule.QueryHistoryCond();
        cond1.setOp(QueryFilterRule.QueryHistoryCond.Operation.TO);
        cond1.setField("startTime");
        cond1.setLeftThreshold("1459362230010");
        cond1.setRightThreshold("1459362239990");
        QueryFilterRule.QueryHistoryCond cond2 = new QueryFilterRule.QueryHistoryCond();
        cond2.setOp(QueryFilterRule.QueryHistoryCond.Operation.EQUAL);
        cond2.setField("realization");
        cond2.setRightThreshold("[Pushdown]");
        QueryFilterRule rule = new QueryFilterRule(Lists.newArrayList(cond1, cond2), "test_rule", false);
        request.setRules(Lists.newArrayList(rule));

        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default")
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(1))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[0].query_id").value("query-1"));

    }
}
