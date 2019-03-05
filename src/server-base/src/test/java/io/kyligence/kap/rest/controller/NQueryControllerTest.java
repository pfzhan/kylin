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

package io.kyligence.kap.rest.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.rest.service.KapQueryService;
import io.kyligence.kap.rest.service.QueryHistoryService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.service.QueryService;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * @author xduo
 */
public class NQueryControllerTest {
    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private KapQueryService kapQueryService;

    @Mock
    private QueryHistoryService queryHistoryService;

    @InjectMocks
    private NQueryController nQueryController = Mockito.spy(new NQueryController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    private PrepareSqlRequest mockPrepareSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        return sqlRequest;
    }

    private SaveSqlRequest mockSaveSqlRequest(String queryName) {
        final SaveSqlRequest sqlRequest = new SaveSqlRequest();
        sqlRequest.setName(queryName);
        return sqlRequest;
    }

    @Test
    public void testQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockPrepareSqlRequest()))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).query((PrepareSqlRequest) Mockito.any());
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = mockPrepareSqlRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).prepareQuery((PrepareSqlRequest) Mockito.any());
    }

    @Test
    public void testSaveQuery() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("query_01");
        sqlRequest.setSql("select * from test_kylin_fact");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testSaveQueryWithEmptyQueryName() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.content().string(containsString("Query name should not be empty.")));

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testSaveQueryWithInvalidQueryName() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("query%");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.content().string(containsString("Query name should only contain alphanumerics and underscores.")));

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testRemoveSavedQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/saved_queries/{project}/{id}", "default", "1")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).removeSavedQuery("default", "1");
    }

    @Test
    public void testGetSavedQueries() throws Exception {
        Mockito.when(kapQueryService.getSavedQueries("ADMIN", "default")).thenReturn(mockSavedQueries());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.saved_queries.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.saved_queries[0].name").value(7))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.saved_queries[1].name").value(8))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.saved_queries[2].name").value(9));

        Mockito.verify(nQueryController).getSavedQueries("default", 2, 3);
    }

    private QueryService.QueryRecord mockSavedQueries() {
        final List<Query> queries = new ArrayList<>();
        queries.add(new Query("1", PROJECT, "", ""));
        queries.add(new Query("2", PROJECT, "", ""));
        queries.add(new Query("3", PROJECT, "", ""));
        queries.add(new Query("4", PROJECT, "", ""));
        queries.add(new Query("5", PROJECT, "", ""));
        queries.add(new Query("6", PROJECT, "", ""));
        queries.add(new Query("7", PROJECT, "", ""));
        queries.add(new Query("8", PROJECT, "", ""));
        queries.add(new Query("9", PROJECT, "", ""));
        queries.add(new Query("10", PROJECT, "", ""));

        return new QueryService.QueryRecord(queries);
    }

    @Test
    public void testDownloadQueryResult() throws Exception {
        Mockito.doNothing().when(nQueryController).downloadQueryResult(Mockito.anyString(),
                Mockito.any(SQLRequest.class), Mockito.any(HttpServletResponse.class));

        final SQLRequest sqlRequest = mockPrepareSqlRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/format/{format}", "xml")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(SQLRequest.class),
                Mockito.any(HttpServletResponse.class));
    }

    @Test
    public void testGetMetadata() throws Exception {
        final MetaRequest metaRequest = new MetaRequest();
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/tables_and_columns")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(metaRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getMetadata(Mockito.any(MetaRequest.class));
    }

    @Test
    public void testErrorMsg() {
        String errorMsg = "Error while executing SQL \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\": From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'";
        Assert.assertEquals(
                "From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'\n"
                        + "while executing SQL: \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\"",
                QueryUtil.makeErrorMsgUserFriendly(errorMsg));
    }

    @Test
    public void testQueryStatistics() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/statistics").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", "0").param("end_time", "999999999999")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getQueryStatistics("default", 0, 999999999999L);
    }

    @Test
    public void testGetQueryCount() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/statistics/count")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("start_time", "0")
                .param("end_time", "999999999999").param("dimension", "model")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getQueryCount("default", 0, 999999999999L, "model");
    }

    @Test
    public void testGetQueryDuration() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/statistics/duration")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("start_time", "0")
                .param("end_time", "999999999999").param("dimension", "model")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getAvgDuration("default", 0, 999999999999L, "model");
    }

    @Test
    public void testQueryStatisticsEngine() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/overview").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start_time", "0").param("end_time", "999999999999")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).queryStatisticsByEngine("default", 0, 999999999999L);
    }

    private List<QueryHistory> mockedQueryHistories() {
        final List<QueryHistory> queries = Lists.newArrayList();
        QueryHistory queryHistory1 = new QueryHistory("sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queries.add(queryHistory1);
        QueryHistory queryHistory2 = new QueryHistory("sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queries.add(queryHistory2);
        QueryHistory queryHistory3 = new QueryHistory("sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queries.add(queryHistory3);

        return queries;
    }

    @Test
    public void testGetQueryHistories() throws Exception {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        request.setStartTimeFrom("0");
        request.setStartTimeTo("1000");
        request.setLatencyFrom("0");
        request.setLatencyTo("10");
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("query_histories", mockedQueryHistories());
        data.put("size", 6);
        Mockito.when(queryHistoryService.getQueryHistories(request, 3, 2)).thenReturn(data);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("startTimeFrom", "0").param("startTimeTo", "1000")
                .param("latencyFrom", "0").param("latencyTo", "10").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(6))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[0].sql_text").value("sql1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[1].sql_text").value("sql2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[2].sql_text").value("sql3"));

        Mockito.verify(nQueryController).getQueryHistories(PROJECT, request.getStartTimeFrom(),
                request.getStartTimeTo(), request.getLatencyFrom(), request.getLatencyTo(), null, null, null, 2, 3);

        // check args
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("startTimeFrom", "0")
                .param("latencyFrom", "0").param("latencyTo", "10").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().is(500));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("startTimeFrom", "0").param("startTimeTo", "1000")
                .param("latencyFrom", "0").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().is(500));
    }
}
