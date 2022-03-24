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



package io.kyligence.kap.rest.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.rest.service.QueryCacheManager;
import io.kyligence.kap.rest.service.QueryHistoryService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.model.Query;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.request.SaveSqlRequest;
import org.apache.kylin.rest.service.QueryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
import redis.clients.jedis.exceptions.JedisException;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.hamcrest.CoreMatchers.containsString;

/**
 * @author xduo
 */
public class NQueryControllerTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private QueryService kapQueryService;

    @Mock
    private QueryHistoryService queryHistoryService;

    @Mock
    private QueryCacheManager queryCacheManager;

    @InjectMocks
    private NQueryController nQueryController = Mockito.spy(new NQueryController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nQueryController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        super.createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    private PrepareSqlRequest mockPrepareSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        sqlRequest.setProject(PROJECT);
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
                .header("User-Agent", "Chrome/89.0.4389.82 Safari/537.36")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).query((PrepareSqlRequest) Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testStopQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/1").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockPrepareSqlRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).stopQuery(Mockito.any());
    }

    @Test
    public void testClearCache() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/cache").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).clearCache(Mockito.any());
    }

    @Test
    public void testClearCacheNotAdmin() throws Exception {
        try {
            final Authentication authentication2 = new TestingAuthenticationToken("MODELER", "MODELER", Constant.ROLE_MODELER);
            SecurityContextHolder.getContext().setAuthentication(authentication2);
            mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/cache").contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());

            Mockito.verify(nQueryController).clearCache(Mockito.any());
        } finally {
            SecurityContextHolder.getContext().setAuthentication(authentication);
        }
    }

    @Test
    public void testClearCacheThrow() throws Exception {
        Mockito.doThrow(new JedisException("for test")).when(queryCacheManager).clearProjectCache(Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/cache").contentType(MediaType.APPLICATION_JSON)
                .param("project", Mockito.anyString())
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError());

        Mockito.verify(nQueryController).clearCache(Mockito.anyString());
    }

    @Test
    public void testRecoverCache() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/cache/recovery").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).recoverCache();
    }

    @Test
    public void testRecoverCacheNotAdmin() throws Exception {
        try {
            final Authentication authentication2 = new TestingAuthenticationToken("MODELER", "MODELER", Constant.ROLE_MODELER);
            SecurityContextHolder.getContext().setAuthentication(authentication2);
            mockMvc.perform(MockMvcRequestBuilders.post("/api/query/cache/recovery").contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                    .andExpect(MockMvcResultMatchers.status().is5xxServerError());

            Mockito.verify(nQueryController).recoverCache();
        } finally {
            SecurityContextHolder.getContext().setAuthentication(authentication);
        }
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequest = mockPrepareSqlRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).prepareQuery((PrepareSqlRequest) Mockito.any());
    }

    @Test
    public void testSaveQuery() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("query_01");
        sqlRequest.setSql("select * from test_kylin_fact");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testSaveQueryWithEmptyQueryName() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content().string(containsString("Query name should not be empty.")));

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testSaveQueryWithInvalidQueryName() throws Exception {
        final SaveSqlRequest sqlRequest = mockSaveSqlRequest("query%");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.content()
                        .string(containsString("Query name should only contain alphanumerics and underscores.")));

        Mockito.verify(nQueryController).saveQuery(Mockito.any(SaveSqlRequest.class));
    }

    @Test
    public void testRemoveSavedQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/saved_queries//{id}", "1").param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).removeSavedQuery("1", "default");
    }

    @Test
    public void testGetSavedQueries() throws Exception {
        Mockito.when(kapQueryService.getSavedQueries("ADMIN", "default")).thenReturn(mockSavedQueries());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/saved_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.total_size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[0].name").value(7))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[1].name").value(8))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[2].name").value(9));

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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).downloadQueryResult(Mockito.anyString(), Mockito.any(SQLRequest.class),
                Mockito.any(HttpServletResponse.class));
    }

    @Test
    public void testGetMetadata() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/tables_and_columns").param("project", "default")
                .param("cube", "model1")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getMetadata("default", "model1");
    }

    @Test
    public void testGetMetadataWhenModelIsNull() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/tables_and_columns").param("project", "default")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getMetadata("default", null);
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getQueryStatistics("default", 0, 999999999999L);
    }

    @Test
    public void testGetQueryCount() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/query/statistics/count").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").param("start_time", "0").param("end_time", "999999999999")
                        .param("dimension", "model").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getQueryCount("default", 0, 999999999999L, "model");
    }

    @Test
    public void testGetQueryDuration() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/query/statistics/duration").contentType(MediaType.APPLICATION_JSON)
                        .param("project", "default").param("start_time", "0").param("end_time", "999999999999")
                        .param("dimension", "model").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryController).getAvgDuration("default", 0, 999999999999L, "model");
    }

    private List<QueryHistory> mockedQueryHistories() {
        final List<QueryHistory> queries = Lists.newArrayList();
        QueryHistory queryHistory1 = new QueryHistory("sql1");
        queryHistory1.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queries.add(queryHistory1);
        QueryHistory queryHistory2 = new QueryHistory("sql2");
        queryHistory2.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory2.setQueryHistoryInfo(null);
        queryHistory2.setNativeQueryRealizations(null);
        queries.add(queryHistory2);
        QueryHistory queryHistory3 = new QueryHistory("sql3");
        queryHistory3.setQueryStatus(QueryHistory.QUERY_HISTORY_SUCCEEDED);
        queryHistory3.setQueryHistoryInfo(new QueryHistoryInfo());
        List<NativeQueryRealization> realizations = Lists.newArrayList();
        realizations.add(new NativeQueryRealization());
        queryHistory3.setNativeQueryRealizations(realizations);
        queries.add(queryHistory3);

        return queries;
    }

    private List<String> mockQueryHistorySubmitters() {
        final List<String> submitters = Lists.newArrayList();
        submitters.add("ADMIN");
        submitters.add("USER1");
        submitters.add("USER2");
        submitters.add("USER3");
        submitters.add("USER4");
        return submitters;
    }

    @Test
    public void testGetQueryHistories() throws Exception {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        request.setStartTimeFrom("0");
        request.setStartTimeTo("1000");
        request.setLatencyFrom("0");
        request.setLatencyTo("10");
        request.setSubmitterExactlyMatch(true);
        request.setQueryStatus(Arrays.asList("FAILED"));
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("query_histories", mockedQueryHistories());
        data.put("size", 6);
        Mockito.when(queryHistoryService.getQueryHistories(request, 3, 2)).thenReturn(data);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("start_time_from", "0").param("start_time_to", "1000")
                .param("latency_from", "0").param("latency_to", "10").param("query_status", "FAILED")
                .param("offset", "2").param("limit", "3").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(6))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[0].sql_text").value("sql1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[1].sql_text").value("sql2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[2].sql_text").value("sql3"));

        Mockito.verify(nQueryController).getQueryHistories(PROJECT, request.getStartTimeFrom(),
                request.getStartTimeTo(), request.getLatencyFrom(), request.getLatencyTo(), request.getQueryStatus(),
                null, null, null, 2, 3, null);

        // check args
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("start_time_from", "0").param("latency_from", "0")
                .param("latency_to", "10").param("query_status", "FAILED").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is(400));

        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/history_queries").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("start_time_from", "0").param("start_time_to", "1000")
                .param("latency_from", "0").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is(400));
    }

    //KE-22624 public query history api
    @Test
    public void testGetQueryHistoriesAPI() throws Exception {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        request.setStartTimeFrom("0");
        request.setStartTimeTo("1000");
        HashMap<String, Object> data = Maps.newHashMap();
        data.put("query_histories", mockedQueryHistories());
        data.put("size", 6);
        Mockito.when(queryHistoryService.getQueryHistories(request, 3, 2)).thenReturn(data);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/query_histories").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("start_time_from", "0").param("start_time_to", "1000")
                .param("page_offset", "2").param("page_size", "3").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(6))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[0].sql_text").value("sql1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[1].sql_text").value("sql2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.query_histories[2].sql_text").value("sql3"));

        Mockito.verify(nQueryController).getQueryHistories(PROJECT, request.getStartTimeFrom(), request.getStartTimeTo(), 2, 3);

        HashMap<String, Object> dataWithNullHistories = Maps.newHashMap();
        dataWithNullHistories.put("query_histories", null);
        dataWithNullHistories.put("size", 6);
        Mockito.when(queryHistoryService.getQueryHistories(request, 6, 2)).thenReturn(dataWithNullHistories);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/query_histories").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("start_time_from", "0").param("start_time_to", "1000")
                .param("page_offset", "2").param("page_size", "6").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(6));
    }

    @Test
    public void testGetQueryHistorySubmitters() throws Exception {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        request.setFilterSubmitter(Lists.newArrayList("USER"));
        final List<String> submitters = Lists.newArrayList();
        submitters.add("USER1");
        submitters.add("USER2");
        submitters.add("USER3");

        Mockito.when(queryHistoryService.getQueryHistoryUsernames(request, 3)).thenReturn(submitters);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/query_history_submitters").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("submitter", "USER").param("page_size", "3")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0]").value("USER1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1]").value("USER2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[2]").value("USER3"));
        Mockito.verify(nQueryController).getQueryHistorySubmitters(PROJECT, request.getFilterSubmitter(), 3);
    }

    @Test
    public void testGetQueryHistoryModels() throws Exception {
        QueryHistoryRequest request = new QueryHistoryRequest();
        request.setProject(PROJECT);
        request.setFilterModelName("MODEL");
        final List<String> models = Lists.newArrayList();
        models.add("MODEL1");
        models.add("MODEL2");

        Mockito.when(queryHistoryService.getQueryHistoryModels(request, 3)).thenReturn(models);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/query_history_models").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("model_name", "MODEL").param("page_size", "3")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.length()").value(2))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[0]").value("MODEL1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data[1]").value("MODEL2"));
        Mockito.verify(nQueryController).getQueryHistoryModels(PROJECT, request.getFilterModelName(), 3);
    }
}