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
import io.kyligence.kap.metadata.favorite.FavoriteQueryResponse;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.request.AppendBlacklistSqlRequest;
import io.kyligence.kap.rest.request.WhitelistUpdateRequest;
import io.kyligence.kap.rest.response.FavoriteRuleResponse;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

public class FavoriteQueryControllerTest {

    private final String PROJECT = "default";
    private final String QUERY_HISTORY_1 = "query_history_1";

    private MockMvc mockMvc;

    @Mock
    private FavoriteQueryService favoriteQueryService;
    @Mock
    private FavoriteRuleService favoriteRuleService;
    @InjectMocks
    private FavoriteQueryController favoriteQueryController = Mockito.spy(new FavoriteQueryController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(favoriteQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

    }

    @Test
    public void testManualFavorite() throws Exception {
        FavoriteRequest request = new FavoriteRequest(PROJECT, QUERY_HISTORY_1, "test_sql_pattern", System.currentTimeMillis(), QueryHistory.QUERY_HISTORY_SUCCEEDED);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).manualFavorite(Mockito.any(request.getClass()));
    }

    private List<FavoriteQueryResponse> mockedFavoriteQueries() {
        List<FavoriteQueryResponse> mockedFavoriteQueries = Lists.newArrayList();
        mockedFavoriteQueries.add(new FavoriteQueryResponse("sql1", "sql1".hashCode(), PROJECT));
        mockedFavoriteQueries.add(new FavoriteQueryResponse("sql2", "sql1".hashCode(), PROJECT));
        mockedFavoriteQueries.add(new FavoriteQueryResponse("sql3", "sql1".hashCode(), PROJECT));

        return mockedFavoriteQueries;
    }

    @Test
    public void testListAllFavorite() throws Exception {
        Mockito.when(favoriteQueryService.getFavoriteQueriesByPage(PROJECT, 10, 0)).thenReturn(mockedFavoriteQueries());
        Mockito.when(favoriteQueryService.getFavoriteQuerySize(PROJECT)).thenReturn(10);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.favorite_queries.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.favorite_queries[0].sql_pattern").value("sql1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.favorite_queries[1].sql_pattern").value("sql2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.favorite_queries[2].sql_pattern").value("sql3"));

        Mockito.verify(favoriteQueryController, Mockito.only()).listFavoriteQuery(PROJECT, 0, 10);
    }

    @Test
    public void testGetAccelerateTips() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/threshold")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getAccelerateTips(PROJECT);
    }

    @Test
    public void testAcceptAccelerate() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/accept")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .param("accelerateSize", "20")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(favoriteQueryController, Mockito.only()).acceptAccelerate(PROJECT, 20);
    }

    @Test
    public void testIgnoreAccelerate() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/ignore/{project}", PROJECT)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).ignoreAccelerate(PROJECT);
    }

    @Test
    public void testGetFrequencyRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/frequency")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getFrequencyRule(PROJECT);
    }

    @Test
    public void testGetSubmitterRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/submitter")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getSubmitterRule(PROJECT);
    }

    @Test
    public void testGetDurationRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/duration")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getDurationRule(PROJECT);
    }

    @Test
    public void testUpdateFrequencyRule() throws Exception {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/frequency")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).updateFrequencyRule(Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateSubmitterRule() throws Exception {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/submitter")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).updateSubmitterRule(Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateDurationRule() throws Exception {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/duration")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).updateDurationRule(Mockito.any(request.getClass()));
    }

    @Test
    public void testGetFavoriteRuleOverallImpact() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/impact")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getRulesOverallImpact(PROJECT);
    }

    @Test
    public void testUpdateWhitelist() throws Exception {
        WhitelistUpdateRequest request = new WhitelistUpdateRequest();
        request.setId("test_id");
        request.setProject(PROJECT);
        request.setSql("select * from test_table");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/whitelist")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).updateWhitelist(Mockito.any(request.getClass()));
    }

    @Test
    public void testRemoveWhitelistSqls() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries/whitelist")
                .contentType(MediaType.APPLICATION_JSON)
                .param("id", "test_id")
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).removeWhitelistSql("test_id", PROJECT);
    }

    private List<FavoriteRuleResponse> getMockedResponse() {
        List<FavoriteRuleResponse> result = Lists.newArrayList();
        result.add(new FavoriteRuleResponse("id1", "sql1"));
        result.add(new FavoriteRuleResponse("id2", "sql2"));
        result.add(new FavoriteRuleResponse("id3", "sql3"));
        result.add(new FavoriteRuleResponse("id4", "sql4"));
        result.add(new FavoriteRuleResponse("id5", "sql5"));
        result.add(new FavoriteRuleResponse("id6", "sql6"));
        result.add(new FavoriteRuleResponse("id7", "sql7"));
        result.add(new FavoriteRuleResponse("id8", "sql8"));
        result.add(new FavoriteRuleResponse("id9", "sql9"));
        result.add(new FavoriteRuleResponse("id10", "sql10"));

        return result;
    }

    @Test
    public void testGetWhiteList() throws Exception {
        Mockito.when(favoriteRuleService.getWhitelist(PROJECT)).thenReturn(getMockedResponse());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/whitelist")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[0].id").value("id7"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[1].id").value("id8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[2].id").value("id9"));

        Mockito.verify(favoriteQueryController).getWhitelist(PROJECT, 2, 3);
    }

    @Test
    public void testLoadSqlsToWhiteList() throws Exception {
        MockMultipartFile file = new MockMultipartFile("file", "sqls.sql", "text/plain", new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls.sql")));
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/query/favorite_queries/whitelist")
                .file(file)
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).loadSqlsToWhitelist(file, PROJECT);
    }

    @Test
    public void testAppendSqlToBlacklist() throws Exception {
        AppendBlacklistSqlRequest request = new AppendBlacklistSqlRequest();
        request.setProject(PROJECT);
        request.setSql("test_sql");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries/blacklist")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).appendSqlToBlacklist(Mockito.any(request.getClass()));
    }

    @Test
    public void testGetBlacklist() throws Exception {
        Mockito.doReturn(getMockedResponse()).when(favoriteRuleService).getBlacklistSqls(PROJECT);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/blacklist")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("offset", "2").param("limit", "3")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[0].id").value("id7"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[1].id").value("id8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.sqls[2].id").value("id9"));

        Mockito.verify(favoriteQueryController).getBlacklist(PROJECT, 2, 3);
    }

    @Test
    public void testRemoveBlacklist() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries/blacklist")
                .contentType(MediaType.APPLICATION_JSON)
                .param("id", "test_id")
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).removeBlacklistSql("test_id", PROJECT);
    }
}
