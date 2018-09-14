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
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.QueryHistoryService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.QueryFilterRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class FavoriteQueryControllerTest {

    private final String PROJECT = "default";
    private final String QUERY_HISTORY_1 = "query_history_1";
    private final String QUERY_HISTORY_2 = "query_history_2";
    private final String FAVORITE_QUERY_1 = "favorite_query_1";
    private final String FAVORITE_QUERY_2 = "favorite_query_2";

    private MockMvc mockMvc;

    @Mock
    private FavoriteQueryService favoriteQueryService;
    @Mock
    private QueryHistoryService queryHistoryService;
    @InjectMocks
    private FavoriteQueryController favoriteQueryController = Mockito.spy(new FavoriteQueryController());

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(favoriteQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

    }

    @Test
    public void testFavorite() throws Exception {
        FavoriteRequest request = new FavoriteRequest(PROJECT, Lists.newArrayList(QUERY_HISTORY_1, QUERY_HISTORY_2));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).favorite(Mockito.any(request.getClass()));
    }

    @Test
    public void testUnFavorite() throws Exception {
        FavoriteRequest request = new FavoriteRequest(PROJECT, Lists.newArrayList(FAVORITE_QUERY_1, FAVORITE_QUERY_2));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries/unfavorite")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).unFavorite(Mockito.any(request.getClass()));
    }

    @Test
    public void testListAllFavorite() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).listFavoriteQuery(PROJECT, 0, 10);
    }

    private QueryFilterRequest mockFilterQueryHistoryRequest() {
        final QueryFilterRequest sqlRequest = new QueryFilterRequest();
        sqlRequest.setProject(PROJECT);
        sqlRequest.setRule(null);
        return sqlRequest;
    }

    @Test
    public void testGetCandidates() throws Exception {
        QueryFilterRequest request = mockFilterQueryHistoryRequest();
        QueryHistory queryHistory = new QueryHistory();
        queryHistory.setQueryId("query-1");
        Mockito.when(favoriteQueryService.getCandidates(request.getProject())).thenReturn(Lists.newArrayList(queryHistory));
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/candidates")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(1))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.candidates[0].query_id").value("query-1"));

        Mockito.verify(favoriteQueryController).getCandidates(PROJECT, -1, -1, -1, -1, null, null, null, 0, 10);
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
    public void testGetFilterRules() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController, Mockito.only()).getFilterRule(PROJECT);
    }

    @Test
    public void testSaveFilterRule() throws Exception {
        QueryFilterRequest request = new QueryFilterRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).saveFilterRule(Mockito.any(QueryFilterRequest.class));
    }

    @Test
    public void testUpdateFilterRule() throws Exception {
        QueryFilterRequest request = new QueryFilterRequest();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).updateFilterRule(Mockito.any(QueryFilterRequest.class));
    }

    @Test
    public void testEnableRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/enable/{project}/{uuid}", PROJECT, "existing_rule_id")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).enableRule(PROJECT, "existing_rule_id");
    }

    @Test
    public void testDeleteRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries/rules/{project}/{uuid}", PROJECT, "rule_1")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).deleteFilterRule(PROJECT, "rule_1");
    }

    @Test
    public void testApplyAll() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/apply/{project}", PROJECT)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).applyAll(PROJECT);
    }

    @Test
    public void testSetAutoMarkFavorite() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/automatic/{project}", PROJECT)
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).setAutoMarkFavorite(PROJECT);
    }

    @Test
    public void testGetAutoMarkFavoriteConfig() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/automatic")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).getAutoMarkFavorite(PROJECT);
    }
}
