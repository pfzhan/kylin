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
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryStatusEnum;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryManager;
import org.apache.kylin.common.util.JsonUtil;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

public class FavoriteQueryControllerTest extends AbstractMVCIntegrationTestCase {

    private final String PROJECT = "default";

    private final String QUERY_UNFAVORITED_1 = "da0c9cad-35c1-4f4b-8c10-669248842c2f";
    private final String QUERY_UNFAVORITED_2 = "ca0c9cad-35c1-4f4b-8c10-669248842c2f";
    private final String QUERY_FAVORITED = "ba0c9cad-35c1-4f4b-8c10-669248842c2b";
    private final String FAVORITE_QUERY = "bd3285c9-55e3-4f2d-a12c-742a8d631195";

    @Test
    public void testFavorite() throws Exception {
        QueryHistoryManager queryHistoryManager = QueryHistoryManager.getInstance(getTestConfig(), PROJECT);
        FavoriteQueryManager favoriteQueryManager = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT);

        // assert query history before
        final QueryHistory actualQueryHistoryBefore1 = queryHistoryManager.findQueryHistory(QUERY_UNFAVORITED_1);
        Assert.assertThat(actualQueryHistoryBefore1, CoreMatchers.notNullValue());
        Assert.assertFalse(actualQueryHistoryBefore1.isFavorite());

        final QueryHistory actualQueryHistoryBefore2 = queryHistoryManager.findQueryHistory(QUERY_UNFAVORITED_2);
        Assert.assertThat(actualQueryHistoryBefore2, CoreMatchers.notNullValue());
        Assert.assertFalse(actualQueryHistoryBefore2.isFavorite());

        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .content(JsonUtil.writeValueAsString(Lists.newArrayList(QUERY_UNFAVORITED_1, QUERY_UNFAVORITED_2)))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print()).andReturn();

        // assert favorite query
        final String actualFavoriteQueryName1 = JsonPath.compile("$.data[0].uuid")
                .<String> read(mvcResult.getResponse().getContentAsString());
        final FavoriteQuery actualFavoriteQuery1 = favoriteQueryManager.get(actualFavoriteQueryName1);
        Assert.assertThat(actualFavoriteQuery1, CoreMatchers.notNullValue());
        Assert.assertEquals(FavoriteQueryStatusEnum.WAITING, actualFavoriteQuery1.getStatus());

        final String actualFavoriteQueryName2 = JsonPath.compile("$.data[1].uuid")
                .<String> read(mvcResult.getResponse().getContentAsString());
        final FavoriteQuery actualFavoriteQuery2 = favoriteQueryManager.get(actualFavoriteQueryName2);
        Assert.assertThat(actualFavoriteQuery2, CoreMatchers.notNullValue());
        Assert.assertEquals(FavoriteQueryStatusEnum.WAITING, actualFavoriteQuery2.getStatus());

        // assert query
        final QueryHistory actualQueryHistoryAfter1 = queryHistoryManager.findQueryHistory(QUERY_UNFAVORITED_1);
        Assert.assertEquals(actualFavoriteQueryName2, actualQueryHistoryAfter1.getFavorite());

        final QueryHistory actualQueryHistoryAfter2 = queryHistoryManager.findQueryHistory(QUERY_UNFAVORITED_2);
        Assert.assertEquals(actualFavoriteQueryName1, actualQueryHistoryAfter2.getFavorite());
    }

    @Test
    public void testUnFavorite() throws Exception {
        getTestConfig().setProperty("kylin.server.mode", "query");
        // assert history query before
        final QueryHistory actualQueryHistoryBefore = QueryHistoryManager.getInstance(getTestConfig(), PROJECT)
                .findQueryHistory(QUERY_FAVORITED);
        Assert.assertThat(actualQueryHistoryBefore, CoreMatchers.notNullValue());
        Assert.assertTrue(actualQueryHistoryBefore.isFavorite());
        Assert.assertEquals(FAVORITE_QUERY, actualQueryHistoryBefore.getFavorite());

        // assert favorite query before
        final FavoriteQuery actualFavoriteQueryBefore = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT)
                .get(FAVORITE_QUERY);
        Assert.assertThat(actualFavoriteQueryBefore, CoreMatchers.notNullValue());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .content(JsonUtil.writeValueAsString(Lists.newArrayList(FAVORITE_QUERY)))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andDo(MockMvcResultHandlers.print());

        // assert query history after
        final QueryHistory actualQueryHistoryAfter = QueryHistoryManager.getInstance(getTestConfig(), PROJECT)
                .findQueryHistory(QUERY_FAVORITED);
        Assert.assertThat(actualQueryHistoryAfter, CoreMatchers.notNullValue());
        Assert.assertFalse(actualQueryHistoryAfter.isFavorite());

        // assert favorite query after
        final FavoriteQuery actualFavoriteQueryAfter = FavoriteQueryManager.getInstance(getTestConfig(), PROJECT)
                .get(FAVORITE_QUERY);
        Assert.assertThat(actualFavoriteQueryAfter, CoreMatchers.nullValue());
        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    @Test
    public void testListAllFavorites() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(1))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.favorite_queries[0].uuid").value(FAVORITE_QUERY));
    }

    @Test
    public void testGetFilterRules() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.size").value(1))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.rules[0].uuid").value("30a73dc4-b1b6-4744-a598-5735f52c249b"));
    }
}
