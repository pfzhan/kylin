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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.junit.After;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.rest.request.SQLValidateRequest;
import io.kyligence.kap.rest.service.FavoriteQueryService;
import io.kyligence.kap.rest.service.FavoriteRuleService;
import io.kyligence.kap.rest.service.ProjectService;

public class FavoriteQueryControllerTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private FavoriteQueryService favoriteQueryService;
    @Mock
    private FavoriteRuleService favoriteRuleService;
    @InjectMocks
    private FavoriteQueryController favoriteQueryController = Mockito.spy(new FavoriteQueryController());

    @Mock
    private ProjectService projectService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(favoriteQueryController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateFavoriteQuery() throws Exception {
        FavoriteRequest request = new FavoriteRequest(PROJECT, Lists.newArrayList("test_sql_pattern"));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).createFavoriteQuery(Mockito.any());
    }

    private List<FavoriteQuery> mockedFavoriteQueries() {
        List<FavoriteQuery> mockedFavoriteQueries = Lists.newArrayList();
        mockedFavoriteQueries.add(new FavoriteQuery("sql1"));
        mockedFavoriteQueries.add(new FavoriteQuery("sql2"));
        mockedFavoriteQueries.add(new FavoriteQuery("sql3"));

        return mockedFavoriteQueries;
    }

    @Test
    public void testListAllFavorite() throws Exception {
        Mockito.when(favoriteQueryService.filterAndSortFavoriteQueries(PROJECT, "last_query_time", false, null))
                .thenReturn(mockedFavoriteQueries());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT).param("sort_by", "last_query_time")
                .param("reverse", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.total_size").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[0].sql_pattern").value("sql1"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[1].sql_pattern").value("sql2"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[2].sql_pattern").value("sql3"));

        Mockito.verify(favoriteQueryController).listFavoriteQuery(PROJECT, "last_query_time", false, null, 0, 10);
    }

    @Test
    public void testGetAccelerateTips() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/threshold")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).getAccelerateTips(PROJECT);
    }

    @Test
    public void testAcceptAccelerate_sqls() throws Exception {
        FavoriteRequest request = new FavoriteRequest();
        request.setProject(PROJECT);
        request.setSqls(Lists.newArrayList());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/accelerate")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(favoriteQueryController).acceptAccelerate(Mockito.any(request.getClass()));
    }

    @Test
    public void testAcceptAccelerate() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/accept")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT).param("accelerate_size", "20")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(favoriteQueryController).acceptAccelerate(PROJECT, 20);
    }

    @Test
    public void testIgnoreAccelerate() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/ignore/").param("project", PROJECT)
                .param("ignore_size", "20").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).ignoreAccelerate(PROJECT, 20);
    }

    @Test
    public void testGetFrequencyRule() throws Exception {
        mockMvc.perform(
                MockMvcRequestBuilders.get("/api/query/favorite_queries/rules").contentType(MediaType.APPLICATION_JSON)
                        .param("project", PROJECT).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).getFavoriteRules(PROJECT);
    }

    @Test
    public void testUpdateFrequencyRule() throws Exception {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setFreqEnable(false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).updateFavoriteRules(Mockito.any(request.getClass()));
    }

    @Test
    public void testUpdateFrequencyRuleWithWrongArgs() throws Exception {
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setFreqEnable(true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is(400));

        request.setFreqEnable(false);
        request.setDurationEnable(true);
        request.setMinDuration("0");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is(400));
    }

    @Test
    public void testGetAccelerateRatio() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/accelerate_ratio")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).getAccelerateRatio(PROJECT);
    }

    private List<FavoriteRule.SQLCondition> getMockedResponse() {
        List<FavoriteRule.SQLCondition> result = Lists.newArrayList();
        result.add(new FavoriteRule.SQLCondition("id1", "sql1"));
        result.add(new FavoriteRule.SQLCondition("id2", "sql2"));
        result.add(new FavoriteRule.SQLCondition("id3", "sql3"));
        result.add(new FavoriteRule.SQLCondition("id4", "sql4"));
        result.add(new FavoriteRule.SQLCondition("id5", "sql5"));
        result.add(new FavoriteRule.SQLCondition("id6", "sql6"));
        result.add(new FavoriteRule.SQLCondition("id7", "sql7"));
        result.add(new FavoriteRule.SQLCondition("id8", "sql8"));
        result.add(new FavoriteRule.SQLCondition("id9", "sql9"));
        result.add(new FavoriteRule.SQLCondition("id10", "sql10"));

        return result;
    }

    @Test
    public void testImportSqls() throws Exception {
        MockMultipartFile file = new MockMultipartFile("file", "sqls.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql")));
        MockMultipartFile file2 = new MockMultipartFile("file", "sqls.sql", "text/plain",
                new FileInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt")));
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/query/favorite_queries/sql_files").file(file)
                .file(file2).contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).importSqls(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDeleteFavoriteQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("uuids", "uuid")
                .param("block", "false").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).batchDeleteFQs("default", Lists.newArrayList("uuid"), false);
    }

    @Test
    public void testGetBlacklist() throws Exception {
        Mockito.doReturn(getMockedResponse()).when(favoriteRuleService).getBlacklistSqls(PROJECT, "");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/blacklist")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("sql", "")
                .param("offset", "2").param("limit", "3").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.total_size").value(10))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value.length()").value(3))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[0].id").value("id7"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[1].id").value("id8"))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.value[2].id").value("id9"));

        Mockito.verify(favoriteQueryController).getBlacklist(PROJECT, "", 2, 3);
    }

    @Test
    public void testRemoveBlacklist() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/query/favorite_queries/blacklist/{id}", "test_id")
                .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).removeBlacklistSql("test_id", PROJECT);
    }

    @Test
    public void testSqlValidate() throws Exception {
        SQLValidateRequest request = new SQLValidateRequest(PROJECT, "sql");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/sql_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(favoriteQueryController).sqlValidate(Mockito.any(SQLValidateRequest.class));
    }
}
