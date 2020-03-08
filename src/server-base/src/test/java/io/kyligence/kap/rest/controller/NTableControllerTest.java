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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SamplingRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.PartitionKeyRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableSamplingService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;

public class NTableControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private TableService tableService;

    @Mock
    private ModelService modelService;

    @Mock
    private TableExtService tableExtService;

    @Mock
    private TableSamplingService tableSamplingService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NTableController nTableController = Mockito.spy(new NTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nTableController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private PartitionKeyRequest mockFactTableRequest() {
        final PartitionKeyRequest partitionKeyRequest = new PartitionKeyRequest();
        partitionKeyRequest.setProject("default");
        partitionKeyRequest.setTable("table1");
        partitionKeyRequest.setColumn("CAL_DT");
        return partitionKeyRequest;
    }

    private TableLoadRequest mockLoadTableRequest() {
        final TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setProject("default");
        tableLoadRequest.setDataSourceType(11);
        String[] tables = { "table1", "DEFAULT.TEST_ACCOUNT" };
        String[] dbs = { "db1", "default" };
        tableLoadRequest.setTables(tables);
        tableLoadRequest.setDatabases(dbs);
        return tableLoadRequest;
    }

    @Test
    public void testGetTableDesc() throws Exception {
        Mockito.when(tableService.getTableDesc("default", false, "", "DEFAULT", true)) //
                .thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("withExt", "false") //
                .param("project", "default") //
                .param("table", "") //
                .param("database", "DEFAULT") //
                .param("pageOffset", "0") //
                .param("pageSize", "10") //
                .param("isFuzzy", "true") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "", "DEFAULT", true, 0, 10);
    }

    @Test
    public void testGetTableDescWithName() throws Exception {
        Mockito.when(tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", true))
                .thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("withExt", "false") //
                .param("project", "default") //
                .param("table", "TEST_KYLIN_FACT") //
                .param("database", "DEFAULT") //
                .param("pageOffset", "0") //
                .param("pageSize", "10") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "TEST_KYLIN_FACT", "DEFAULT", true, 0, 10);
    }

    @Test
    public void testShowDatabases() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("ddd");
        list.add("fff");
        Mockito.when(tableService.getSourceDbNames("default")).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/databases") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("datasourceType", "11") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showDatabases("default");
    }

    @Test
    public void testShowTables() throws Exception {
        List<TableNameResponse> list = new ArrayList<>();
        list.add(new TableNameResponse());
        list.add(new TableNameResponse());
        Mockito.when(tableService.getTableNameResponses("default", "db1", "")).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/names") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("data_source_type", "11") //
                .param("database", "db1") //
                .param("page_offset", "0") //
                .param("page_size", "10") //
                .param("table", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showTables("default", 11, "", 0, 10, "db1");
    }

    @Test
    public void testSetPartitionKey() throws Exception {
        final PartitionKeyRequest partitionKeyRequest = mockFactTableRequest();
        Mockito.doNothing().when(tableService).setPartitionKey(partitionKeyRequest.getProject(),
                partitionKeyRequest.getTable(), partitionKeyRequest.getColumn(),
                partitionKeyRequest.getPartitionColumnFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/partition_key") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(partitionKeyRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPartitionKey(Mockito.any(PartitionKeyRequest.class));
    }

    @Test
    public void testSetNoPartitionKey() throws Exception {
        final PartitionKeyRequest partitionKeyRequest = mockFactTableRequest();
        partitionKeyRequest.setColumn("");
        Mockito.doNothing().when(tableService).setPartitionKey(partitionKeyRequest.getProject(),
                partitionKeyRequest.getTable(), partitionKeyRequest.getColumn(),
                partitionKeyRequest.getPartitionColumnFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/partition_key") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(partitionKeyRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPartitionKey(Mockito.any(PartitionKeyRequest.class));
    }

    @Test
    public void testSetDateRangePass() throws Exception {
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setTable("DEFAULT.TEST_KYLIN_FACT");
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(dateRangeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));
    }

    @Test
    public void testgetPartitioinColumnFormat() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/partition_column_format") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .param("partition_column", "CAL_DT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getPartitioinColumnFormat("default", "DEFAULT.TEST_KYLIN_FACT", "CAL_DT");
    }

    @Test
    public void testGetLatestData() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/data_range/latest_data") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getLatestData("default", "DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void getGetBatchLoadTables() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/batch_load") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getBatchLoadTables("default");
    }

    @Test
    public void batchLoadTablesWithEmptyRequest() throws Exception {
        List<DateRangeRequest> requests = Lists.newArrayList();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/batch_load") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(requests)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).batchLoad(Mockito.anyList());
    }

    @Test
    public void batchLoadTables_DateRange_LessThan0_Exception() throws Exception {
        String errorMsg = "Start or end of range must be greater than 0!";
        DateRangeRequest request = new DateRangeRequest();
        request.setProject("default");
        request.setTable("DEFAULT.TEST_KYLIN_FACT");
        request.setStart("-1");
        request.setEnd("-1");
        List<DateRangeRequest> requests = Lists.newArrayList(request);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/batch_load") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(requests)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).batchLoad(Mockito.anyList());

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void batchLoadTables_DateRange_EndLessThanStart_Exception() throws Exception {
        String errorMsg = "End of range must be greater than start!";
        DateRangeRequest request = new DateRangeRequest();
        request.setProject("default");
        request.setTable("DEFAULT.TEST_KYLIN_FACT");
        request.setStart("100");
        request.setEnd("1");
        List<DateRangeRequest> requests = Lists.newArrayList(request);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/batch_load") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(requests)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).batchLoad(Mockito.anyList());

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testSetDateRang_lessThan0_exception() throws Exception {
        String errorMsg = "Start of range must be greater than 0!";
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("-1");
        dateRangeRequest.setTable("TEST_KYLIN_FACT");
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(dateRangeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testSetDateRang_EndLessThanStart_exception() throws Exception {
        String errorMsg = "End of range must be greater than start!";
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("100");
        dateRangeRequest.setEnd("1");
        dateRangeRequest.setTable("TEST_KYLIN_FACT");
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(dateRangeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testSetTop() throws Exception {
        final TopTableRequest topTableRequest = mockTopTableRequest();
        Mockito.doNothing().when(tableService).setTop(topTableRequest.getTable(), topTableRequest.getProject(),
                topTableRequest.isTop());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/top") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(topTableRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setTableTop(Mockito.any(TopTableRequest.class));

    }

    private TopTableRequest mockTopTableRequest() {
        TopTableRequest topTableRequest = new TopTableRequest();
        topTableRequest.setTop(true);
        topTableRequest.setTable("table1");
        topTableRequest.setProject("default");
        return topTableRequest;
    }

    private DateRangeRequest mockDateRangeRequest() {
        DateRangeRequest request = new DateRangeRequest();
        request.setStart("0");
        request.setEnd("" + Long.MAX_VALUE);
        request.setProject("default");
        request.setTable("TEST_KYLIN_FACT");
        return request;
    }

    private void initMockito(LoadTableResponse loadTableResponse, TableLoadRequest tableLoadRequest) throws Exception {
        String[] succTables = { "DEFAULT.TEST_ACCOUNT" };
        String[] succDbs = { "DEFAULT" };
        Mockito.when(tableService.classifyDbTables("default", tableLoadRequest.getTables()))
                .thenReturn(new Pair<>(succTables, Sets.newHashSet("table1")));
        Mockito.when(tableService.classifyDbTables("default", tableLoadRequest.getDatabases()))
                .thenReturn(new Pair<>(succDbs, Sets.newHashSet("db1")));
        Mockito.when(tableExtService.loadTables(succTables, "default")).thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadTablesByDatabase("default", succDbs)).thenReturn(loadTableResponse);
    }

    @Test
    public void testLoadTables() throws Exception {
        Set<String> loaded = Sets.newHashSet("table1");
        Set<String> failed = Sets.newHashSet("table2");
        Set<String> loading = Sets.newHashSet("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        initMockito(loadTableResponse, tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testLoadTablesException() throws Exception {
        String errorMsg = "You should select at least one table or database to load!!";
        Set<String> loaded = Sets.newHashSet("table1");
        Set<String> failed = Sets.newHashSet("table2");
        Set<String> loading = Sets.newHashSet("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setTables(null);
        tableLoadRequest.setDatabases(null);
        Mockito.when(tableExtService.loadTables(tableLoadRequest.getTables(), "default")).thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadTablesByDatabase("default", tableLoadRequest.getDatabases()))
                .thenReturn(loadTableResponse);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));

        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testUnloadTable() throws Exception {
        Mockito.doReturn(false).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doNothing().when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                .param("project", "default").accept(MediaType.parseMediaType(APPLICATION_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE", false);
    }

    @Test
    public void testUnloadTableException() throws Exception {
        Mockito.doReturn(true).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doNothing().when(tableService).unloadTable("default", "DEFAULT.TABLE", false);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{database}/{table}", "DEFAULT", "TABLE")
                .param("project", "default").accept(MediaType.parseMediaType(APPLICATION_JSON)));
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE", false);
    }

    @Test
    public void testGetTablesAndColumns() throws Exception {
        Mockito.doReturn(mockTableAndColumns()).when(tableService).getTableAndColumns("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/simple_table") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("pageSize", "10") //
                .param("pageOffset", "0") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getTablesAndColomns("default", 0, 10);
    }

    @Test
    public void testGetAutoMergeConfig() throws Exception {
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("model", "") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getAutoMergeConfig("", "DEFAULT.TEST_KYLIN_FACT", "default");
    }

    @Test
    public void testGetAutoMergeConfigException() throws Exception {
        String errorMsg = "model name or table name must be specified!";
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByModel("default", "");
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("model", "") //
                .param("table", "") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).getAutoMergeConfig("", "", "default");
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testGetRefreshDateRange() throws Exception {
        Mockito.doNothing().when(tableService).checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0",
                "100");
        Mockito.doReturn(null).when(modelService).getRefreshAffectedSegmentsResponse("default",
                "DEFAULT.TEST_KYLIN_FACT", "0", "100");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/affected_data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("start", "0") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .param("end", "100") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getRefreshAffectedDateRange("default", "DEFAULT.TEST_KYLIN_FACT", "0", "100");
    }

    @Test
    public void testGetPushdownMode() throws Exception {
        Mockito.doReturn(true).when(tableService).getPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pushdown_mode") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .param("table", "DEFAULT.TEST_KYLIN_FACT") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getPushdownMode("default", "DEFAULT.TEST_KYLIN_FACT");
    }

    @Test
    public void testRefreshSegments() throws Exception {
        Mockito.doNothing().when(modelService).refreshSegments("default", "TEST_KYLIN_FACT", "0", "100", "0", "100");
        RefreshSegmentsRequest refreshSegmentsRequest = new RefreshSegmentsRequest();
        refreshSegmentsRequest.setProject("default");
        refreshSegmentsRequest.setRefreshStart("0");
        refreshSegmentsRequest.setRefreshEnd("100");
        refreshSegmentsRequest.setAffectedStart("0");
        refreshSegmentsRequest.setAffectedEnd("100");
        refreshSegmentsRequest.setTable("TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testUpdateAutoMergeConfigException() throws Exception {
        String errorMsg = "You should specify at least one autoMerge range!";
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setAutoMergeTimeRanges(null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testUpdateAutoMergeConfigException2() throws Exception {
        String errorMsg = "model name or table name must be specified!";
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setModel("");
        autoMergeRequest.setTable("");
        val mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config")
                        .contentType(MediaType.APPLICATION_JSON) //
                        .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                        .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testUpdateAutoMergeConfig() throws Exception {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        Mockito.doNothing().when(tableService).setAutoMergeConfigByTable("default", autoMergeRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(autoMergeRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
    }

    @Test
    public void testUpdatePushdownMode() throws Exception {
        PushDownModeRequest config = new PushDownModeRequest();
        config.setProject("default");
        config.setPushdownRangeLimited(true);
        config.setTable("DEFAULT.TEST_KYLIN_FACT");
        Mockito.doNothing().when(tableService).setPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT", true);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/pushdown_mode") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(config)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPushdownMode(Mockito.any(PushDownModeRequest.class));
    }

    @Test
    public void testGetLoadedDatabases() throws Exception {
        Mockito.doReturn(null).when(tableService).getLoadedDatabases("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/loaded_databases") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", "default") //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getLoadedDatabases("default");
    }

    @Test
    public void testSubmitSampling() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("default.test_kylin_fact");
        Mockito.doNothing().when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).submitSampling(Mockito.any(SamplingRequest.class));
    }

    @Test
    public void testLoadTablesWithSampling() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact", "default.test_account");
        Set<String> failed = Sets.newHashSet("default.test_country");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(20000);
        initMockito(loadTableResponse, tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testLoadTablesExceptionForSamplingRowsTooSmall() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(200);

        String errorMsg = "Sampling range should not be less than the max limit(10000 rows)!";
        initMockito(loadTableResponse, tableLoadRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testLoadTablesExceptionForSamplingRowsTooLarge() throws Exception {
        Set<String> loaded = Sets.newHashSet("default.test_kylin_fact");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(30_000_000);

        String errorMsg = "Sampling range should not exceed the max limit(20000000 rows)!";
        initMockito(loadTableResponse, tableLoadRequest);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();

        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testSubmitSamplingFailedForNoTable() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);

        String errorMsg = "Please input at least one table(database.table) for sampling!";
        Mockito.doNothing().when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows());
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testSubmitSamplingFailedForIllegalTableName() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("test_kylin_fact");

        String errorMsg = "Illegal table name 'test_kylin_fact', please input a qualified table name as database.table!";
        Mockito.doNothing().when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows());
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nTableController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    private List<TablesAndColumnsResponse> mockTableAndColumns() {
        List<TablesAndColumnsResponse> result = new ArrayList<>();
        result.add(new TablesAndColumnsResponse());
        return result;
    }

    private AutoMergeRequest mockAutoMergeRequest() {
        AutoMergeRequest autoMergeRequest = new AutoMergeRequest();
        autoMergeRequest.setProject("default");
        autoMergeRequest.setTable("DEFAULT.TEST_KYLIN_FACT");
        autoMergeRequest.setAutoMergeEnabled(true);
        autoMergeRequest.setAutoMergeTimeRanges(new String[] { "MINUTE" });
        autoMergeRequest.setVolatileRangeEnabled(true);
        autoMergeRequest.setVolatileRangeNumber(7);
        autoMergeRequest.setVolatileRangeType("MINUTE");
        return autoMergeRequest;
    }

    private List<TableDesc> mockTables() {
        final List<TableDesc> tableDescs = new ArrayList<>();
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName("table1");
        tableDescs.add(tableDesc);
        TableDesc tableDesc2 = new TableDesc();
        tableDesc2.setName("table2");
        tableDescs.add(tableDesc2);
        return tableDescs;
    }

    @Test
    public void testReloadHiveTablename() throws Exception {
        Mockito.when(tableService.loadHiveTableNameToCache(false)).thenReturn(null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/reload_hive_table_name") //
                .contentType(MediaType.APPLICATION_JSON) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).reloadHiveTablename(false);
    }

}
