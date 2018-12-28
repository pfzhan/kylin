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

import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.rest.request.AutoMergeRequest;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.FactTableRequest;
import io.kyligence.kap.rest.request.PushDownModeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.request.TopTableRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import io.kyligence.kap.rest.response.TablesAndColumnsResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NTableControllerTest {

    private MockMvc mockMvc;

    @Mock
    private TableService tableService;

    @Mock
    private ModelService modelService;

    @Mock
    private TableExtService tableExtService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NTableController nTableController = Mockito.spy(new NTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nTableController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    private FactTableRequest mockFactTableRequest() {
        final FactTableRequest factTableRequest = new FactTableRequest();
        factTableRequest.setProject("default");
        factTableRequest.setTable("table1");
        factTableRequest.setColumn("CAL_DT");
        factTableRequest.setPartitionDateFormat("YYYY-mm-DD");
        factTableRequest.setFact(true);
        return factTableRequest;
    }

    private TableLoadRequest mockLoadTableRequest() {
        final TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setProject("default");
        tableLoadRequest.setDatasourceType(11);
        String[] tables = { "table1" };
        String[] dbs = { "db1" };
        tableLoadRequest.setTables(tables);
        tableLoadRequest.setDatabases(dbs);
        return tableLoadRequest;
    }

    @Test
    public void testGetTableDesc() throws Exception {
        Mockito.when(tableService.getTableDesc("default", false, "", "DEFAULT", true)).thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .param("withExt", "false").param("project", "default").param("table", "")
                .param("database", "DEFAULT").param("pageOffset", "0").param("pageSize", "10")
                .param("isFuzzy", "true")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "", "DEFAULT", true, 0, 10);
    }

    @Test
    public void testGetTableDescWithName() throws Exception {
        Mockito.when(tableService.getTableDesc("default", true, "TEST_KYLIN_FACT", "DEFAULT", true)).thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .param("withExt", "false").param("project", "default").param("table", "TEST_KYLIN_FACT")
                .param("database", "DEFAULT").param("pageOffset", "0").param("pageSize", "10")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "TEST_KYLIN_FACT", "DEFAULT", true, 0, 10);
    }

    @Test
    public void testShowDatabases() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("ddd");
        list.add("fff");
        Mockito.when(tableService.getSourceDbNames("default", 11)).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/databases").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("datasourceType", "11")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showDatabases("default", 11);
    }

    @Test
    public void testShowTables() throws Exception {
        List<TableNameResponse> list = new ArrayList<>();
        list.add(new TableNameResponse());
        list.add(new TableNameResponse());
        Mockito.when(tableService.getTableNameResponses("default", "db1", 11, "")).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/names").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("datasourceType", "11").param("database", "db1")
                .param("pageOffset", "0").param("pageSize", "10").param("table", "")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showTables("default", 11, "", 0, 10, "db1");
    }

    @Test
    public void testSetTableFact() throws Exception {
        final FactTableRequest factTableRequest = mockFactTableRequest();
        Mockito.doNothing().when(tableService).setFact(factTableRequest.getProject(), factTableRequest.getTable(),
                factTableRequest.isFact(), factTableRequest.getColumn(), factTableRequest.getPartitionDateFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/fact").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(factTableRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setTableFact(Mockito.any(FactTableRequest.class));
    }

    @Test
    public void testSetTableFactException() throws Exception {
        final FactTableRequest factTableRequest = mockFactTableRequest();
        factTableRequest.setColumn("");
        Mockito.doNothing().when(tableService).setFact(factTableRequest.getProject(), factTableRequest.getTable(),
                factTableRequest.isFact(), factTableRequest.getColumn(), factTableRequest.getPartitionDateFormat());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/fact").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(factTableRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).setTableFact(Mockito.any(FactTableRequest.class));
    }

    @Test
    public void testSetDateRangePass() throws Exception {
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(dateRangeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

    }


    @Test
    public void testSetDateRang_lessThan0_exception() throws Exception {
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("-1");
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(dateRangeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

    }

    @Test
    public void testSetDateRang_EndLessThanStart_exception() throws Exception {
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        dateRangeRequest.setStart("100");
        dateRangeRequest.setEnd("1");
        Mockito.doNothing().when(tableService).setDataRange("default", dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(dateRangeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

    }

    @Test
    public void testSetTop() throws Exception {
        final TopTableRequest topTableRequest = mockTopTableRequest();
        Mockito.doNothing().when(tableService).setTop(topTableRequest.getTable(), topTableRequest.getProject(), topTableRequest.isTop());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/top").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(topTableRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
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

    @Test
    public void testLoadTables() throws Exception {
        Set<String> loaded = new HashSet<String>();
        loaded.add("table1");
        Set<String> failed = new HashSet<String>();
        loaded.add("table2");
        Set<String> loading = new HashSet<String>();
        loaded.add("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        Mockito.when(tableExtService.loadTables(tableLoadRequest.getTables(), "default", 11)).thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadTablesByDatabase("default", tableLoadRequest.getDatabases(), 11)).thenReturn(loadTableResponse);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(tableLoadRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testLoadTablesException() throws Exception {
        Set<String> loaded = new HashSet<String>();
        loaded.add("table1");
        Set<String> failed = new HashSet<String>();
        loaded.add("table2");
        Set<String> loading = new HashSet<String>();
        loaded.add("table3");
        LoadTableResponse loadTableResponse = new LoadTableResponse();
        loadTableResponse.setLoaded(loaded);
        loadTableResponse.setFailed(failed);
        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        tableLoadRequest.setTables(null);
        tableLoadRequest.setDatabases(null);
        Mockito.when(tableExtService.loadTables(tableLoadRequest.getTables(), "default", 11)).thenReturn(loadTableResponse);
        Mockito.when(tableExtService.loadTablesByDatabase("default", tableLoadRequest.getDatabases(), 11)).thenReturn(loadTableResponse);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(tableLoadRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
    }

    @Test
    public void testUnloadTable() throws Exception {
        Mockito.doReturn(false).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doNothing().when(tableService).unloadTable("default", "DEFAULT.TABLE");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{project}/{database}/{table}", "default", "DEFAULT", "TABLE")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE");
    }

    @Test
    public void testUnloadTableException() throws Exception {
        Mockito.doReturn(true).when(modelService).isModelsUsingTable("DEFAULT.TABLE", "default");
        Mockito.doNothing().when(tableService).unloadTable("default", "DEFAULT.TABLE");
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/tables/{project}/{database}/{table}", "default", "DEFAULT", "TABLE")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).unloadTable("default", "DEFAULT", "TABLE");
    }

    @Test
    public void testGetTablesAndColumns() throws Exception {
        Mockito.doReturn(mockTableAndColumns()).when(tableService).getTableAndColumns("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/simple_table").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("pageSize", "10").param("pageOffset", "0")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getTablesAndColomns("default", 0, 10);
    }

    @Test
    public void testGetAutoMergeConfig() throws Exception {
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByTable("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("model", "").param("table", "DEFAULT.TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getAutoMergeConfig("", "DEFAULT.TEST_KYLIN_FACT", "default");
    }

    @Test
    public void testGetAutoMergeConfigException() throws Exception {
        Mockito.doReturn(null).when(tableService).getAutoMergeConfigByModel("default", "");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/auto_merge_config").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("model", "").param("table", "")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).getAutoMergeConfig("", "", "default");
    }

    @Test
    public void testGetRefreshDateRange() throws Exception {
        Mockito.doNothing().when(tableService).checkRefreshDataRangeReadiness("default", "DEFAULT.TEST_KYLIN_FACT", "0", "100");
        Mockito.doReturn(null).when(modelService).getAffectedSegmentsResponse("default", "DEFAULT.TEST_KYLIN_FACT", "0", "100", ManagementType.TABLE_ORIENTED);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/affected_data_range").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("start", "0").param("table", "DEFAULT.TEST_KYLIN_FACT")
                .param("end", "100")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getRefreshAffectedDateRange("default", "DEFAULT.TEST_KYLIN_FACT", "0", "100");
    }

    @Test
    public void testGetPushdownMode() throws Exception {
        Mockito.doReturn(true).when(tableService).getPushDownMode("default", "DEFAULT.TEST_KYLIN_FACT");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pushdown_mode").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("table", "DEFAULT.TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getPushdownMode("default", "DEFAULT.TEST_KYLIN_FACT");
    }


    @Test
    public void testRefreshSegments() throws Exception {
        Mockito.doNothing().when(modelService).refreshSegments("default", "TEST_KYLIN_FACT", "0", "100", "0", "100");
        RefreshSegmentsRequest refreshSegmentsRequest = new RefreshSegmentsRequest();
        refreshSegmentsRequest.setProject("default");
        refreshSegmentsRequest.setTable("TEST_KYLIN_FACT");
        refreshSegmentsRequest.setRefreshStart("0");
        refreshSegmentsRequest.setRefreshEnd("100");
        refreshSegmentsRequest.setAffectedStart("0");
        refreshSegmentsRequest.setAffectedEnd("100");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/data_range").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testUpdateAutoMergeConfigException() throws Exception {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setAutoMergeTimeRanges(null);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(autoMergeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
    }

    @Test
    public void testUpdateAutoMergeConfigException2() throws Exception {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        autoMergeRequest.setModel("");
        autoMergeRequest.setTable("");
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(autoMergeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).updateAutoMergeConfig(Mockito.any(AutoMergeRequest.class));
    }

    @Test
    public void testUpdateAutoMergeConfig() throws Exception {
        AutoMergeRequest autoMergeRequest = mockAutoMergeRequest();
        Mockito.doNothing().when(tableService).setAutoMergeConfigByTable("default", autoMergeRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/auto_merge_config").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(autoMergeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
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
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/pushdown_mode").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(config))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setPushdownMode(Mockito.any(PushDownModeRequest.class));
    }

    @Test
    public void testGetLoadedDatabases() throws Exception {
        Mockito.doReturn(null).when(tableService).getLoadedDatabases("default");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/loaded_databases")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).getLoadedDatabases("default");
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

}
