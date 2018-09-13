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

import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.FactTableRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NTableControllerTest {

    private MockMvc mockMvc;

    @Mock
    private TableService tableService;

    @Mock
    private TableExtService tableExtService;

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
        factTableRequest.setFact(true);
        return factTableRequest;
    }

    private TableLoadRequest mockLoadTableRequest() {
        final TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setProject("default");
        tableLoadRequest.setDatasourceType(11);
        String[] tables = { "table1", "table2" };
        tableLoadRequest.setTables(tables);

        return tableLoadRequest;
    }

    @Test
    public void testGetTableDesc() throws Exception {
        Mockito.when(tableService.getTableDesc("default", false, "")).thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .param("withExt", "false").param("project", "default").param("table", "")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "");
    }

    @Test
    public void testGetTableDescWithName() throws Exception {
        Mockito.when(tableService.getTableDesc("default", true, "TEST_KYLIN_FACT")).thenReturn(mockTables());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .param("withExt", "false").param("project", "default").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nTableController).getTableDesc(false, "default", "TEST_KYLIN_FACT");
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
        List<String> list = new ArrayList<>();
        list.add("ddd");
        list.add("fff");
        Mockito.when(tableService.getSourceTableNames("default", "db1", 11)).thenReturn(list);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/names").contentType(MediaType.APPLICATION_JSON)
                .param("project", "default").param("datasourceType", "11").param("database", "db1")
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).showTables("default", 11, "db1");
    }

    @Test
    public void testSetTableFact() throws Exception {
        final FactTableRequest factTableRequest = mockFactTableRequest();
        Mockito.doNothing().when(tableService).setFact(factTableRequest.getProject(), factTableRequest.getTable(),
                factTableRequest.getFact(), factTableRequest.getColumn());

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
                factTableRequest.getFact(), factTableRequest.getColumn());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/fact").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(factTableRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
        Mockito.verify(nTableController).setTableFact(Mockito.any(FactTableRequest.class));
    }

    @Test
    public void testSetDateRangePass() throws Exception {
        final DateRangeRequest dateRangeRequest = mockDateRangeRequest();
        Mockito.doNothing().when(tableService).setDataRange(dateRangeRequest.getProject(), dateRangeRequest.getTable(),
                new SegmentRange.TimePartitionedSegmentRange(dateRangeRequest.getStartTime(),
                        dateRangeRequest.getEndTime()));
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/date_range").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(dateRangeRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).setDateRanges(Mockito.any(DateRangeRequest.class));

    }

    private DateRangeRequest mockDateRangeRequest() {
        DateRangeRequest request = new DateRangeRequest();
        request.setStartTime(0L);
        request.setEndTime(Long.MAX_VALUE);
        request.setProject("default");
        request.setTable("TEST_KYLIN_FACT");
        return request;
    }

    @Test
    public void testLoadTables() throws Exception {
        String[] tables = { "table1", "table2" };
        Set<String> loaded = new HashSet<String>();
        loaded.add("table1");
        Set<String>[] result = new Set[3];
        org.apache.commons.lang.ArrayUtils.add(result, loaded);

        Mockito.when(tableExtService.loadTables(tables, "default", 11)).thenReturn(result);

        final TableLoadRequest tableLoadRequest = mockLoadTableRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(tableLoadRequest))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nTableController).loadTables(Mockito.any(TableLoadRequest.class));
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
