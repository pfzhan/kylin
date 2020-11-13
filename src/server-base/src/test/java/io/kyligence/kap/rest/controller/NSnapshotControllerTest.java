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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.SnapshotRequest;
import io.kyligence.kap.rest.service.SnapshotService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Set;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

public class NSnapshotControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_PUBLIC_JSON = HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

    private MockMvc mockMvc;

    @Mock
    private SnapshotService snapshotService;

    @InjectMocks
    private NSnapshotController nSnapshotController = Mockito.spy(new NSnapshotController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nSnapshotController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSnapshot() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);

        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables, false, 3);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).buildSnapshotsManually(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testBuildSnapshotFail() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet();
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);
        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables, false, 3);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Mockito.verify(nSnapshotController).buildSnapshotsManually(Mockito.any(SnapshotRequest.class));
        String errorMsg = "KE-10000005(Empty Parameter):You should select at least one table or database to load!!";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testRefreshSnapshot() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);

        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables, false, 3);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).refreshSnapshotsManually(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testRefreshSnapshotFail() throws Exception {
        String project = "default";
        Set<String> needBuildSnapshotTables = Sets.newHashSet();
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(needBuildSnapshotTables);
        Mockito.doAnswer(x -> null).when(snapshotService).buildSnapshots(project, needBuildSnapshotTables, false, 3);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.put("/api/snapshots").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Mockito.verify(nSnapshotController).refreshSnapshotsManually(Mockito.any(SnapshotRequest.class));
        String errorMsg = "KE-10000005(Empty Parameter):You should select at least one table or database to load!!";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testCheckBeforeDeleteSnapshot() throws Exception {
        String project = "default";
        Set<String> deleteSnapshot = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(deleteSnapshot);

        Mockito.doAnswer(x -> null).when(snapshotService).checkBeforeDeleteSnapshots(project, deleteSnapshot);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/snapshots/check_before_delete")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).checkBeforeDelete(Mockito.any(SnapshotRequest.class));
    }

    @Test
    public void testDeleteSnapshot() throws Exception {
        String project = "default";
        Set<String> deleteSnapshot = Sets.newHashSet("TEST_ACCOUNT");
        SnapshotRequest request = new SnapshotRequest();
        request.setProject(project);
        request.setTables(deleteSnapshot);

        Mockito.doAnswer(x -> null).when(snapshotService).checkBeforeDeleteSnapshots(project, deleteSnapshot);
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/snapshots").param("project", project)
                .param("tables", "TEST_ACCOUNT").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).deleteSnapshots(project, deleteSnapshot);
    }

    @Test
    public void testGetSnapshots() throws Exception {
        String project = "default";
        String table = "";
        Set<String > statusFilter = Sets.newHashSet();
        String sortBy = "last_modified_time";
        boolean isReversed = true;
        Mockito.doAnswer(x -> null).when(snapshotService).getProjectSnapshots(project, table, statusFilter, sortBy, isReversed);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots").param("project", project)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).getSnapshots(project, table, 0, 10, statusFilter, sortBy, isReversed);
    }

    @Test
    public void testGetSnapshotsWithInvalidSortBy() throws Exception {
        String project = "default";
        String table = "";
        Set<String > statusFilter = Sets.newHashSet();
        String sortBy = "UNKNOWN";
        boolean isReversed = true;
        Mockito.doAnswer(x -> null).when(snapshotService).getProjectSnapshots(project, table, statusFilter, sortBy, isReversed);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots")
                .param("project", project).param("sort_by", sortBy)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andReturn();
        Mockito.verify(nSnapshotController).getSnapshots(project, table, 0, 10, statusFilter, sortBy, isReversed);
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        String errorMsg = "KE-10000003(Invalid Parameter):No field called 'UNKNOWN'.";
        Assert.assertEquals(errorMsg, jsonNode.get("exception").textValue());
    }

    @Test
    public void testTables() throws Exception {
        String project = "default";
        Mockito.doAnswer(x -> null).when(snapshotService).getTables(project, "", 0, 10);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots/tables").param("project", project)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).getTables(project, "", 0, 10);
    }

    @Test
    public void testLoadMoreTables() throws Exception {
        String project = "default";
        String database = "SSB";
        String table = "";
        Mockito.doAnswer(x -> null).when(snapshotService).getTableNameResponses(project, database, table);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/snapshots/tables/more")
                .param("project", project).param("database", database)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSnapshotController).loadMoreTables(project, table, database, 0, 10);
    }
}
