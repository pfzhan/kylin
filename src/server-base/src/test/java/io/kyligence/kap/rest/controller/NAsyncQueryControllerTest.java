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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.FAILED;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.MISS;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.RUNNING;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.SUCCESS;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.SQLResponse;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.AsyncQuerySQLRequest;
import io.kyligence.kap.rest.response.AsyncQueryResponse;
import io.kyligence.kap.rest.service.AsyncQueryService;
import io.kyligence.kap.rest.service.KapQueryService;

public class NAsyncQueryControllerTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private KapQueryService kapQueryService;

    @Mock
    private AsyncQueryService asyncQueryService;

    @InjectMocks
    private NAsyncQueryController nAsyncQueryController = Mockito.spy(new NAsyncQueryController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nAsyncQueryController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    private AsyncQuerySQLRequest mockAsyncQuerySQLRequest() {
        final AsyncQuerySQLRequest asyncQuerySQLRequest = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest.setQueryId("123");
        asyncQuerySQLRequest.setFormat("csv");
        asyncQuerySQLRequest.setEncode("gbk");
        asyncQuerySQLRequest.setLimit(500);
        asyncQuerySQLRequest.setOffset(0);
        asyncQuerySQLRequest.setProject(PROJECT);
        asyncQuerySQLRequest.setSql("select PART_DT from KYLIN_SALES limit 500");
        asyncQuerySQLRequest.setSeparator(",");
        return asyncQuerySQLRequest;
    }

    @Test
    public void testQuery() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testAsyncQueryContextClean() throws Exception {
        AsyncQuerySQLRequest asyncQuerySQLRequest1 = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest1.setProject(PROJECT);
        AsyncQuerySQLRequest asyncQuerySQLRequest2 = new AsyncQuerySQLRequest();
        asyncQuerySQLRequest2.setProject(PROJECT);
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setException(false);

        Mockito.doReturn(AsyncQueryService.QueryStatus.SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(),
                Mockito.anyString());
        Mockito.doReturn(sqlResponse).when(kapQueryService).doQueryWithCache(Mockito.any(), Mockito.anyBoolean());

        EnvelopeResponse<AsyncQueryResponse> query1 = nAsyncQueryController.query(asyncQuerySQLRequest1);
        EnvelopeResponse<AsyncQueryResponse> query2 = nAsyncQueryController.query(asyncQuerySQLRequest2);

        Assert.assertNotEquals(query1.getData().getQueryID(), query2.getData().getQueryID());
    }

    @Test
    public void testQueryResponseException() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        SQLResponse response = new SQLResponse();
        response.setException(true);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryException() throws Exception {
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQuerySuccess() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryFailed() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryRunning() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testQueryMiss() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).doQueryWithCache((SQLRequest) Mockito.any(),
                Mockito.anyBoolean());

        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).query(Mockito.any());
    }

    @Test
    public void testBatchDeleteAll() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).batchDelete(null, null);
    }

    @Test
    public void testBatchDeleteOldResult() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).batchDelete(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        nAsyncQueryController.batchDelete(PROJECT, "2011-11-11 11:11:11");
    }

    @Test
    public void testDeleteByQueryIdNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(true).when(asyncQueryService).deleteByQueryId(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testDeleteByQueryIdFailed() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(false).when(asyncQueryService).deleteByQueryId(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.delete("/api/async_query/{query_id}", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).deleteByQueryId(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testInqueryStatusNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInqueryStatusSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInqueryStatusFailed() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInqueryStatusRunning() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testInqueryStatusMiss() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).inqueryStatus(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testFileStatusNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).fileStatus(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testFileStatus() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/file_status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).fileStatus(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testMetadataNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).metadata(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testMetadata() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).metadata(Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testDownloadQueryResultNoPermission() throws Exception {
        Mockito.doReturn(KylinConfig.getInstanceFromEnv()).when(kapQueryService).getConfig();
        AsyncQueryService.FileInfo fileInfo = new AsyncQueryService.FileInfo("csv", "gbk", "result");
        Mockito.doReturn(fileInfo).when(asyncQueryService).getFileInfo(Mockito.anyString(), Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.anyBoolean(),
                Mockito.anyBoolean(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testDownloadQueryResult() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        AsyncQueryService.FileInfo fileInfo = new AsyncQueryService.FileInfo("csv", "gbk", "result");
        Mockito.doReturn(fileInfo).when(asyncQueryService).getFileInfo(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(KylinConfig.getInstanceFromEnv()).when(kapQueryService).getConfig();

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/result_download", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).downloadQueryResult(Mockito.anyString(), Mockito.anyBoolean(),
                Mockito.anyBoolean(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testQueryPathNoPermission() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).queryPath(Mockito.anyString(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testQueryPath() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/result_path", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryController).queryPath(Mockito.anyString(), Mockito.any(), Mockito.any());
    }
}