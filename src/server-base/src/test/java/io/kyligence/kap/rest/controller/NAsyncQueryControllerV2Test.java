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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.FAILED;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.MISS;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.RUNNING;
import static io.kyligence.kap.rest.service.AsyncQueryService.QueryStatus.SUCCESS;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.AsyncQuerySQLRequestV2;
import io.kyligence.kap.rest.service.AsyncQueryService;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.AclEvaluate;
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


import java.io.IOException;

public class NAsyncQueryControllerV2Test extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    private MockMvc mockMvc;

    @Mock
    private QueryService kapQueryService;

    @Mock
    private AsyncQueryService asyncQueryService;

    @Mock
    private AclEvaluate aclEvaluate;

    @InjectMocks
    private NAsyncQueryController nAsyncQueryController = Mockito.spy(new NAsyncQueryController());

    @InjectMocks
    private NAsyncQueryControllerV2 nAsyncQueryControllerV2 = Mockito.spy(new NAsyncQueryControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nAsyncQueryControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();

        Mockito.doReturn("default").when(asyncQueryService).searchQueryResultProject(Mockito.anyString());
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        QueryContext.current().close();
    }

    private AsyncQuerySQLRequestV2 mockAsyncQuerySQLRequest() {
        final AsyncQuerySQLRequestV2 asyncQuerySQLRequest = new AsyncQuerySQLRequestV2();
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
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQuerySuccess() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryFailed() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(FAILED).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryRunning() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(RUNNING).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testQueryMiss() throws Exception {
        SQLResponse response = new SQLResponse();
        response.setException(false);
        Mockito.doReturn(response).when(kapQueryService).queryWithCache(Mockito.any());

        Mockito.doReturn(MISS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.post("/api/async_query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).query(Mockito.any());
    }

    @Test
    public void testInqueryStatusSuccess() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());
        Mockito.doReturn(SUCCESS).when(asyncQueryService).queryStatus(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/status", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).inqueryStatus(Mockito.anyString());
    }

    @Test
    public void testFileStatus() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id}/filestatus", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).fileStatus(Mockito.anyString());
    }

    @Test
    public void testMetadata() throws Exception {
        Mockito.doReturn(true).when(asyncQueryService).hasPermission(Mockito.anyString(), Mockito.anyString());

        mockMvc.perform(MockMvcRequestBuilders.get("/api/async_query/{query_id:.+}/metadata", "123")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockAsyncQuerySQLRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).metadata(Mockito.anyString());
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
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nAsyncQueryControllerV2).downloadQueryResult(Mockito.anyString(), Mockito.anyBoolean(), Mockito.any());
    }

}