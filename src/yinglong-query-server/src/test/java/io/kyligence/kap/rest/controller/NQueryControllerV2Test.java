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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
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
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryMetrics;
import io.kyligence.kap.rest.controller.v2.NQueryControllerV2;
import io.kyligence.kap.rest.response.SQLResponseV2;
import io.kyligence.kap.rest.service.QueryHistoryService;
import io.kyligence.kap.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;

public class NQueryControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private QueryService kapQueryService;

    @Mock
    private QueryHistoryService queryHistoryService;

    @InjectMocks
    private NQueryControllerV2 nQueryControllerV2 = Mockito.spy(new NQueryControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nQueryControllerV2).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    private PrepareSqlRequest mockPrepareSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        sqlRequest.setProject("default");
        return sqlRequest;
    }

    @Test
    public void testPrepareQuery() throws Exception {
        final PrepareSqlRequest sqlRequestV2 = mockPrepareSqlRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(sqlRequestV2))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryControllerV2).prepareQuery(Mockito.any());
    }

    @Test
    public void testQuery() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockPrepareSqlRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nQueryControllerV2).query(Mockito.any());
    }

    private PrepareSqlRequest mockInvalidateTagSqlRequest() {
        final PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setSql("SELECT * FROM empty_table");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i <= 256; i++) {
            builder.append('a');
        }
        sqlRequest.setUser_defined_tag(builder.toString());
        return sqlRequest;
    }

    @Test
    public void testQueryUserTagExceedLimitation() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockInvalidateTagSqlRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(MockMvcResultMatchers.jsonPath("$.code").value("999")).andExpect(MockMvcResultMatchers
                        .jsonPath("$.msg").value("Can’t add the tag, as the length exceeds the maximum 256 characters. Please modify it."));

        Mockito.verify(nQueryControllerV2, Mockito.times(0)).query(Mockito.any());
    }

    @Test
    public void testPrepareQueryUserTagExceedLimitation() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query/prestate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockInvalidateTagSqlRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        Mockito.verify(nQueryControllerV2, Mockito.times(0)).prepareQuery(Mockito.any());
    }

    @Test
    public void testSqlResponseV2() {
        SQLResponse sqlResponse = new SQLResponse();
        sqlResponse.setScanRows(Lists.newArrayList(50L, 10L));
        NativeQueryRealization nativeQueryRealization1 = new NativeQueryRealization();
        nativeQueryRealization1.setIndexType(QueryMetrics.AGG_INDEX);
        nativeQueryRealization1.setModelAlias("modelA");
        NativeQueryRealization nativeQueryRealization2 = new NativeQueryRealization();
        nativeQueryRealization2.setIndexType(QueryMetrics.TABLE_INDEX);
        nativeQueryRealization2.setModelAlias("modelB");
        sqlResponse.setNativeRealizations(Lists.newArrayList(nativeQueryRealization1, nativeQueryRealization2));
        SQLResponseV2 sqlResponseV2 = new SQLResponseV2(sqlResponse);
        Assert.assertEquals(sqlResponseV2.getThrowable(), sqlResponse.getThrowable());
        Assert.assertEquals(sqlResponseV2.getTotalScanRows(), sqlResponse.getTotalScanRows());
        Assert.assertEquals(sqlResponseV2.getTotalScanBytes(), sqlResponse.getTotalScanBytes());
        Assert.assertEquals(sqlResponseV2.getTotalScanCount(), sqlResponse.getTotalScanRows());
        Assert.assertTrue(sqlResponseV2.isSparderUsed());
        Assert.assertEquals("CUBE[name=modelA,modelB],INVERTED_INDEX[name=modelB]", sqlResponseV2.getCube());

        SQLResponseV2 sqlResponseV22 = new SQLResponseV2();
        Assert.assertFalse(sqlResponseV22.isSparderUsed());
        Assert.assertTrue(StringUtils.isEmpty(sqlResponseV22.adapterCubeField(sqlResponseV22.getNativeRealizations())));
        Assert.assertEquals("CUBE[name=modelA]",
                sqlResponseV22.adapterCubeField(Lists.newArrayList(nativeQueryRealization1)));
        Assert.assertThrows(NullPointerException.class, () -> new SQLResponseV2(null));
    }
}
