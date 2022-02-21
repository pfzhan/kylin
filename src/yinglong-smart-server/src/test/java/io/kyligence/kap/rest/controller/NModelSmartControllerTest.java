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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
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

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.service.ModelSmartService;

public class NModelSmartControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    @Mock
    private ModelSmartService modelSmartService;
    @InjectMocks
    private NModelSmartController nModelSmartController = Mockito.spy(new NModelSmartController());
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nModelSmartController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testSuggestModelWithReuseExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        SqlAccelerateRequest favoriteRequest = new SqlAccelerateRequest("gc_test", sqls, true);
        // reuse existed model
        Mockito.doReturn(null).when(modelSmartService).suggestModel(favoriteRequest.getProject(), Mockito.spy(sqls),
                true, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelSmartController).suggestModel(Mockito.any());
    }

    @Test
    public void testSuggestModelWithoutReuseExistedModel() throws Exception {
        // don't reuse existed model
        String sql = "SELECT lstg_format_name, test_cal_dt.week_beg_dt, sum(price)\n" + "FROM test_kylin_fact\n"
                + "INNER JOIN edw.test_cal_dt AS test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "GROUP BY lstg_format_name, test_cal_dt.week_beg_dt";
        List<String> sqls = Lists.newArrayList(sql);
        SqlAccelerateRequest accerelateRequest = new SqlAccelerateRequest("gc_test", sqls, false);
        Mockito.doReturn(null).when(modelSmartService).suggestModel(accerelateRequest.getProject(), Mockito.spy(sqls),
                false, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(accerelateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelSmartController).suggestModel(Mockito.any());
    }

    @Test
    public void test_api_can_answered_by_existed_model() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("gc_test", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/can_answered_by_existed_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nModelSmartController).couldAnsweredByExistedModel(Mockito.any());
    }
}
