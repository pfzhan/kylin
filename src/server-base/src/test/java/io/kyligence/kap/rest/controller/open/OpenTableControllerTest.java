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
package io.kyligence.kap.rest.controller.open;

import io.kyligence.kap.rest.controller.NTableController;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

public class OpenTableControllerTest {

    private MockMvc mockMvc;

    @Mock
    private NTableController nTableController;

    @InjectMocks
    private OpenTableController openTableController = Mockito.spy(new OpenTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openTableController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    private void mockGetTable(String project, String tableName) {
        TableDesc tableDesc = new TableDesc();
        Mockito.doReturn(tableDesc).when(openTableController).getTable(project, tableName);
    }

    @Test
    public void testSetDateRangePass() throws Exception {
        String project = "default";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        mockGetTable(project, tableName);

        DateRangeRequest dateRangeRequest = new DateRangeRequest();
        dateRangeRequest.setProject(project);
        dateRangeRequest.setTable(tableName);
        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nTableController)
                .setDateRanges(dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/open/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(dateRangeRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).setDateRanges(Mockito.any(DateRangeRequest.class));
    }

    @Test
    public void testRefreshSegments() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";
        mockGetTable(project, tableName);

        RefreshSegmentsRequest refreshSegmentsRequest = new RefreshSegmentsRequest();
        refreshSegmentsRequest.setProject(project);
        refreshSegmentsRequest.setRefreshStart("0");
        refreshSegmentsRequest.setRefreshEnd("100");
        refreshSegmentsRequest.setAffectedStart("0");
        refreshSegmentsRequest.setAffectedEnd("100");
        refreshSegmentsRequest.setTable(tableName);

        Mockito.doReturn(new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, "", "")).when(nTableController)
                .refreshSegments(refreshSegmentsRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/open/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

}
