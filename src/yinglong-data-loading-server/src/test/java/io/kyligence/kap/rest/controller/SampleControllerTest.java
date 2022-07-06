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

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.dao.ExecutablePO;
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
import io.kyligence.kap.job.service.TableSampleService;
import io.kyligence.kap.rest.request.PartitionKeyRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.TableSamplingService;
import io.kyligence.kap.rest.service.TableService;

public class SampleControllerTest extends NLocalFileMetadataTestCase {

    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private ModelBuildService modelBuildService;

    @Mock
    private TableSamplingService tableSamplingService;

    @Mock
    private TableSampleService tableSampleService;

    @Mock
    private TableService tableService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private final SampleController sampleController = Mockito.spy(new SampleController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(sampleController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

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
    public void testRefreshSegments() throws Exception {
        Mockito.doNothing().when(modelBuildService).refreshSegments("default", "TEST_KYLIN_FACT", "0", "100", "0",
                "100");
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
        Mockito.verify(sampleController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testSubmitSampling() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("default.test_kylin_fact");
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
    }

    @Test
    public void testSubmitSamplingFailedForNoTable() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);

        String errorMsg = "Can’t perform table sampling. Please select at least one table.";
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testSubmitSamplingFailedForIllegalTableName() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        request.setQualifiedTableName("test_kylin_fact");

        String errorMsg = "The name of table for sampling is invalid. Please enter a table name like “database.table”.";
        Mockito.doReturn(Lists.newArrayList()).when(tableSamplingService) //
                .sampling(Sets.newHashSet(request.getQualifiedTableName()), request.getProject(), request.getRows(),
                        ExecutablePO.DEFAULT_PRIORITY, null, null);
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(sampleController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

}
