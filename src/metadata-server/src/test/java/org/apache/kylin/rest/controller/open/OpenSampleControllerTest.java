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

package org.apache.kylin.rest.controller.open;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.SampleController;
import org.apache.kylin.rest.request.RefreshSegmentsRequest;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

public class OpenSampleControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private SampleController sampleController;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private ProjectService projectService;

    @Mock
    private TableService tableService;

    @InjectMocks
    private final OpenSampleController openSampleController = Mockito.spy(new OpenSampleController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openSampleController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .defaultResponseCharacterEncoding(StandardCharsets.UTF_8).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("default");
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService)
                .getReadableProjects(projectInstance.getName(), true);
        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());

        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private void mockGetTable(String project, String tableName) {
        TableDesc tableDesc = new TableDesc();
        Mockito.doReturn(tableDesc).when(openSampleController).getTable(project, tableName);
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

        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(sampleController)
                .refreshSegments(refreshSegmentsRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSampleController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testSubmitSamplingCaseInsensitive() throws Exception {
        String tableMixture = "dEFault.teST_kylIN_fact";
        String tableLowercase = "default.test_kylin_fact";
        String tableUppercase = "DEFAULT.TEST_KYLIN_FACT";
        SamplingRequest request = new SamplingRequest();
        request.setProject("default");
        request.setRows(20000);
        ArgumentCaptor<SamplingRequest> argumentCaptor = ArgumentCaptor.forClass(SamplingRequest.class);

        request.setQualifiedTableName(tableMixture);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sampleController).submitSampling(argumentCaptor.capture());
        Assert.assertEquals(tableUppercase, argumentCaptor.getValue().getQualifiedTableName());

        request.setQualifiedTableName(tableLowercase);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sampleController, Mockito.times(2)).submitSampling(argumentCaptor.capture());
        Assert.assertEquals(tableUppercase, argumentCaptor.getValue().getQualifiedTableName());

        request.setQualifiedTableName(tableUppercase);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(sampleController, Mockito.times(3)).submitSampling(argumentCaptor.capture());
        Assert.assertEquals(tableUppercase, argumentCaptor.getValue().getQualifiedTableName());
    }

    @Test
    public void testSubmitSamplingFailedForKafkaTable() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("streaming_test");
        request.setRows(20000);
        request.setQualifiedTableName("SSB.P_LINEORDER");

        String errorMsg = MsgPicker.getMsg().getStreamingOperationNotSupport();
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(openSampleController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testGetPartitionColumnFormat() throws Exception {
        {
            String project = "default";
            String tableName = "TEST_KYLIN_FaCT";
            String columnName = "PART_DT";
            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/column_format") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableName).param("column_name", columnName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(openSampleController).getPartitionColumnFormat(project, tableName, columnName, null);
        }

        {
            // test case-insensitive
            String project = "default";
            String tableNameMixture = "LINeOrder";
            String tableNameLowercase = "lineorder";
            String tableNameUppercase = "LINEORDER";
            String columnName = "PART_DT";

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/column_format") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameMixture).param("column_name", columnName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(1)).getPartitionColumnFormat(project, tableNameUppercase,
                    columnName, null);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/column_format") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameLowercase).param("column_name", columnName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(2)).getPartitionColumnFormat(project, tableNameUppercase,
                    columnName, null);

            mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/column_format") //
                    .contentType(MediaType.APPLICATION_JSON) //
                    .param("project", project).param("table", tableNameUppercase).param("column_name", columnName)
                    .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(tableService, Mockito.times(3)).getPartitionColumnFormat(project, tableNameUppercase,
                    columnName, null);
        }
    }

}
