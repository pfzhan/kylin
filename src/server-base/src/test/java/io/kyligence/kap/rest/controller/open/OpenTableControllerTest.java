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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SamplingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.controller.NTableController;
import io.kyligence.kap.rest.request.DateRangeRequest;
import io.kyligence.kap.rest.request.OpenReloadTableRequest;
import io.kyligence.kap.rest.request.RefreshSegmentsRequest;
import io.kyligence.kap.rest.request.TableLoadRequest;
import io.kyligence.kap.rest.service.ProjectService;
import io.kyligence.kap.rest.service.TableService;

public class OpenTableControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private NTableController nTableController;

    @Mock
    private AclEvaluate aclEvaluate;

    @Mock
    private AclUtil aclUtil;

    @Mock
    private ProjectService projectService;

    @Mock
    private TableService tableService;

    @InjectMocks
    private OpenTableController openTableController = Mockito.spy(new OpenTableController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openTableController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

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
        Mockito.doReturn(tableDesc).when(openTableController).getTable(project, tableName);
    }

    @Test
    public void testGetTable() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";
        String database = "DEFAULT";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project).param("table", tableName).param("database", database)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).getTableDesc(project, tableName, database, false, true, 0, 10, 9);

        // call failed  when table is kafka table
        String project1 = "streaming_test";
        String tableName1 = "P_LINEORDER";
        String database1 = "SSB";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project1).param("table", tableName1).param("database", database1)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).getTableDesc(project1, tableName1, database1, false, true, 0, 10, 9);
    }

    @Test
    public void testSetDateRangePass() throws Exception {
        String project = "default";
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        mockGetTable(project, tableName);

        DateRangeRequest dateRangeRequest = new DateRangeRequest();
        dateRangeRequest.setProject(project);
        dateRangeRequest.setTable(tableName);
        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nTableController)
                .setDateRanges(dateRangeRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(dateRangeRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
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

        Mockito.doReturn(new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "")).when(nTableController)
                .refreshSegments(refreshSegmentsRequest);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/tables/data_range") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(refreshSegmentsRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).refreshSegments(Mockito.any(RefreshSegmentsRequest.class));
    }

    @Test
    public void testLoadTables() throws Exception {
        TableLoadRequest tableLoadRequest = new TableLoadRequest();
        tableLoadRequest.setDatabases(new String[] { "kk" });
        tableLoadRequest.setTables(new String[] { "hh.kk" });
        tableLoadRequest.setNeedSampling(false);
        tableLoadRequest.setProject("default");
        Mockito.doNothing().when(openTableController).updateDataSourceType("default", 9);
        Mockito.doAnswer(x -> null).when(nTableController).loadTables(tableLoadRequest);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(10_000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);

        tableLoadRequest.setNeedSampling(true);
        tableLoadRequest.setSamplingRows(1_000);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(tableLoadRequest)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).loadTables(tableLoadRequest);
    }

    @Test
    public void testPreReloadTable() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project).param("table", tableName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).preReloadTable(project, tableName);

        // call failed  when table is kafka table
        String project1 = "streaming_test";
        String tableName1 = "SSB.P_LINEORDER";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/pre_reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project1).param("table", tableName1)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).preReloadTable(project1, tableName1);
    }

    @Test
    public void testReloadTable() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";

        OpenReloadTableRequest request = new OpenReloadTableRequest();
        request.setProject(project);
        request.setTable(tableName);
        request.setNeedSampling(false);

        Mockito.doReturn(new Pair<String, List<String>>()).when(tableService).reloadTable(request.getProject(),
                request.getTable(), request.getNeedSampling(), 0, false, ExecutablePO.DEFAULT_PRIORITY);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).reloadTable(request);

        // test request without need_sampling
        OpenReloadTableRequest request2 = new OpenReloadTableRequest();
        request2.setProject(project);
        request2.setTable(tableName);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request2)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).reloadTable(request2);

        // test request without need_sampling
        OpenReloadTableRequest request3 = new OpenReloadTableRequest();
        request3.setProject("streaming_test");
        request3.setTable("SSB.P_LINEORDER");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/reload") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request3)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
        Mockito.verify(openTableController).reloadTable(request3);
    }


    @Test
    public void testSubmitSamplingFailedForKafkaTable() throws Exception {
        final SamplingRequest request = new SamplingRequest();
        request.setProject("streaming_test");
        request.setRows(20000);
        request.setQualifiedTableName("SSB.P_LINEORDER");

        String errorMsg = MsgPicker.getMsg().getSTREAMING_OPERATION_NOT_SUPPORT();
        final MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders.post("/api/tables/sampling_jobs") //
                .contentType(MediaType.APPLICATION_JSON) //
                .content(JsonUtil.writeValueAsString(request)) //
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Mockito.verify(openTableController).submitSampling(Mockito.any(SamplingRequest.class));
        final JsonNode jsonNode = JsonUtil.readValueAsTree(mvcResult.getResponse().getContentAsString());
        Assert.assertTrue(StringUtils.contains(jsonNode.get("exception").textValue(), errorMsg));
    }

    @Test
    public void testGetPartitioinColumnFormat() throws Exception {
        String project = "default";
        String tableName = "TEST_KYLIN_FACT";
        String columnName = "PART_DT";

        mockMvc.perform(MockMvcRequestBuilders.get("/api/tables/column_format") //
                .contentType(MediaType.APPLICATION_JSON) //
                .param("project", project).param("table", tableName).param("column_name", columnName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON))) //
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openTableController).getPartitionColumnFormat(project, tableName, columnName);
    }

}
