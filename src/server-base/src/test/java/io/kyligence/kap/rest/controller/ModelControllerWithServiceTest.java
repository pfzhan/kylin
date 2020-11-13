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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.controller.open.OpenModelController;
import io.kyligence.kap.rest.request.ModelSuggestionRequest;
import io.kyligence.kap.rest.response.OpenModelSuggestionResponse;
import io.kyligence.kap.rest.service.ProjectService;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelControllerWithServiceTest extends ServiceTestBase {

    private MockMvc mockMvc;

    @Mock
    private ProjectService projectService;

    @Autowired
    OpenModelController openModelController;

    @Autowired
    NModelController modelController;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openModelController, modelController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("default");
        Mockito.doReturn(Lists.newArrayList(projectInstance)).when(projectService)
                .getReadableProjects(projectInstance.getName(), true);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testApproveSuggestModel() throws Exception {
        changeProjectToSemiAutoMode("default");
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");

        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact group by price limit 1");
        val favoriteRequest = new SqlAccelerateRequest("default", sqls, false);

        val ref = new TypeReference<EnvelopeResponse<ModelSuggestionRequest>>() {
        };

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result1 -> {
                    val response = JsonUtil.readValue(result1.getResponse().getContentAsString(), ref);
                    val req = response.getData();
                    req.setProject("default");
                    req.setWithEmptySegment(true);
                    req.setWithModelOnline(true);
                    val modelId = req.getNewModels().get(0).getId();
                    mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_recommendation")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(JsonUtil.writeValueAsString(req))
                            .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                            .andExpect(MockMvcResultMatchers.status().isOk()) //
                            .andExpect(result2 -> {
                                val df = dataflowManager.getDataflow(modelId);
                                Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
                                Assert.assertEquals(1, df.getSegments().size());
                                Assert.assertEquals(SegmentStatusEnum.READY, df.getSegments().get(0).getStatus());
                            });
                });
    }

    @Test
    public void testSuggestModels() throws Exception {
        changeProjectToSemiAutoMode("default");

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        TypeReference<EnvelopeResponse<OpenModelSuggestionResponse>> ref = new TypeReference<EnvelopeResponse<OpenModelSuggestionResponse>>() {
        };

        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact group by price limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);
        favoriteRequest.setWithModelOnline(true);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result -> {
                    val response = JsonUtil.readValue(result.getResponse().getContentAsString(), ref);
                    Assert.assertEquals(1, response.getData().getModels().size());
                    val modelId = response.getData().getModels().get(0).getUuid();
                    val df = dataflowManager.getDataflow(modelId);
                    Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
                    Assert.assertEquals(1, df.getSegments().size());
                    Assert.assertEquals(SegmentStatusEnum.READY, df.getSegments().get(0).getStatus());
                });

        favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);
        favoriteRequest.setWithEmptySegment(false);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result -> {
                    val response = JsonUtil.readValue(result.getResponse().getContentAsString(), ref);
                    Assert.assertEquals(1, response.getData().getModels().size());
                    val modelId = response.getData().getModels().get(0).getUuid();
                    val df = dataflowManager.getDataflow(modelId);
                    Assert.assertEquals(RealizationStatusEnum.OFFLINE, df.getStatus());
                    Assert.assertEquals(0, df.getSegments().size());
                });
    }

    private void changeProjectToSemiAutoMode(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(project, copyForWrite -> {
            copyForWrite.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }
}
