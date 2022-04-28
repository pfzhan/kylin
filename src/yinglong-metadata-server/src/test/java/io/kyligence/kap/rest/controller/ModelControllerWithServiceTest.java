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
import org.junit.Ignore;
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
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.controller.open.OpenModelController;
import io.kyligence.kap.rest.controller.open.OpenModelSmartController;
import io.kyligence.kap.rest.request.ModelSuggestionRequest;
import io.kyligence.kap.rest.response.OpenSuggestionResponse;
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

    @Autowired
    OpenModelSmartController openModelSmartController;

    @Autowired
    NModelSmartController modelSmartController;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders
                .standaloneSetup(openModelController, modelController, openModelSmartController, modelSmartController)
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
                            .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(req))
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
    public void testReuseSuggestModelJoinType() throws Exception {
        changeProjectToSemiAutoMode("default");
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");

        val modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        modelManager.listAllModels().forEach(nDataModel -> {
            modelManager.dropModel(nDataModel.getId());
            dataflowManager.dropDataflow(nDataModel.getId());
        });

        String createNewModelSql = "SELECT * FROM\n"
                + "(SELECT TEST_KYLIN_FACT.SELLER_ID,TEST_KYLIN_FACT.LSTG_FORMAT_NAME,TEST_KYLIN_FACT.LEAF_CATEG_ID,\n"
                + "TEST_KYLIN_FACT.CAL_DT,TEST_KYLIN_FACT.SLR_SEGMENT_CD,TEST_ACCOUNT.ACCOUNT_ID,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,\n"
                + "TEST_ACCOUNT.ACCOUNT_COUNTRY,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,TEST_ACCOUNT.ACCOUNT_CONTACT,\n"
                + "SUM(TEST_KYLIN_FACT.SELLER_ID),SUM(TEST_KYLIN_FACT.TRANS_ID)\n"
                + "FROM TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY TEST_KYLIN_FACT.SELLER_ID,TEST_KYLIN_FACT.LSTG_FORMAT_NAME,TEST_KYLIN_FACT.LEAF_CATEG_ID,\n"
                + "TEST_KYLIN_FACT.CAL_DT,TEST_KYLIN_FACT.SLR_SEGMENT_CD,TEST_ACCOUNT.ACCOUNT_ID,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,\n"
                + "TEST_ACCOUNT.ACCOUNT_COUNTRY,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,TEST_ACCOUNT.ACCOUNT_CONTACT) D\n"
                + "WHERE D.ACCOUNT_ID>1;";

        String suggestReuseModelSql = "SELECT * FROM\n"
                + "(SELECT TEST_KYLIN_FACT.TRANS_ID,TEST_KYLIN_FACT.LSTG_FORMAT_NAME,TEST_KYLIN_FACT.LEAF_CATEG_ID,\n"
                + "TEST_KYLIN_FACT.CAL_DT,TEST_KYLIN_FACT.SLR_SEGMENT_CD,TEST_ACCOUNT.ACCOUNT_ID,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,\n"
                + "TEST_ACCOUNT.ACCOUNT_COUNTRY,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,TEST_ACCOUNT.ACCOUNT_CONTACT,\n"
                + "SUM(TEST_KYLIN_FACT.SELLER_ID),SUM(TEST_KYLIN_FACT.TRANS_ID)\n"
                + "FROM TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID\n"
                + "GROUP BY TEST_KYLIN_FACT.TRANS_ID,TEST_KYLIN_FACT.LSTG_FORMAT_NAME,TEST_KYLIN_FACT.LEAF_CATEG_ID,\n"
                + "TEST_KYLIN_FACT.CAL_DT,TEST_KYLIN_FACT.SLR_SEGMENT_CD,TEST_ACCOUNT.ACCOUNT_ID,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,\n"
                + "TEST_ACCOUNT.ACCOUNT_COUNTRY,TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL,TEST_ACCOUNT.ACCOUNT_CONTACT) D\n"
                + "WHERE D.ACCOUNT_ID>1;";

        List<String> sqls = Lists.newArrayList(createNewModelSql);
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
                            .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(req))
                            .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                            .andExpect(MockMvcResultMatchers.status().isOk()) //
                            .andExpect(result2 -> {
                                val df = dataflowManager.getDataflow(modelId);
                                Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
                                Assert.assertEquals(1, df.getSegments().size());
                                Assert.assertEquals(SegmentStatusEnum.READY, df.getSegments().get(0).getStatus());
                            });
                });

        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        List<String> sqls2 = Lists.newArrayList(suggestReuseModelSql);
        val favoriteRequest2 = new SqlAccelerateRequest("default", sqls2, true);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest2))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result1 -> {
                    val response = JsonUtil.readValue(result1.getResponse().getContentAsString(), ref);
                    val req = response.getData();
                    req.setProject("default");
                    req.setWithEmptySegment(true);
                    req.setWithModelOnline(true);
                    val modelId = req.getReusedModels().get(0).getId();
                    mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_recommendation")
                            .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(req))
                            .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                            .andExpect(MockMvcResultMatchers.status().isOk()) //
                            .andExpect(result2 -> {
                                val dataModel = modelManager.getDataModelDesc(modelId);
                                Assert.assertEquals("LEFT", dataModel.getJoinTables().get(0).getJoin().getType());
                            });
                });
    }

    @Test
    @Ignore
    public void testSuggestModels() throws Exception {
        changeProjectToSemiAutoMode("default");

        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), "default");
        TypeReference<EnvelopeResponse<OpenSuggestionResponse>> ref = new TypeReference<EnvelopeResponse<OpenSuggestionResponse>>() {
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

    @Test
    public void testSuggestModelsWithMatchJoins1() throws Exception {
        changeProjectToSemiAutoMode("default");
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        String sql = "SELECT \n" + "COUNT(\"TEST_KYLIN_FACT\".\"SELLER_ID\")\n" + "FROM \n"
                + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                + "INNER JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n" + // left or inner join
                "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND "
                + "\"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n" + // left or inner join
                "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n" + // left or inner join
                "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\"\n"
                + "WHERE \"BUYER_COUNTRY\".\"COUNTRY\" is not null\n" + "GROUP BY \"TEST_KYLIN_FACT\".\"TRANS_ID\"";
        List<String> sqls = Lists.newArrayList(sql);
        val favoriteRequest = new SqlAccelerateRequest("default", sqls, true);
        val ref = new TypeReference<EnvelopeResponse<ModelSuggestionRequest>>() {
        };
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result1 -> {
                    val response = JsonUtil.readValue(result1.getResponse().getContentAsString(), ref);
                    val reusedModels = response.getData().getReusedModels();
                    Assert.assertEquals(1, reusedModels.size());
                });
    }

    @Test
    public void testSuggestModelsWithMatchJoins2() throws Exception {
        changeProjectToSemiAutoMode("default");
        getTestConfig().setProperty("kylin.query.join-match-optimization-enabled", "true");
        String sql = "SELECT \n" + "COUNT(\"TEST_KYLIN_FACT\".\"SELLER_ID\")\n" + "FROM \n"
                + "\"DEFAULT\".\"TEST_KYLIN_FACT\" as \"TEST_KYLIN_FACT\" \n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ORDER\" as \"TEST_ORDER\"\n" + // left or inner join
                "ON \"TEST_KYLIN_FACT\".\"ORDER_ID\"=\"TEST_ORDER\".\"ORDER_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SELLER_TYPE_DIM\" as \"TEST_SELLER_TYPE_DIM\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"SLR_SEGMENT_CD\"=\"TEST_SELLER_TYPE_DIM\".\"SELLER_TYPE_CD\"\n"
                + "INNER JOIN \"EDW\".\"TEST_CAL_DT\" as \"TEST_CAL_DT\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"CAL_DT\"=\"TEST_CAL_DT\".\"CAL_DT\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_CATEGORY_GROUPINGS\" as \"TEST_CATEGORY_GROUPINGS\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LEAF_CATEG_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"LEAF_CATEG_ID\" AND "
                + "\"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_CATEGORY_GROUPINGS\".\"SITE_ID\"\n"
                + "INNER JOIN \"EDW\".\"TEST_SITES\" as \"TEST_SITES\"\n"
                + "ON \"TEST_KYLIN_FACT\".\"LSTG_SITE_ID\"=\"TEST_SITES\".\"SITE_ID\"\n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"SELLER_ACCOUNT\"\n" + // left or inner join
                "ON \"TEST_KYLIN_FACT\".\"SELLER_ID\"=\"SELLER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "LEFT JOIN \"DEFAULT\".\"TEST_ACCOUNT\" as \"BUYER_ACCOUNT\"\n" + // left or inner join
                "ON \"TEST_ORDER\".\"BUYER_ID\"=\"BUYER_ACCOUNT\".\"ACCOUNT_ID\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"SELLER_COUNTRY\"\n"
                + "ON \"SELLER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"SELLER_COUNTRY\".\"COUNTRY\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_COUNTRY\" as \"BUYER_COUNTRY\"\n"
                + "ON \"BUYER_ACCOUNT\".\"ACCOUNT_COUNTRY\"=\"BUYER_COUNTRY\".\"COUNTRY\"\n"
                + "GROUP BY \"TEST_KYLIN_FACT\".\"TRANS_ID\"";
        List<String> sqls = Lists.newArrayList(sql);
        val favoriteRequest = new SqlAccelerateRequest("default", sqls, true);
        val ref = new TypeReference<EnvelopeResponse<ModelSuggestionRequest>>() {
        };
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()) //
                .andExpect(result1 -> {
                    val response = JsonUtil.readValue(result1.getResponse().getContentAsString(), ref);
                    val reusedModels = response.getData().getReusedModels();
                    Assert.assertEquals(1, reusedModels.size());
                });
    }

    private void changeProjectToSemiAutoMode(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.updateProject(project, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }
}
