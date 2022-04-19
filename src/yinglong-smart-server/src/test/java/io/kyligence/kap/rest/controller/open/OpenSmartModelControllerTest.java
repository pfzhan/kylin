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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.util.AclEvaluate;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.response.OpenValidationResponse;
import io.kyligence.kap.rest.response.SuggestionResponse;
import io.kyligence.kap.rest.service.ModelSmartService;
import io.kyligence.kap.smart.AbstractContext;
import io.kyligence.kap.smart.ModelCreateContext;
import io.kyligence.kap.smart.ModelReuseContext;
import io.kyligence.kap.smart.ModelSelectContext;
import lombok.val;
import lombok.var;

public class OpenSmartModelControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelSmartService modelSmartService;

    @InjectMocks
    private OpenModelSmartController openSmartModelController = Mockito.spy(new OpenModelSmartController());

    @Mock
    private AclEvaluate aclEvaluate;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(openSmartModelController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);

        Mockito.doReturn(true).when(aclEvaluate).hasProjectWritePermission(Mockito.any());
        Mockito.doReturn(true).when(aclEvaluate).hasProjectOperationPermission(Mockito.any());
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCouldAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        AbstractContext proposeContext = new ModelSelectContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        Mockito.doReturn(proposeContext).when(modelSmartService).probeRecommendation(favoriteRequest.getProject(),
                favoriteRequest.getSqls());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validation").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).couldAnsweredByExistedModel(Mockito.any());
    }

    @Test
    public void testAnsweredByExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("default", sqls);
        Mockito.doReturn(new OpenValidationResponse()).when(openSmartModelController).batchSqlValidate("default", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_validation")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).answeredByExistedModel(Mockito.any());
    }

    @Test
    public void testSuggestModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelCreateContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, false,
                false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_suggestion")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).suggestModels(Mockito.any());
    }

    @Test
    public void testOptimizeModels() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/model_optimization")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).optimizeModels(Mockito.any());
    }

    @Test
    public void testAccSqls() throws Exception {
        changeProjectToSemiAutoMode("default");
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        OpenSqlAccelerateRequest favoriteRequest = new OpenSqlAccelerateRequest("default", sqls, null);

        // reuse existed model
        AbstractContext context = new ModelReuseContext(getTestConfig(), favoriteRequest.getProject(),
                sqls.toArray(new String[0]));
        val result = new SuggestionResponse(Lists.newArrayList(), Lists.newArrayList());
        Mockito.doReturn(context).when(modelSmartService).suggestModel(favoriteRequest.getProject(), sqls, true, false);
        Mockito.doReturn(result).when(modelSmartService).buildModelSuggestionResponse(context);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/accelerate_sqls")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(openSmartModelController).accelerateSqls(Mockito.any());
    }

    @Test
    public void testCheckSqlListNotEmpty() {
        List<String> sqls = null;
        try {
            OpenModelSmartController.checkNotEmpty(sqls);
        } catch (KylinException e) {
            Assert.assertEquals("999", e.getCode());
            Assert.assertEquals(MsgPicker.getMsg().getNULL_EMPTY_SQL(), e.getMessage());
        }
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
