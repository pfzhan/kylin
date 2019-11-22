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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.controller.v2.NModelControllerV2;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.RelatedModelResponse;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.OptimizeRecommendationService;
import io.kyligence.kap.rest.service.ProjectService;
import org.apache.kylin.rest.constant.Constant;
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
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.List;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

public class NModelControllerV2Test extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @Mock
    private ProjectService projectService;

    @Mock
    private OptimizeRecommendationService optimizeRecommendationService;

    @InjectMocks
    private NModelControllerV2 nModelControllerV2 = Mockito.spy(new NModelControllerV2());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nModelControllerV2)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private List<NDataModelResponse> mockModels() {
        final List<NDataModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new NDataModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        models.add(new NDataModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new NDataModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new NDataModelResponse(model3));

        return models;
    }

    private List<RelatedModelResponse> mockRelatedModels() {
        final List<RelatedModelResponse> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setUuid("model1");
        models.add(new RelatedModelResponse(model));
        NDataModel model1 = new NDataModel();
        model.setUuid("model2");
        models.add(new RelatedModelResponse(model1));
        NDataModel model2 = new NDataModel();
        model.setUuid("model3");
        models.add(new RelatedModelResponse(model2));
        NDataModel model3 = new NDataModel();
        model.setUuid("model4");
        models.add(new RelatedModelResponse(model3));

        return models;
    }

    @Test
    public void testGetModels() throws Exception {

        Mockito.when(modelService.getModels("model1", "default", true, "ADMIN", "NEW", "last_modify", false))
                .thenReturn(mockModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("projectName", "default").param("model", "model1").param("limit", "10")
                .param("exact", "true").param("table", "").param("owner", "ADMIN").param("status", "NEW")
                .param("sortBy", "last_modify").param("reverse", "true")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelControllerV2).getModels("model1", true, "default", "ADMIN", "NEW", "", 0, 10,
                "last_modify", true);
    }

    @Test
    public void testGetRelatedModels() throws Exception {

        Mockito.when(modelService.getRelateModels("default", "TEST_KYLIN_FACT", "model1"))
                .thenReturn(mockRelatedModels());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                .param("offset", "0").param("projectName", "default").param("model", "model1").param("limit", "10")
                .param("exact", "true").param("owner", "ADMIN").param("status", "NEW").param("sortBy", "last_modify")
                .param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelControllerV2).getModels("model1", true, "default", "ADMIN", "NEW", "TEST_KYLIN_FACT", 0,
                10, "last_modify", true);
    }

    @Test
    public void testGetModelsWithOutModelName() throws Exception {
        Mockito.when(modelService.getModels("", "default", true, "ADMIN", "NEW", "last_modify", true))
                .thenReturn(mockModels());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                        .param("offset", "0").param("projectName", "default").param("model", "").param("limit", "10")
                        .param("exact", "true").param("owner", "ADMIN").param("status", "NEW")
                        .param("sortBy", "last_modify").param("reverse", "true").param("table", "TEST_KYLIN_FACT")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V2_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelControllerV2).getModels("", true, "default", "ADMIN", "NEW", "TEST_KYLIN_FACT", 0, 10,
                "last_modify", true);
    }

}
