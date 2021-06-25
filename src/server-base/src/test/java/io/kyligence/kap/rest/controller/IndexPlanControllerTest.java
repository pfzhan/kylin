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

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.rest.request.CreateBaseIndexRequest;
import io.kyligence.kap.rest.service.FusionIndexService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
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
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.rest.request.UpdateRuleBasedCuboidRequest;
import io.kyligence.kap.rest.response.BuildIndexResponse;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.ModelService;
import lombok.val;

public class IndexPlanControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private IndexPlanService indexPlanService;

    @Mock
    private FusionIndexService fusionIndexService;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private NIndexPlanController indexPlanController = Mockito.spy(new NIndexPlanController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(indexPlanController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateRule() throws Exception {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").aggregationGroups(Lists.newArrayList()).build();
        Mockito.when(fusionIndexService.updateRuleBasedCuboid(Mockito.anyString(),
                Mockito.any(UpdateRuleBasedCuboidRequest.class)))
                .thenReturn(new Pair<>(new IndexPlan(), new BuildIndexResponse()));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(indexPlanController).updateRule(Mockito.any(UpdateRuleBasedCuboidRequest.class));
    }

    @Test
    public void testGetRule() throws Exception {
        Mockito.doReturn(null).when(fusionIndexService).getIndexes("default", "abc", "",
                Lists.newArrayList(IndexEntity.Status.NO_BUILD), "data_size", false, null, null, null);
        mockMvc.perform(MockMvcRequestBuilders.get("/api/index_plans/index")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model", "abc")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testCalculateDiffRuleBasedIndex() throws Exception {
        val request = UpdateRuleBasedCuboidRequest.builder().project("default")
                .modelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa").aggregationGroups(Lists.newArrayList()).build();
        Mockito.when(indexPlanService.calculateDiffRuleBasedIndex(Mockito.any(UpdateRuleBasedCuboidRequest.class)))
                .thenReturn(new DiffRuleBasedIndexResponse("", 1, 1, 1));
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/rule_based_index_diff")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(indexPlanController)
                .calculateDiffRuleBasedIndex(Mockito.any(UpdateRuleBasedCuboidRequest.class));
    }

    @Test
    public void testCreateBaseIndex() throws Exception {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setProject("default");
        request.setModelId("abc");
        Mockito.doReturn(null).when(indexPlanService).createBaseIndex("default", request);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/index_plans/base_index")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testUpdateBaseIndex() throws Exception {
        CreateBaseIndexRequest request = new CreateBaseIndexRequest();
        request.setProject("default");
        request.setModelId("abc");
        Mockito.doReturn(null).when(indexPlanService).updateBaseIndex("default", request, false);
        mockMvc.perform(MockMvcRequestBuilders.put("/api/index_plans/base_index")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetIndexStat() throws Exception {
        Mockito.doReturn(null).when(indexPlanService).getStat("default", "abc");
        mockMvc.perform(MockMvcRequestBuilders.get("/api/index_plans/index_stat")
                .contentType(MediaType.APPLICATION_JSON).param("project", "default").param("model_id", "abc")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
