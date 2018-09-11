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

package io.kyligence.kap.rest.controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.response.CuboidDescResponse;
import io.kyligence.kap.rest.service.ModelService;
import org.apache.kylin.metadata.model.Segments;
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

public class NModelControllerTest {

    private MockMvc mockMvc;

    @Mock
    private ModelService modelService;

    @InjectMocks
    private NModelController nModelController = Mockito.spy(new NModelController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(nModelController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetModelRelations() throws Exception {
        Mockito.when(modelService.getModelRelations("model1", "default")).thenReturn(mockRelations());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/relations").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelRelations("model1", "default");
        System.out.println(mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testGetModelJson() throws Exception {
        String json = "testjson";
        Mockito.when(modelService.getModelJson("model1", "default")).thenReturn(json);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/json").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getModelJson("model1", "default");
    }

    @Test
    public void testTableIndices() throws Exception {
        Mockito.when(modelService.getTableIndices("model1", "default")).thenReturn(mockCuboidDescs());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/table_indices").contentType(MediaType.APPLICATION_JSON)
                        .param("model", "model1").param("project", "default")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).getTableIndices("model1", "default");
    }

    @Test
    public void testGetCuboids() throws Exception {
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(432323);
        CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
        Mockito.when(modelService.getCuboidById("model1", "default", 432323L)).thenReturn(cuboidDescResponse);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/cuboids").contentType(MediaType.APPLICATION_JSON)
                        .param("id", "432323").param("project", "default").param("model", "model1")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getCuboids(432323L, "default", "model1");
    }

    @Test
    public void testGetSegments() throws Exception {

        Mockito.when(modelService.getSegments("model1", "default", 432323L, 2234L)).thenReturn(mockSegments());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models/segments").contentType(MediaType.APPLICATION_JSON)
                        .param("offset", "0").param("project", "default").param("model", "model1").param("limit", "10")
                        .param("startTime", "432323").param("endTime", "2234")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getSegments("model1", "default", 0, 10, 432323L, 2234L);
    }

    @Test
    public void testGetModels() throws Exception {

        Mockito.when(modelService.getModels("", "default", true)).thenReturn(mockModels());
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/models").contentType(MediaType.APPLICATION_JSON)
                        .param("offset", "0").param("project", "default").param("model", "model1").param("limit", "10")
                        .param("exact", "true").param("table", "")
                        .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(nModelController).getModels("model1", true, "default", "", 0, 10);
    }

    private HashMap<String, Object> mockRelations() {
        HashMap<String, Object> resultMap = new HashMap<>();
        final List<NForestSpanningTree> nSpanningTrees = new ArrayList<>();
        Map<NCuboidDesc, Collection<NCuboidLayout>> cuboids = new HashMap<>();
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(1234);
        List<NCuboidLayout> layouts = new ArrayList<>();
        NCuboidLayout nCuboidLayout = new NCuboidLayout();
        nCuboidLayout.setId(12345);
        layouts.add(nCuboidLayout);
        cuboids.put(cuboidDesc, layouts);
        NForestSpanningTree nSpanningTree = new NForestSpanningTree(cuboids, "test");
        nSpanningTrees.add(nSpanningTree);
        resultMap.put("relations", nSpanningTrees);
        resultMap.put("storage", 222);
        return resultMap;
    }

    private List<CuboidDescResponse> mockCuboidDescs() {
        final List<CuboidDescResponse> nCuboidDescs = new ArrayList<>();
        NCuboidDesc cuboidDesc = new NCuboidDesc();
        cuboidDesc.setId(1234);
        nCuboidDescs.add(new CuboidDescResponse(cuboidDesc));
        return nCuboidDescs;
    }

    private Segments<NDataSegment> mockSegments() {
        final Segments<NDataSegment> nDataSegments = new Segments<NDataSegment>();
        NDataSegment segment = new NDataSegment();
        segment.setId(123);
        segment.setName("seg1");
        nDataSegments.add(segment);
        return nDataSegments;
    }

    private List<NDataModel> mockModels() {
        final List<NDataModel> models = new ArrayList<>();
        NDataModel model = new NDataModel();
        model.setName("seg1");
        models.add(model);
        return models;
    }

}
