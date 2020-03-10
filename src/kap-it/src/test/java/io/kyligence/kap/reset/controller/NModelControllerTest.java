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
package io.kyligence.kap.reset.controller;

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;

import java.util.Arrays;
import java.util.List;

public class NModelControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testCreateModel() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("DEfault");
        request.setAlias("NMODEL_BASIC");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("Model alias nmodel_basic already exists!"));
    }

    @Test
    public void testBatchSaveModels() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("GC_test");
        request.setAlias("new_MOdel");

        ModelRequest request2 = new ModelRequest();
        request2.setProject("gc_TEst");
        request2.setAlias("new_model");

        List<ModelRequest> modelRequests = Arrays.asList(request, request2);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/batch_save_models").contentType(MediaType.APPLICATION_JSON)
                .param("project", "GC_TEST")
                .content(JsonUtil.writeValueAsString(modelRequests))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("Model alias new_MOdel, new_model are duplicated!"));
    }

    @Test
    public void testValidateModelAlias() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("DEfault");
        request.setUuid("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        request.setAlias("NMODEL_BASIc");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/validate_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andExpect(jsonPath("$.code").value("000"))
                .andExpect(jsonPath("$.data").value("true"));
    }

    @Test
    public void testUpdateModelName() throws Exception {
        ModelUpdateRequest request = new ModelUpdateRequest();
        request.setNewModelName("UT_inner_JOIN_CUBE_PARTIAL");
        request.setProject("deFaUlt");

        mockMvc.perform(MockMvcRequestBuilders.put("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/name")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("Model alias ut_inner_join_cube_partial already exists!"));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setProject("deFaUlt");
        request.setNewModelName("nmodel_BASIC");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/clone")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("Model alias nmodel_basic already exists!"));
    }
}
