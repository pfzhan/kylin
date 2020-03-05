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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ComputedColumnConfigRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;

public class NProjectControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testSaveProject() throws Exception {
        ProjectRequest request = new ProjectRequest();
        request.setName("test_PROJECT");
        request.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        request.setName("test_project");
        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999"))
                .andExpect(jsonPath("$.msg").value("The project named 'test_PROJECT' already exists."));
    }

    @Test
    public void testUpdateProjectConfig() throws Exception {
        String projectName = "test_update_PROJECT";
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName(projectName);
        projectRequest.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ComputedColumnConfigRequest request = new ComputedColumnConfigRequest();
        request.setExposeComputedColumn(false);
        mockMvc.perform(MockMvcRequestBuilders
                .put(String.format("/api/projects/%s/computed_column_config", projectName.toUpperCase()))
                .content(JsonUtil.writeValueAsString(request)).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance project = projectManager.getProject(projectName);
        Assert.assertEquals("false", project.getOverrideKylinProps().get(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF));
    }
}
