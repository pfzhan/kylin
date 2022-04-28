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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.ComputedColumnConfigRequest;
import io.kyligence.kap.rest.request.JdbcRequest;
import io.kyligence.kap.rest.request.ProjectRequest;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;

public class NProjectControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testSaveProject() throws Exception {
        ProjectRequest request = new ProjectRequest();
        request.setName("test_PROJECT");

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        request.setName("test_project");
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        Assert.assertTrue(StringUtils.contains(result.getResponse().getContentAsString(),
                "The project name \\\"test_PROJECT\\\" already exists. Please rename it."));
    }

    @Test
    public void testUpdateProjectConfig() throws Exception {
        String projectName = "test_update_PROJECT";
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName(projectName);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        ComputedColumnConfigRequest request = new ComputedColumnConfigRequest();
        request.setExposeComputedColumn(false);
        mockMvc.perform(MockMvcRequestBuilders
                .put(String.format(Locale.ROOT, "/api/projects/%s/computed_column_config",
                        projectName.toUpperCase(Locale.ROOT)))
                .content(JsonUtil.writeValueAsString(request)).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance project = projectManager.getProject(projectName);
        Assert.assertEquals("false", project.getOverrideKylinProps().get(ProjectInstance.EXPOSE_COMPUTED_COLUMN_CONF));
    }

    @Test
    public void testUpdateJdbcConfig() throws Exception {
        String projectName = "test_update_jdbc_config";
        ProjectRequest projectRequest = new ProjectRequest();
        projectRequest.setName(projectName);

        mockMvc.perform(MockMvcRequestBuilders.post("/api/projects").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(projectRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        JdbcRequest jdbcRequest = new JdbcRequest();
        mockMvc.perform(MockMvcRequestBuilders
                .put(String.format(Locale.ROOT, "/api/projects/%s/jdbc_config", projectName.toUpperCase(Locale.ROOT)))
                .content(JsonUtil.writeValueAsString(jdbcRequest)).contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testGetDashboardStatistics() throws Exception {
        String project = "gc_test";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/statistics").contentType(MediaType.APPLICATION_JSON)
                .param("project", project).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testIsAccelerating() throws Exception {
        String project = "gc_test";
        mockMvc.perform(MockMvcRequestBuilders.get("/api/projects/acceleration").contentType(MediaType.APPLICATION_JSON)
                .param("project", project).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void testAccelerate() throws Exception {
        String project = "gc_test";
        mockMvc.perform(MockMvcRequestBuilders.put("/api/projects/acceleration").contentType(MediaType.APPLICATION_JSON)
                .param("project", project).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }
}
