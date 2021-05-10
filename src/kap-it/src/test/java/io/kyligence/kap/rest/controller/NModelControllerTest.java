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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import io.kyligence.kap.rest.request.ModelCloneRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.request.ModelUpdateRequest;
import io.kyligence.kap.server.AbstractMVCIntegrationTestCase;

public class NModelControllerTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testCreateModel() throws Exception {
        ModelRequest request = new ModelRequest();
        request.setProject("DEfault");
        request.setAlias("NMODEL_BASIC");
        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models").contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andExpect(jsonPath("$.code").value("999"))
                .andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getMODEL_ALIAS_DUPLICATED(), "nmodel_basic");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
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

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/batch_save_models")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "GC_TEST")
                        .content(JsonUtil.writeValueAsString(modelRequests))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest()).andExpect(jsonPath("$.code").value("999"))
                .andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getMODEL_ALIAS_DUPLICATED(),
                "new_MOdel, new_model");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
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

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.put("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/name")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        String msg = String.format(Locale.ROOT, Message.getInstance().getMODEL_ALIAS_DUPLICATED(),
                "ut_inner_join_cube_partial");
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }

    @Test
    public void testCloneModel() throws Exception {
        ModelCloneRequest request = new ModelCloneRequest();
        request.setProject("deFaUlt");
        request.setNewModelName("nmodel_BASIC");

        MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/89af4ee2-2cdb-4b07-b39e-4c29856309aa/clone")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(jsonPath("$.code").value("999")).andReturn();
        String msg = String.format(Locale.ROOT, Message.getInstance().getMODEL_ALIAS_DUPLICATED(), "nmodel_basic");
        JsonNode jsonNode = JsonUtil.readValueAsTree(result.getResponse().getContentAsString());
        Assert.assertTrue(jsonNode.get("msg").asText().contains(msg));
    }
}
