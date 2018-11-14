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

package io.kyligence.kap.server;

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

public class FavoriteQueryControllerTest extends AbstractMVCIntegrationTestCase {

    private final String PROJECT = "default";

    @Test
    public void testGetFrequencyRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/frequency")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.freqValue").value(0.1));
    }

    @Test
    public void testGetSubmitterRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/submitter")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.users").value(Lists.newArrayList("userA", "userB", "userC")));
    }

    @Test
    public void testGetDurationRule() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/duration")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(true))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.durationValue").value(Lists.newArrayList(5, 8)));
    }

    @Test
    public void testUpdateRules() throws Exception {
        // the request of updating frequency rule
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setEnable(false);
        request.setFreqValue("0.2");

        // update frequency rule
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/frequency")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        // assert if get updated
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/frequency")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(false))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.freqValue").value(0.2));

        // the request of updating submitter rule
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));

        // update submitter rule
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/submitter")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        // assert if get updated
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/submitter")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(false))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.users").value(request.getUsers()))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.groups").value(Lists.newArrayList()));

        // the request of updating submitter rule
        request.setDurationValue(new String[]{"0", "10"});

        // update submitter rule
        mockMvc.perform(MockMvcRequestBuilders.put("/api/query/favorite_queries/rules/duration")
                .contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk());

        // assert if get updated
        mockMvc.perform(MockMvcRequestBuilders.get("/api/query/favorite_queries/rules/duration")
                .contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT)
                .accept(MediaType.parseMediaType("application/vnd.apache.kylin-v2+json")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.enable").value(false))
                .andExpect(MockMvcResultMatchers.jsonPath("$.data.durationValue").value(Lists.newArrayList(0, 10)));
    }
}
