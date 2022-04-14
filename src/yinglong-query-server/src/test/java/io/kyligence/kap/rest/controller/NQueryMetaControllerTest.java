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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_EARLY_JSON;

import org.apache.kylin.rest.service.QueryService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.rest.controller.v2.NQueryMetaController;

class NQueryMetaControllerTest {
    private MockMvc mockMvc;

    @InjectMocks
    private NQueryMetaController queryMetaController = Mockito.spy(new NQueryMetaController());
    @Mock
    private QueryService queryService;

    @BeforeEach
    private void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(queryMetaController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
    }

    @Test
    void testGetMetadata() throws Exception {
        Mockito.when(queryService.getMetadata("default", "test")).thenReturn(Lists.newArrayList());

        ResultActions result = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables_and_columns")//
                .param("project", "default") //
                .param("cube", "test") //
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_EARLY_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Assertions.assertEquals(0, result.andReturn().getResponse().getContentLength());
    }

    @Test
    void testGetMetadataWhenModelIsNull() throws Exception {
        Mockito.when(queryService.getMetadata("default")).thenReturn(Lists.newArrayList());

        ResultActions result = mockMvc.perform(MockMvcRequestBuilders.get("/api/tables_and_columns") //
                .param("project", "default") //
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_EARLY_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Assertions.assertEquals(0, result.andReturn().getResponse().getContentLength());
    }
}
