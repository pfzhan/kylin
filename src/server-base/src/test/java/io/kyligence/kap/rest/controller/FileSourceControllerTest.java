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

import static io.kyligence.kap.common.http.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.CSVRequest;
import io.kyligence.kap.rest.response.LoadTableResponse;
import io.kyligence.kap.rest.service.FileSourceService;

public class FileSourceControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Mock
    private FileSourceService fileSourceService;

    @InjectMocks
    private FileSourceController fileSourceController = Mockito.spy(new FileSourceController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(fileSourceController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private CSVRequest mockCsvRequest() {
        CSVRequest csvRequest = new CSVRequest();
        csvRequest.setCredential("mockCredential");
        csvRequest.setType("mockType");
        csvRequest.setProject("default");
        return csvRequest;
    }

    @Test
    public void testVerifyCredential() throws Exception {
        Mockito.when(fileSourceService.verifyCredential(mockCsvRequest())).thenReturn(true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/source/verify").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockCsvRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(fileSourceController).verifyCredential(mockCsvRequest());
    }

    @Test
    public void testValidateSql() throws Exception {
        Mockito.when(fileSourceService.validateSql(mockCsvRequest().getDdl())).thenReturn(true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/source/validate").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockCsvRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(fileSourceController).validateSql(mockCsvRequest());
    }

    @Test
    public void testSaveCSV() throws Exception {
        String mode = "guide";
        LoadTableResponse loadTableResponse = mock(LoadTableResponse.class);
        Mockito.when(fileSourceService.saveCSV(mode, mockCsvRequest())).thenReturn(loadTableResponse);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/source/csv").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockCsvRequest())).param("mode", "guide")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(fileSourceController).saveCsv("guide", mockCsvRequest());
    }

    @Test
    public void testCsvSamples() throws Exception {
        String[][] samples = new String[][] {};
        Mockito.when(fileSourceService.csvSamples(mockCsvRequest())).thenReturn(samples);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/source/csv/samples").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockCsvRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(fileSourceController).csvSamples(mockCsvRequest());
    }

    @Test
    public void testCsvSchema() throws Exception {
        List<String> schema = Lists.newArrayList();
        Mockito.when(fileSourceService.csvSchema(mockCsvRequest())).thenReturn(schema);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/source/csv/schema").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(mockCsvRequest()))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(fileSourceController).csvSchema(mockCsvRequest());
    }

}
