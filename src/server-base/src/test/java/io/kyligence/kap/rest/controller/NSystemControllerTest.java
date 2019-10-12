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

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.service.SystemService;

public class NSystemControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = "application/vnd.apache.kylin-v2+json";

    private MockMvc mockMvc;

    @Mock
    private LicenseInfoService licenseInfoService;

    @Mock
    private SystemService systemService;

    @InjectMocks
    private NSystemController nSystemController = Mockito.spy(new NSystemController());

    @Before
    public void setUp() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(nSystemController, "licenseInfoService", licenseInfoService);
        mockMvc = MockMvcBuilders.standaloneSetup(nSystemController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testTrialLicense() throws Exception {
        LicenseRequest licenseRequest = new LicenseRequest();
        licenseRequest.setUsername("a");
        licenseRequest.setEmail("b");
        licenseRequest.setCompany("c");
        RemoteLicenseResponse response = new RemoteLicenseResponse();
        response.setSuccess(true);
        response.setData("");
        Mockito.when(licenseInfoService.getTrialLicense(licenseRequest)).thenReturn(response);
        Mockito.doNothing().when(licenseInfoService).updateLicense(response.getData());
        Mockito.when(licenseInfoService.extractLicenseInfo()).thenReturn(null);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/license/trial") //
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(licenseRequest))// //
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nSystemController).trialLicense(licenseRequest);

    }

    @Test
    public void testUploadLicense() throws Exception {
        //for /license/file
        String string = "kkkkkk";
        String string2 = "\"kkkkkk\"";
        Mockito.doNothing().when(licenseInfoService).updateLicense(string.getBytes());
        Mockito.when(licenseInfoService.extractLicenseInfo()).thenReturn(null);
        MockMultipartFile mockMultipartFile = new MockMultipartFile("file", string.getBytes("UTF-8"));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/system/license/file").file(mockMultipartFile)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).uploadLicense(mockMultipartFile);

        //for /license/content

        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/license/content")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(string))
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).uploadLicense(string2);

    }

}
