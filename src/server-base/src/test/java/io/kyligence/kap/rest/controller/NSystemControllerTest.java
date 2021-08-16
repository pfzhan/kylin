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
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.rule.TransactionExceptedException;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.DiagPackageRequest;
import io.kyligence.kap.rest.request.DiagProgressRequest;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.service.SystemService;

public class NSystemControllerTest extends NLocalFileMetadataTestCase {
    private static final String APPLICATION_JSON = HTTP_VND_APACHE_KYLIN_JSON;

    private MockMvc mockMvc;

    @Mock
    private LicenseInfoService licenseInfoService;

    @Mock
    private SystemService systemService;

    @InjectMocks
    private NSystemController nSystemController = Mockito.spy(new NSystemController());

    @Rule
    public TransactionExceptedException thrown = TransactionExceptedException.none();

    @Before
    public void setUp() {
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
        ReflectionTestUtils.setField(nSystemController, "licenseInfoService", licenseInfoService);
        mockMvc = MockMvcBuilders.standaloneSetup(nSystemController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));

    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testTrialLicense() throws Exception {
        final String email = "b@kylin.com";
        LicenseRequest licenseRequest = new LicenseRequest();
        licenseRequest.setUsername("aaa");
        licenseRequest.setEmail(email);
        licenseRequest.setCompany("ccc");
        RemoteLicenseResponse response = new RemoteLicenseResponse();
        response.setSuccess(true);
        response.setData("");
        Mockito.when(licenseInfoService.getTrialLicense(licenseRequest)).thenReturn(response);
        Mockito.when(licenseInfoService.filterEmail(email)).thenReturn(true);
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
        Mockito.doNothing().when(licenseInfoService).updateLicense(string.getBytes(StandardCharsets.UTF_8));
        Mockito.when(licenseInfoService.extractLicenseInfo()).thenReturn(null);
        MockMultipartFile mockMultipartFile = new MockMultipartFile("file", string.getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/system/license/file").file(mockMultipartFile)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).uploadLicense(mockMultipartFile);

        //for /license/content

        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/license/content")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(string))
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).uploadLicense(string2);

    }

    @Test
    public void testRemoteDumpDiagPackage() throws Exception {
        DiagPackageRequest request = new DiagPackageRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemoteDumpDiagPackage(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testGetRemoteDumpDiagPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(),
                Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag/status").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).getRemotePackageStatus(Mockito.anyString(), Mockito.anyString(),
                Mockito.any());
    }

    @Test
    public void testRemoteDownloadPackage() throws Exception {
        Mockito.doNothing().when(nSystemController).downloadFromRemoteHost(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("id", "id").param("host", "ip").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).remoteDownloadPackage(Mockito.anyString(), Mockito.anyString(), Mockito.any(),
                Mockito.any());
    }

    @Test
    public void testRemoteStopPackage() throws Exception {
        Mockito.doAnswer(x -> null).when(nSystemController).generateTaskForRemoteHost(Mockito.any(),
                Mockito.anyString());
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/system/diag").contentType(MediaType.APPLICATION_JSON)
                .param("host", "ip").param("id", "id").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).remoteStopPackage(Mockito.anyString(), Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testUpdateDiagProgress() throws Exception {
        DiagProgressRequest request = new DiagProgressRequest();
        Mockito.doAnswer(x -> null).when(nSystemController).updateDiagProgress(Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.put("/api/system/diag/progress").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON))
                .content(JsonUtil.writeValueAsString(request))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).updateDiagProgress(Mockito.any());
    }

    @Test
    public void testRollEventLog() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.put("/api/system/roll_event_log").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController).rollEventLog();
    }

    @Test
    public void testCheckProjectArg() {
        AclEvaluate sourceValue = nSystemController.getAclEvaluate();
        AclEvaluate mockAclEvaluate = Mockito.mock(AclEvaluate.class);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<ProjectInstance> projectInstanceList = projectManager.listAllProjects();
        projectInstanceList.stream().forEach(projectInstance -> {
            Mockito.doReturn(true).when(mockAclEvaluate).hasProjectAdminPermission(projectInstance);
        });

        nSystemController.setAclEvaluate(mockAclEvaluate);

        List<String> validProjectList = nSystemController.getValidProjects(new String[] {}, false);
        Assert.assertEquals(projectInstanceList.size(), validProjectList.size());

        validProjectList = nSystemController.getValidProjects(new String[] {}, true);
        Assert.assertEquals(projectInstanceList.size(), validProjectList.size());

        validProjectList = nSystemController.getValidProjects(new String[] { "SSB" }, false);
        Assert.assertEquals(1, validProjectList.size());

        validProjectList = nSystemController.getValidProjects(new String[] { "SSB" }, true);
        Assert.assertEquals(0, validProjectList.size());

        validProjectList = nSystemController.getValidProjects(new String[] { "ssb1" }, false);
        Assert.assertEquals(0, validProjectList.size());

        validProjectList = nSystemController.getValidProjects(new String[] { "ssb1" }, true);
        Assert.assertEquals(0, validProjectList.size());

        nSystemController.setAclEvaluate(sourceValue);
    }

    @Test
    public void testGetHostname() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.get("/api/system/host").contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.parseMediaType(APPLICATION_JSON))).andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(nSystemController, Mockito.times(1)).getHostname();
    }

    @Test
    public void testRefreshAll() throws Exception {
        AclEvaluate sourceValue = nSystemController.getAclEvaluate();
        try {
            AclEvaluate mockAclEvaluate = mock(AclEvaluate.class);
            nSystemController.setAclEvaluate(mockAclEvaluate);
            mockMvc.perform(MockMvcRequestBuilders.put("/api/system/capacity/refresh_all")
                    .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());

            Mockito.verify(mockAclEvaluate, Mockito.times(1)).checkIsGlobalAdmin();
            Mockito.verify(nSystemController, Mockito.times(1)).refreshAll(Mockito.any());



        } finally {
            nSystemController.setAclEvaluate(sourceValue);
        }
    }

    @Test
    public void testRefresh() throws Exception {
        AclEvaluate sourceValue = nSystemController.getAclEvaluate();
        try {
            AclEvaluate mockAclEvaluate = mock(AclEvaluate.class);
            nSystemController.setAclEvaluate(mockAclEvaluate);
            mockMvc.perform(MockMvcRequestBuilders.put("/api/system/capacity/refresh")
                    .param("project", "default")
                    .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(APPLICATION_JSON)))
                    .andExpect(MockMvcResultMatchers.status().isOk());
            Mockito.verify(nSystemController, Mockito.times(1)).refresh(Mockito.any());
        } finally {
            nSystemController.setAclEvaluate(sourceValue);
        }
    }
}
