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

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

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
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.rest.request.ModelImportRequest;
import io.kyligence.kap.rest.request.ModelPreviewRequest;
import io.kyligence.kap.rest.service.MetaStoreService;

public class NMetaStoreControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Mock
    private MetaStoreService metaStoreService;

    @InjectMocks
    private NMetaStoreController nModelController = Mockito.spy(new NMetaStoreController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(nModelController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testPreviewModels() throws Exception {
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/metastore/previews/models")
                        .contentType(MediaType.APPLICATION_JSON).param("project", "default")
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(nModelController).previewModels("default", Collections.emptyList());
    }

    @Test
    public void testExportModelMetadata() throws Exception {
        Mockito.doNothing().when(nModelController).exportModelMetadata(Mockito.anyString(),
                Mockito.any(ModelPreviewRequest.class), Mockito.any(HttpServletResponse.class));

        final ModelPreviewRequest request = mockModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", "default").accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).exportModelMetadata(Mockito.anyString(),
                Mockito.any(ModelPreviewRequest.class), Mockito.any(HttpServletResponse.class));
    }

    @Test
    public void testUploadAndCheckModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        SchemaChangeCheckResult schemaChangeCheckResult = new SchemaChangeCheckResult();
        Mockito.when(metaStoreService.checkModelMetadata("default", multipartFile, null))
                .thenReturn(schemaChangeCheckResult);

        final ModelPreviewRequest request = mockModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/validation/models").file(multipartFile)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).uploadAndCheckModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testImportModelMetadata() throws Throwable {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        final ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        request.setModels(models);
        models.add(new ModelImportRequest.ModelImport("ssb_model", null, ModelImportRequest.ImportType.OVERWRITE));

        MockMultipartFile requestFile = new MockMultipartFile("request", "request", "application/json",
                JsonUtil.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/models").file(multipartFile).file(requestFile)
                .contentType(MediaType.MULTIPART_FORM_DATA_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(nModelController).importModelMetadata("default", multipartFile, request);
    }

    private ModelPreviewRequest mockModelPreviewRequest() {
        ModelPreviewRequest modelPreviewRequest = new ModelPreviewRequest();
        List<String> modelIdList = Lists.newArrayList("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        modelPreviewRequest.setIds(modelIdList);
        modelPreviewRequest.setExportRecommendations(true);
        modelPreviewRequest.setExportOverProps(true);
        return modelPreviewRequest;
    }
}
