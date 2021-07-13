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

package io.kyligence.kap.rest.controller.open;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.controller.NMetaStoreController;
import io.kyligence.kap.rest.request.ModelImportRequest;
import io.kyligence.kap.rest.request.ModelPreviewRequest;
import io.kyligence.kap.rest.request.OpenModelPreviewRequest;
import io.kyligence.kap.rest.service.MetaStoreService;
import io.kyligence.kap.rest.service.ModelService;

public class OpenMetaStoreControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    private static String defaultProjectName = "default";

    @Mock
    private MetaStoreService metaStoreService;

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @InjectMocks
    private OpenMetaStoreController openMetaStoreController = Mockito.spy(new OpenMetaStoreController());

    @Mock
    private NMetaStoreController metaStoreController;

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(openMetaStoreController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
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
        mockMvc.perform(MockMvcRequestBuilders.get("/api/metastore/previews/models")
                .contentType(MediaType.APPLICATION_JSON).param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(openMetaStoreController).previewModels(defaultProjectName, Collections.emptyList());
    }

    @Test
    public void testExportModelMetadata() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        Mockito.doReturn(Mockito.mock(NDataModel.class)).when(dataModelManager)
                .getDataModelDescByAlias("warningmodel1");
        Mockito.doReturn(dataModelManager).when(modelService).getDataModelManager(defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataEmptyModelNameException() throws Exception {
        final OpenModelPreviewRequest request = new OpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataModelNotExistException() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        Mockito.doReturn(Mockito.mock(NDataModel.class)).when(dataModelManager).getDataModelDescByAlias(null);
        Mockito.doReturn(dataModelManager).when(modelService).getDataModelManager(defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testExportModelMetadataModelBrokenException() throws Exception {
        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn(true).when(dataModel).isBroken();
        Mockito.doReturn(dataModel).when(dataModelManager).getDataModelDescByAlias("warningmodel1");
        Mockito.doReturn(dataModelManager).when(modelService).getDataModelManager(defaultProjectName);

        final OpenModelPreviewRequest request = mockOpenModelPreviewRequest();
        mockMvc.perform(MockMvcRequestBuilders.post("/api/metastore/backup/models")
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).content(JsonUtil.writeValueAsString(request))
                .param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        Mockito.verify(openMetaStoreController).exportModelMetadata(anyString(), any(OpenModelPreviewRequest.class),
                any(HttpServletResponse.class));
    }

    @Test
    public void testValidateModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/validation/models").file(multipartFile)
                .contentType(MediaType.APPLICATION_JSON_UTF8_VALUE).param("project", defaultProjectName)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).uploadAndCheckModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testImportModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MockMultipartFile multipartFile = new MockMultipartFile("file", "ut_model_matadata.zip", "text/plain",
                new FileInputStream(file));

        final ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        request.setModels(models);
        models.add(new ModelImportRequest.ModelImport("ssb_model", null, ModelImportRequest.ImportType.OVERWRITE));

        MockMultipartFile requestFile = new MockMultipartFile("request", "request", "application/json",
                JsonUtil.writeValueAsString(request).getBytes(StandardCharsets.UTF_8));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/metastore/import/models").file(multipartFile)
                .file(requestFile).contentType(MediaType.MULTIPART_FORM_DATA_VALUE).param("project", "default")
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());

        Mockito.verify(openMetaStoreController).importModelMetadata("default", multipartFile, request);
    }

    private OpenModelPreviewRequest mockOpenModelPreviewRequest() {
        OpenModelPreviewRequest modelPreviewRequest = new OpenModelPreviewRequest();
        List<String> modelNameList = Lists.newArrayList("warningmodel1");
        modelPreviewRequest.setNames(modelNameList);
        modelPreviewRequest.setExportRecommendations(true);
        modelPreviewRequest.setExportOverProps(true);
        return modelPreviewRequest;
    }

    @Test
    public void testConvertToModelPreviewRequest() throws Exception {
        OpenModelPreviewRequest openModelPreviewRequest = new OpenModelPreviewRequest();
        openModelPreviewRequest.setNames(Collections.singletonList("model1"));
        openModelPreviewRequest.setExportOverProps(true);
        openModelPreviewRequest.setExportRecommendations(true);
        openModelPreviewRequest.setExportMultiplePartitionValues(true);

        NDataModelManager dataModelManager = Mockito.mock(NDataModelManager.class);
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.doReturn("1").when(dataModel).getUuid();
        Mockito.doReturn(dataModel).when(dataModelManager).getDataModelDescByAlias(anyString());

        Mockito.doReturn(dataModelManager).when(modelService).getDataModelManager(defaultProjectName);

        ModelPreviewRequest request = Whitebox.invokeMethod(openMetaStoreController, "convertToModelPreviewRequest",
                defaultProjectName, openModelPreviewRequest);

        Assert.assertEquals(Collections.singletonList("1"), request.getIds());
        Assert.assertTrue(request.isExportOverProps());
        Assert.assertTrue(request.isExportRecommendations());
        Assert.assertTrue(request.isExportMultiplePartitionValues());
    }

}
