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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.metadata.model.ModelMetadataConflictType.COLUMN_NOT_EXISTED;
import static io.kyligence.kap.metadata.model.ModelMetadataConflictType.DUPLICATE_MODEL_NAME;
import static io.kyligence.kap.metadata.model.ModelMetadataConflictType.INVALID_COLUMN_DATATYPE;
import static io.kyligence.kap.metadata.model.ModelMetadataConflictType.TABLE_NOT_EXISTED;
import static io.kyligence.kap.rest.response.ModelMetadataCheckResponse.ModelMetadataConflict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.response.ModelMetadataCheckResponse;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import io.kyligence.kap.rest.response.NModelDescResponse;
import lombok.val;

public class MetaStoreServiceTest extends CSVSourceTestCase {
    @InjectMocks
    private MetaStoreService metaStoreService = Mockito.spy(new MetaStoreService());

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private ModelService modelService = Mockito.spy(ModelService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        super.setup();
        System.setProperty("HADOOP_USER_NAME", "root");

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(metaStoreService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(metaStoreService, "modelService", modelService);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Test
    public void testGetSimplifiedModel() {
        List<ModelPreviewResponse> modelPreviewResponseList = metaStoreService.getPreviewModels("default");
        Assert.assertTrue(modelPreviewResponseList.size() == 6);
    }

    @Test
    public void testExatractModelMetadata() throws Exception {
        String projectName = "default";
        List<NDataflow> dataflowList = modelService.getDataflowManager(projectName).listAllDataflows(true);
        List<NDataModel> dataModelList = dataflowList.stream().map(NDataflow::getModel).collect(Collectors.toList());
        List<String> modelIdList = dataModelList.stream().map(NDataModel::getId).collect(Collectors.toList());
        ByteArrayOutputStream byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(projectName, modelIdList);
        Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
        Map<String, RawResource> rawResourceMap = getRawResourceFromZipFile(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Assert.assertEquals(rawResourceMap.size(), 24);
    }

    private Map<String, RawResource> getRawResourceFromZipFile(InputStream inputStream) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try(ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }

    @Test
    public void testCheckModelMetadata() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        ModelMetadataCheckResponse metadataCheckResponse = metaStoreService.checkModelMetadata("default", multipartFile);
        List<ModelMetadataConflict> conflictList = metadataCheckResponse.getModelMetadataConflictList();
        Assert.assertEquals(conflictList.size(), 5);

        ModelMetadataConflict duplicateModelName = conflictList.stream()
                .filter(conflictItem -> conflictItem.getConflictItemList().get(0).getElement().contains("nmodel_basic"))
                .findFirst().orElse(null);
        Assert.assertEquals(duplicateModelName.getModelMetadataConflictType(), DUPLICATE_MODEL_NAME);

        ModelMetadataConflict tableNotFoundModel = conflictList.stream()
                .filter(conflictItem -> conflictItem.getConflictItemList().get(0).getElement().contains("tablenotfoundmodel"))
                .findFirst().orElse(null);
        Assert.assertEquals(tableNotFoundModel.getModelMetadataConflictType(), TABLE_NOT_EXISTED);

        ModelMetadataConflict columnNotFoundModel = conflictList.stream()
                .filter(conflictItem -> conflictItem.getConflictItemList().get(0).getElement().contains("srcmodel1"))
                .findFirst().orElse(null);
        Assert.assertEquals(columnNotFoundModel.getModelMetadataConflictType(), COLUMN_NOT_EXISTED);

        ModelMetadataConflict invalidColumnDataType = conflictList.stream()
                .filter(conflictItem -> conflictItem.getConflictItemList().get(0).getElement().contains("columnnotxist"))
                .findFirst().orElse(null);
        Assert.assertEquals(invalidColumnDataType.getModelMetadataConflictType(), INVALID_COLUMN_DATATYPE);

        ModelMetadataConflict correctModel = conflictList.stream()
                .filter(conflictItem -> conflictItem.getConflictItemList().get(0).getElement().contains("warningmodel1"))
                .findFirst().orElse(null);
        Assert.assertNull(correctModel);
    }

    @Test
    public void testCheckModelMetadataWithModelNames() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        ModelMetadataCheckResponse metadataCheckResponse = metaStoreService.checkModelMetadata("default",
                multipartFile, Lists.newArrayList("nmodel_basic", "tablenotfoundmodel"));
        List<ModelMetadataConflict> conflictList = metadataCheckResponse.getModelMetadataConflictList();
        Assert.assertEquals(conflictList.size(), 3);
        Assert.assertEquals(metadataCheckResponse.getModelPreviewResponsesList().size(), 2);
    }

    @Test
    public void testCheckModelMetadataWithModelNamesNotExistException() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        thrown.expect(KylinException.class);
        thrown.expectMessage("The models are not exist. Models name: [notexistmodel].");
        metaStoreService.checkModelMetadata("default",
                multipartFile, Lists.newArrayList("nmodel_basic", "notexistmodel"));
    }

    @Test
    public void testImportModelMetadata() throws Exception {
        String importedModelUuid = "442ce965-e7d8-4096-a9aa-bcdcd93dd187";
        File file = new File(
                "src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        metaStoreService.importModelMetadata("default", multipartFile, Lists.newArrayList(importedModelUuid));
        NModelDescResponse modelDescResponse = modelService.getModelDesc("warningmodel1", "default");
        Assert.assertNotNull(modelDescResponse);
        Assert.assertNotEquals(0L, modelDescResponse.getLastModified());
    }

    @Test
    public void testImportModelMetadataException() throws Exception {
        // duplicated cc name
        String duplicatedCcNameModelUuid = "741ca86a-1f13-46da-a59f-95fb68615e3a";
        File file = new File(
                "src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model 'cc_name_existed' import failed: Computed column 'TEST_KYLIN_FACT.DEAL_AMOUNT' of this model has the same name as computed column in model 'nmodel_basic_inner'.");
        metaStoreService.importModelMetadata("default", multipartFile, Lists.newArrayList(duplicatedCcNameModelUuid));

        // duplicated cc expression
        String duplicatedCcExpressionModelUuid = "841ca86a-1f13-46da-a59f-95fb68615e3a";
        thrown.expect(KylinException.class);
        thrown.expectMessage("Model 'cc_expression_existed' import failed: Computed column 'TEST_KYLIN_FACT.DEAL_AMOUNT_DUPLICATED' of this model has the same expression as model 'nmodel_basic_inner' computed column 'DEAL_AMOUNT'.");
        metaStoreService.importModelMetadata("default", multipartFile, Lists.newArrayList(duplicatedCcExpressionModelUuid));
    }

    @Test
    public void testMetadataChecker() throws IOException {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        KylinConfig modelConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        MetadataChecker metadataChecker = new MetadataChecker(MetadataStore.createMetadataStore(modelConfig));
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(multipartFile);
        MetadataChecker.VerifyResult verifyResult = metadataChecker.verifyModelMetadata(Lists.newArrayList(rawResourceMap.keySet()));
        Assert.assertTrue(verifyResult.isModelMetadataQualified());
        String messageResult = "the uuid file exists : true\n" +
                "the image file exists : false\n" +
                "the user_group file exists : false\n" +
                "the user dir exist : false\n" +
                "the acl dir exist : false\n";
        Assert.assertEquals(messageResult, verifyResult.getResultMessage());
    }

    @Test
    public void testImportModelMetadataWithModelNames() throws IOException {
        String importedModelName = "warningmodel1";
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        MultipartFile multipartFile = new MockMultipartFile(file.getName(), new FileInputStream(file));
        metaStoreService.importModelMetadataWithModelNames("default", multipartFile, Lists.newArrayList(importedModelName));
        NModelDescResponse modelDescResponse = modelService.getModelDesc("warningmodel1", "default");
        Assert.assertNotNull(modelDescResponse);
    }

    private Map<String, RawResource> getRawResourceFromUploadFile(MultipartFile uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try(ZipInputStream zipInputStream = new ZipInputStream(uploadFile.getInputStream())) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }
}
