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

import static io.kyligence.kap.common.license.Constants.KE_VERSION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.multipart.MultipartFile;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.MetadataChecker;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.schema.SchemaChangeCheckResult;
import io.kyligence.kap.metadata.model.schema.SchemaNodeType;
import io.kyligence.kap.metadata.recommendation.candidate.JdbcRawRecStore;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.rest.request.ModelImportRequest;
import io.kyligence.kap.rest.response.ModelPreviewResponse;
import lombok.val;
import lombok.var;

public class MetaStoreServiceTest extends ServiceTestBase {
    @Autowired
    private MetaStoreService metaStoreService;

    @Autowired
    private ModelService modelService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    JdbcTemplate jdbcTemplate = null;
    JdbcRawRecStore jdbcRawRecStore = null;
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_meta/metastore_model");
        System.setProperty("HADOOP_USER_NAME", "root");

        try {
            SecurityContextHolder.getContext().setAuthentication(authentication);
            jdbcTemplate = JdbcUtil.getJdbcTemplate(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
        jdbcTemplate.batchUpdate("DROP ALL OBJECTS");
        try {
            jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Test
    public void testGetSimplifiedModel() {
        List<ModelPreviewResponse> modelPreviewResponseList = metaStoreService.getPreviewModels("default");
        Assert.assertEquals(9, modelPreviewResponseList.size());
    }

    @Test
    public void testGetCompressedModelMetadata() throws Exception {
        List<NDataflow> dataflowList = modelService.getDataflowManager(getProject()).listAllDataflows();
        List<NDataModel> dataModelList = dataflowList.stream().filter(df -> !df.checkBrokenWithRelatedInfo())
                .map(NDataflow::getModel).collect(Collectors.toList());
        List<String> modelIdList = dataModelList.stream().map(NDataModel::getId).collect(Collectors.toList());
        ByteArrayOutputStream byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(getProject(),
                modelIdList, false, false);
        Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
        Map<String, RawResource> rawResourceMap = getRawResourceFromZipFile(
                new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Assert.assertEquals(31, rawResourceMap.size());

        // export recommendations
        byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(getProject(), modelIdList, true, false);
        Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
        rawResourceMap = getRawResourceFromZipFile(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Assert.assertTrue(
                rawResourceMap.keySet().stream().anyMatch(path -> path.startsWith("/" + getProject() + "/rec")));

        // export over props
        byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(getProject(), modelIdList, false, true);
        Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
        rawResourceMap = getRawResourceFromZipFile(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

        KylinConfig kylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        rawResourceMap.values().forEach(rs -> {
            long mvcc = -1;
            RawResource originalResource = resourceStore.getResource(rs.getResPath());
            if (originalResource != null) {
                mvcc = originalResource.getMvcc();
            }
            resourceStore.checkAndPutResource(rs.getResPath(), rs.getByteSource(), mvcc);
        });

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, getProject());
        Assert.assertTrue(indexPlanManager.listAllIndexPlans().stream()
                .anyMatch(indexPlan -> !indexPlan.getOverrideProps().isEmpty()));
    }

    @Test
    public void testGetCompressedModelMetadataWithVersionFile() throws Exception {
        String keVersion = System.getProperty(KE_VERSION);
        try {
            System.clearProperty(KE_VERSION);
            List<NDataflow> dataflowList = modelService.getDataflowManager(getProject()).listAllDataflows();
            List<NDataModel> dataModelList = dataflowList.stream().filter(df -> !df.checkBrokenWithRelatedInfo())
                    .map(NDataflow::getModel).collect(Collectors.toList());
            List<String> modelIdList = dataModelList.stream().map(NDataModel::getId).collect(Collectors.toList());
            ByteArrayOutputStream byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(getProject(),
                    modelIdList, false, false);
            Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
            Map<String, RawResource> rawResourceMap = getRawResourceFromZipFile(
                    new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            Assert.assertEquals(31, rawResourceMap.size());

            RawResource rw = rawResourceMap.get(ResourceStore.VERSION_FILE);
            try (InputStream inputStream = rw.getByteSource().openStream()) {
                Assert.assertEquals("unknown", IOUtils.toString(inputStream));
            }

            System.setProperty(KE_VERSION, "4.3.x");

            byteArrayOutputStream = metaStoreService.getCompressedModelMetadata(getProject(), modelIdList, false,
                    false);
            Assert.assertTrue(ArrayUtils.isNotEmpty(byteArrayOutputStream.toByteArray()));
            rawResourceMap = getRawResourceFromZipFile(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            Assert.assertEquals(31, rawResourceMap.size());

            rw = rawResourceMap.get(ResourceStore.VERSION_FILE);
            try (InputStream inputStream = rw.getByteSource().openStream()) {
                Assert.assertEquals("4.3.x", IOUtils.toString(inputStream));
            }
        } finally {
            if (keVersion != null) {
                System.setProperty(KE_VERSION, keVersion);
            }
        }

    }

    @Test
    public void testExportNotExistsModel() throws Exception {
        String notExistsUuid = UUID.randomUUID().toString();
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format("Data Model with name '%s' not found.", notExistsUuid));
        metaStoreService.getCompressedModelMetadata(getProject(), Lists.newArrayList(notExistsUuid), false, false);
    }

    @Test
    public void testExportBrokenModel() throws Exception {
        // broken model id
        String brokenModelId = "8b5a2d39-304f-4a20-a9da-942f461534d8";
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format("Model [%s] is broken, can not export.", brokenModelId));
        metaStoreService.getCompressedModelMetadata(getProject(), Lists.newArrayList(brokenModelId), false, false);

    }

    @Test
    public void testExportEmptyModel() throws Exception {
        // empty model list
        thrown.expect(KylinException.class);
        thrown.expectMessage("You should export one model at least.");
        metaStoreService.getCompressedModelMetadata(getProject(), Lists.newArrayList(), false, false);
    }

    private Map<String, RawResource> getRawResourceFromZipFile(InputStream inputStream) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry zipEntry;
            while ((zipEntry = zipInputStream.getNextEntry()) != null) {
                val bs = ByteStreams.asByteSource(IOUtils.toByteArray(zipInputStream));
                long t = zipEntry.getTime();
                String resPath = StringUtils.prependIfMissing(zipEntry.getName(), "/");
                if (!resPath.startsWith(ResourceStore.METASTORE_UUID_TAG) && !resPath.equals(ResourceStore.VERSION_FILE)
                        && !resPath.endsWith(".json")) {
                    continue;
                }
                rawResourceMap.put(resPath, new RawResource(resPath, bs, t, 0));
            }
            return rawResourceMap;
        }
    }

    @Test
    public void testCheckModelMetadataModelCCUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("model_cc_update");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertEquals(1, modelSchemaChange.getNewItems().size());
        var schemaChange = modelSchemaChange.getNewItems().get(0);
        Assert.assertEquals(SchemaNodeType.MODEL_CC, schemaChange.getType());
        Assert.assertEquals("CC2", schemaChange.getDetail());
        Assert.assertEquals("model_cc_update", schemaChange.getModelAlias());
        Assert.assertEquals("P_LINEORDER.LO_SUPPKEY + 2", schemaChange.getAttributes().get("expression"));
        Assert.assertTrue(schemaChange.isOverwritable());
    }

    @Test
    public void testCheckModelMetadataNoChanges() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("ssb_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(0, modelSchemaChange.getDifferences());
    }

    @Test
    public void testCheckModelMetadataModelAggUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("model_agg_update");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(4, modelSchemaChange.getDifferences());
        Assert.assertEquals(4, modelSchemaChange.getReduceItems().size());
        Assert.assertEquals("10001,40001,60001,70001", modelSchemaChange.getReduceItems().stream()
                .map(SchemaChangeCheckResult.ChangedItem::getDetail).sorted().collect(Collectors.joining(",")));
        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(SchemaChangeCheckResult.BaseItem::isOverwritable));
    }

    @Test
    public void testCheckModelMetadataModelDimConflict() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("conflict_dim_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_DIM && !sc.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_DIM && !sc.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream().anyMatch(
                sc -> sc.getType() == SchemaNodeType.MODEL_JOIN && sc.getDetail().equals("P_LINEORDER-SUPPLIER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream().anyMatch(
                sc -> sc.getType() == SchemaNodeType.MODEL_JOIN && sc.getDetail().equals("P_LINEORDER-CUSTOMER")));
    }

    @Test
    public void testCheckModelMetadataModelJoinConflict() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("conflict_join_condition_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertEquals(1, modelSchemaChange.getUpdateItems().size());
        val schemaUpdate = modelSchemaChange.getUpdateItems().get(0);
        Assert.assertTrue(schemaUpdate.getType() == SchemaNodeType.MODEL_JOIN
                && schemaUpdate.getFirstSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                && schemaUpdate.getFirstSchemaNode().getAttributes().get("join_type").equals("INNER")
                && schemaUpdate.getFirstSchemaNode().getAttributes().get("primary_keys").equals("CUSTOMER.C_CUSTKEY")
                && schemaUpdate.getFirstSchemaNode().getAttributes().get("foreign_keys")
                        .equals("P_LINEORDER.LO_CUSTKEY")
                && schemaUpdate.getFirstSchemaNode().getAttributes().get("non_equal_join_condition").equals("")
                && schemaUpdate.getSecondSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                && schemaUpdate.getSecondSchemaNode().getAttributes().get("join_type").equals("LEFT")
                && schemaUpdate.getSecondSchemaNode().getAttributes().get("primary_keys").equals("CUSTOMER.C_NAME")
                && schemaUpdate.getSecondSchemaNode().getAttributes().get("foreign_keys")
                        .equals("P_LINEORDER.LO_CUSTKEY")
                && schemaUpdate.getSecondSchemaNode().getAttributes().get("non_equal_join_condition").equals(
                        "\"P_LINEORDER\".\"LO_CUSTKEY\" = \"CUSTOMER\".\"C_NAME\" AND CAST(\"P_LINEORDER\".\"LO_CUSTKEY\" AS BIGINT) < CAST(\"CUSTOMER\".\"C_CITY\" AS BIGINT) AND \"P_LINEORDER\".\"LO_CUSTKEY\" >= \"CUSTOMER\".\"C_CUSTKEY\"")
                && !schemaUpdate.isOverwritable() && schemaUpdate.isCreatable());
    }

    @Test
    public void testCheckModelMetadataModelFactConflict() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("conflict_fact_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertTrue(
                modelSchemaChange.getNewItems().stream().anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_FACT
                        && sc.isCreatable() && sc.getDetail().equals("LINEORDER")));
        Assert.assertTrue(modelSchemaChange.getNewItems().stream().anyMatch(
                sc -> sc.getType() == SchemaNodeType.MODEL_JOIN && sc.getDetail().equals("LINEORDER-CUSTOMER")));

        Assert.assertTrue(
                modelSchemaChange.getReduceItems().stream().anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_FACT
                        && sc.isCreatable() && sc.getDetail().equals("P_LINEORDER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream().anyMatch(
                sc -> sc.getType() == SchemaNodeType.MODEL_JOIN && sc.getDetail().equals("P_LINEORDER-CUSTOMER")));
    }

    @Test
    public void testCheckModelMetadataModelColumnUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("model_column_update");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_DIMENSION && sc.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.RULE_BASED_INDEX && sc.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_DIMENSION && sc.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(sc -> sc.getType() == SchemaNodeType.RULE_BASED_INDEX && sc.isOverwritable()));
    }

    @Test
    public void testCheckModelMetadataModelFilterConflict() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("conflict_filter_condition_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream().anyMatch(
                pair -> pair.getType() == SchemaNodeType.MODEL_FILTER && !pair.isOverwritable() && pair.isCreatable()));
    }

    @Test
    public void testCheckModelMetadataModelPartitionConflict() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("conflict_partition_col_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_PARTITION && !pair.isOverwritable()
                        && pair.isCreatable()));
    }

    @Test
    public void testCheckModelMetadataModelMissingTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("missing_table_model");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(9, modelSchemaChange.getDifferences());
        Assert.assertTrue(
                modelSchemaChange.getMissingItems().stream().anyMatch(sc -> sc.getType() == SchemaNodeType.MODEL_TABLE
                        && sc.getDetail().equals("SSB.CUSTOMER_NEW") && !sc.isImportable()));
        Assert.assertFalse(modelSchemaChange.importable());
    }

    @Test
    public void testCheckModelMetadataModelIndex() throws IOException {
        val file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        val metadataCheckResponse = metaStoreService.checkModelMetadata("original_project", multipartFile, null);

        SchemaChangeCheckResult.ModelSchemaChange modelSchemaChange = metadataCheckResponse.getModels()
                .get("model_index");
        Assert.assertNotNull(modelSchemaChange);

        Assert.assertEquals(3, modelSchemaChange.getDifferences());

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .filter(sc -> sc.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(sc -> sc.getDetail().equals("20000000001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(sc -> {
                    String col_orders = String.join(",", ((ArrayList<String>) sc.getAttributes().get("col_orders")));
                    return col_orders.equals(
                            "P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_ORDERDATE,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY");
                }));

        Assert.assertTrue(
                modelSchemaChange.getNewItems().stream().filter(sc -> sc.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                        .filter(sc -> sc.getDetail().equals("20000000001"))
                        .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(sc -> {
                            String col_orders = String.join(",",
                                    ((ArrayList<String>) sc.getAttributes().get("col_orders")));
                            return col_orders.equals(
                                    "P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY,P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_ORDERDATE");
                        }));

        Assert.assertTrue(
                modelSchemaChange.getNewItems().stream().filter(sc -> sc.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                        .filter(sc -> sc.getDetail().equals("20000010001"))
                        .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(sc -> {
                            String col_orders = String.join(",",
                                    ((ArrayList<String>) sc.getAttributes().get("col_orders")));
                            return col_orders.equals("P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY");
                        }));
    }

    @Test
    public void testCheckModelMetadataWithoutMD5Checksum() throws Exception {
        File file = new File("src/test/resources/ut_model_metadata/metastore_model_metadata.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        thrown.expect(KylinException.class);
        thrown.expectMessage("Please verify the metadata file first");
        metaStoreService.checkModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testCheckModelMetadataWithWrongMD5Checksum() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b1.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        thrown.expect(KylinException.class);
        thrown.expectMessage("Please verify the metadata file first");
        metaStoreService.checkModelMetadata("default", multipartFile, null);
    }

    @Test
    public void testImportModelMetadata() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("model_index", "model_index",
                ModelImportRequest.ImportType.OVERWRITE));
        models.add(new ModelImportRequest.ModelImport("model_column_update", "model_column_update",
                ModelImportRequest.ImportType.OVERWRITE));
        models.add(new ModelImportRequest.ModelImport("model_agg_update", "model_agg_update",
                ModelImportRequest.ImportType.OVERWRITE));
        models.add(new ModelImportRequest.ModelImport("conflict_partition_col_model", "conflict_partition_col_model_2",
                ModelImportRequest.ImportType.NEW));
        models.add(new ModelImportRequest.ModelImport("conflict_filter_condition_model", null,
                ModelImportRequest.ImportType.UN_IMPORT));

        request.setModels(models);
        metaStoreService.importModelMetadata("original_project", multipartFile, request);

        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), "original_project");

        IndexPlan indexPlan = indexPlanManager.getIndexPlanByModelAlias("model_index");
        Assert.assertEquals(2, indexPlan.getWhitelistLayouts().size());
        LayoutEntity layout = indexPlan.getCuboidLayout(20000000001L);
        Assert.assertEquals("1,4,5,6,7,8,10,12",
                layout.getColOrder().stream().map(String::valueOf).collect(Collectors.joining(",")));

        layout = indexPlan.getCuboidLayout(20000010001L);
        Assert.assertEquals("4,5", layout.getColOrder().stream().map(String::valueOf).collect(Collectors.joining(",")));

        NDataModel dataModel = dataModelManager.getDataModelDescByAlias("model_column_update");

        Assert.assertTrue(dataModel.getEffectiveDimensions().values().stream()
                .anyMatch(tblColRef -> tblColRef.getName().equals("LO_REVENUE")));
        Assert.assertTrue(dataModel.getEffectiveDimensions().values().stream()
                .anyMatch(tblColRef -> tblColRef.getName().equals("LO_TAX")));
        Assert.assertTrue(dataModel.getEffectiveDimensions().values().stream()
                .noneMatch(tblColRef -> tblColRef.getName().equals("LO_LINENUMBER")));
        Assert.assertTrue(dataModel.getEffectiveMeasures().values().stream()
                .anyMatch(measure -> measure.getName().equals("LO_REVENUE_SUM")));
        Assert.assertTrue(dataModel.getEffectiveMeasures().values().stream()
                .anyMatch(measure -> measure.getName().equals("LO_TAX_SUM")));
        Assert.assertTrue(dataModel.getEffectiveMeasures().values().stream()
                .noneMatch(measure -> measure.getName().equals("LO_ORDERDATE_COUNT")));

        indexPlan = indexPlanManager.getIndexPlanByModelAlias("model_agg_update");
        Assert.assertTrue(indexPlan.getAllLayouts().stream().noneMatch(layoutEntity -> layoutEntity.getId() == 70001L));
        Assert.assertTrue(indexPlan.getAllLayouts().stream().noneMatch(layoutEntity -> layoutEntity.getId() == 60001L));
        Assert.assertTrue(indexPlan.getAllLayouts().stream().noneMatch(layoutEntity -> layoutEntity.getId() == 40001L));
        Assert.assertTrue(indexPlan.getAllLayouts().stream().noneMatch(layoutEntity -> layoutEntity.getId() == 10001L));

        dataModel = dataModelManager.getDataModelDescByAlias("conflict_partition_col_model_2");
        Assert.assertNotNull(dataModel);
        PartitionDesc partitionDesc = dataModel.getPartitionDesc();
        // changed to yyyyMMdd
        Assert.assertEquals("yyyyMMdd", partitionDesc.getPartitionDateFormat());

        dataModel = dataModelManager.getDataModelDescByAlias("conflict_filter_condition_model");
        // still (P_LINEORDER.LO_CUSTKEY <> 1)
        Assert.assertEquals("(P_LINEORDER.LO_CUSTKEY <> 1)", dataModel.getFilterCondition());
    }

    @Test
    public void testImportModelMetadataWithRec() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/target_project_model_metadata_2020_11_21_16_40_43_61D23206229CEB0C078F24AAACADF5DB.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("model_index", "model_index",
                ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), "original_project");
        NDataModel dataModel = dataModelManager.getDataModelDescByAlias("model_index");
        List<RawRecItem> rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(), 1, 10);
        Assert.assertEquals(0, rawRecItems.size());
        metaStoreService.importModelMetadata("original_project", multipartFile, request);

        rawRecItems = jdbcRawRecStore.listAll("original_project", dataModel.getUuid(), 1, 10);
        Assert.assertEquals(3, rawRecItems.size());
    }

    @Test
    public void testImportModelMetadataWithOverProps() throws Exception {
        KylinConfig testConfig = getTestConfig();
        File file = new File(
                "src/test/resources/ut_model_metadata/override_props_project_model_metadata_2020_11_23_17_48_49_40126DF6694B94066ED623AC84291D9E.zip");
        var multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        NDataModelManager dataModelManager = NDataModelManager.getInstance(testConfig, "original_project");
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(testConfig, "original_project");
        NDataModel dataModel = dataModelManager.getDataModelDescByAlias("ssb_model");
        Assert.assertNull(dataModel.getSegmentConfig().getAutoMergeEnabled());
        IndexPlan indexPlan = indexPlanManager.getIndexPlanByModelAlias("ssb_model");
        Assert.assertEquals(1, indexPlan.getOverrideProps().size());
        Assert.assertEquals("2", indexPlan.getOverrideProps().get("kylin.engine.spark-conf.spark.executor.cores"));

        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(
                new ModelImportRequest.ModelImport("ssb_model", "ssb_model", ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);

        metaStoreService.importModelMetadata("original_project", multipartFile, request);

        KylinConfig kylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        dataModelManager = NDataModelManager.getInstance(kylinConfig, "original_project");
        dataModel = dataModelManager.getDataModelDescByAlias("ssb_model");

        Assert.assertEquals(Boolean.TRUE, dataModel.getSegmentConfig().getAutoMergeEnabled());

        indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, "original_project");
        Assert.assertEquals(4, indexPlanManager.getIndexPlanByModelAlias("ssb_model").getOverrideProps().size());
    }

    @Test
    public void testImportModelMetadataWithoutOverProps() throws Exception {
        KylinConfig testConfig = getTestConfig();
        File file = new File(
                "src/test/resources/ut_model_metadata/override_props_project_model_metadata_2020_11_23_18_43_01_8E323F797DDE2989BEBECC747AE40257.zip");
        var multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        NDataModelManager dataModelManager = NDataModelManager.getInstance(testConfig, "original_project");
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(testConfig, "original_project");
        NDataModel dataModel = dataModelManager.getDataModelDescByAlias("ssb_model");
        Assert.assertNull(dataModel.getSegmentConfig().getAutoMergeEnabled());
        IndexPlan indexPlan = indexPlanManager.getIndexPlanByModelAlias("ssb_model");
        Assert.assertEquals(1, indexPlan.getOverrideProps().size());
        Assert.assertEquals("2", indexPlan.getOverrideProps().get("kylin.engine.spark-conf.spark.executor.cores"));

        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(
                new ModelImportRequest.ModelImport("ssb_model", "ssb_model", ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);

        metaStoreService.importModelMetadata("original_project", multipartFile, request);

        KylinConfig kylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        dataModelManager = NDataModelManager.getInstance(kylinConfig, "original_project");
        dataModel = dataModelManager.getDataModelDescByAlias("ssb_model");

        Assert.assertNull(dataModel.getSegmentConfig().getAutoMergeEnabled());
        indexPlan = indexPlanManager.getIndexPlanByModelAlias("ssb_model");
        Assert.assertEquals(1, indexPlan.getOverrideProps().size());
        Assert.assertEquals("2", indexPlan.getOverrideProps().get("kylin.engine.spark-conf.spark.executor.cores"));
    }

    @Test
    public void testImportModelMetadataWithUnOverWritable() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        var multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("conflict_filter_condition_model", null,
                ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                return ((Exception) item).getMessage()
                        .contains("Model [conflict_filter_condition_model]'s ImportType [OVERWRITE] is illegal.");
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        metaStoreService.importModelMetadata("original_project", multipartFile, request);
    }

    @Test
    public void testImportModelMetadataWithUnCreatable() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        var multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("missing_table_model", "missing_table_model_1",
                ModelImportRequest.ImportType.NEW));

        request.setModels(models);

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                return ((Exception) item).getMessage()
                        .contains("Model [missing_table_model_1]'s ImportType [NEW] is illegal.");
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        metaStoreService.importModelMetadata("original_project", multipartFile, request);
    }

    @Test
    public void testImportModelMetadataOverwriteWithUnExistsOriginalModel() throws Exception {
        File file = new File(
                "src/test/resources/ut_model_metadata/metastore_model_metadata_c4a20039c16dfbb5dcc5610c5052d7b3.zip");
        var multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        ModelImportRequest request = new ModelImportRequest();
        List<ModelImportRequest.ModelImport> models = new ArrayList<>();
        models.add(new ModelImportRequest.ModelImport("ssb_model_1", "ssb_model_2",
                ModelImportRequest.ImportType.OVERWRITE));

        request.setModels(models);

        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                return ((Exception) item).getMessage()
                        .contains("Model [ssb_model_1] not exists, Can not overwrite model.");
            }

            @Override
            public void describeTo(Description description) {

            }
        });
        metaStoreService.importModelMetadata("original_project", multipartFile, request);
    }

    @Test
    public void testMetadataChecker() throws IOException {
        File file = new File("src/test/resources/ut_model_metadata/ut_model_matadata.zip");
        val multipartFile = new MockMultipartFile(file.getName(), file.getName(), null, new FileInputStream(file));
        KylinConfig modelConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        MetadataChecker metadataChecker = new MetadataChecker(MetadataStore.createMetadataStore(modelConfig));
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(multipartFile);
        MetadataChecker.VerifyResult verifyResult = metadataChecker
                .verifyModelMetadata(Lists.newArrayList(rawResourceMap.keySet()));
        Assert.assertTrue(verifyResult.isModelMetadataQualified());
        String messageResult = "the uuid file exists : true\n" + "the image file exists : false\n"
                + "the user_group file exists : false\n" + "the user dir exist : false\n"
                + "the acl dir exist : false\n";
        Assert.assertEquals(messageResult, verifyResult.getResultMessage());
    }

    private Map<String, RawResource> getRawResourceFromUploadFile(MultipartFile uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try (ZipInputStream zipInputStream = new ZipInputStream(uploadFile.getInputStream())) {
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

    private String getProject() {
        return "default";
    }
}
