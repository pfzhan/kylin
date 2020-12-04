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

package io.kyligence.kap.metadata.model.schema;

import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_METADATA_FILE_ERROR;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class SchemaUtilTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project");
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    public String getTargetProject() {
        return "original_project";
    }

    public String getTargetModel() {
        return "ssb_model";
    }

    @Test
    public void testConflictDifferentFactTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_fact_table_project/conflict_fact_table_project_model_metadata_2020_11_17_07_38_50_ECFF797F0B088D53A2FA781ABA3BC111.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertEquals(64, modelSchemaChange.getDifferences());

        Assert.assertEquals(18, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(13, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(20, modelSchemaChange.getUpdateItems().size());
        Assert.assertEquals(13, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("LINEORDER")));
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("LINEORDER-CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));

    }

    @Test
    public void testConflictWithDifferentDimTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_dim_table_project/conflict_dim_table_project_model_metadata_2020_11_14_16_20_06_5BCDB43E43D8C8D9E94A90C396CDA23F.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream().anyMatch(
                schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM && !schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream().anyMatch(
                schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM && !schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-SUPPLIER")));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));
    }

    @Test
    public void testConflictWithDifferentFilterCondition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_filter_condition_project/conflict_filter_condition_project_model_metadata_2020_11_13_14_07_15_2AE3E159782BD88DF0445DE9F8B5101C.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_FILTER && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithDifferentJoinCondition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_join_condition_project/conflict_join_condition_project_model_metadata_2020_11_16_02_17_56_FB6573FA0363D0850D6807ABF0BCE060.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(1, modelSchemaChange.getUpdateItems().size());
        Assert.assertEquals(0, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(0, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(0, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_JOIN
                        && pair.getFirstSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                        && pair.getFirstSchemaNode().getAttributes().get("join_type").equals("INNER")
                        && pair.getFirstSchemaNode().getAttributes().get("primary_keys").equals("CUSTOMER.C_CUSTKEY")
                        && pair.getFirstSchemaNode().getAttributes().get("foreign_keys")
                                .equals("P_LINEORDER.LO_CUSTKEY")
                        && pair.getFirstSchemaNode().getAttributes().get("non_equal_join_condition").equals("")
                        && pair.getSecondSchemaNode().getDetail().equals("P_LINEORDER-CUSTOMER")
                        && pair.getSecondSchemaNode().getAttributes().get("join_type").equals("LEFT")
                        && pair.getSecondSchemaNode().getAttributes().get("primary_keys").equals("CUSTOMER.C_CUSTKEY")
                        && pair.getSecondSchemaNode().getAttributes().get("foreign_keys")
                                .equals("P_LINEORDER.LO_CUSTKEY")
                        && pair.getSecondSchemaNode().getAttributes().get("non_equal_join_condition").equals("")
                        && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithDifferentPartition() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/conflict_partition_col_project/conflict_partition_col_project_model_metadata_2020_11_14_17_09_51_98DA15B726CE71B8FACA563708B8F4E5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_PARTITION && !pair.isOverwritable()));
    }

    @Test
    public void testConflictWithModelColumnUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_column_update/model_column_update_model_metadata_2020_11_14_17_11_19_77E61B40A0A3C2D1E1DE54E6982C98F5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION
                        && schemaChange.isOverwritable()));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX
                        && schemaChange.isOverwritable()));

    }

    @Test
    public void testConflictWithModelCCUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_update/model_cc_update_model_metadata_2020_11_14_17_15_29_FAAAF2F8FA213F2888D3380720C28B4D.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(2, modelSchemaChange.getDifferences());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_CC
                        && schemaChange.isOverwritable() && schemaChange.getDetail().equals("CC2")
                        && schemaChange.getAttributes().get("expression").equals("P_LINEORDER.LO_SUPPKEY + 1")));

        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.MODEL_CC
                        && pair.getFirstSchemaNode().getDetail().equals("CC1")
                        && pair.getFirstSchemaNode().getAttributes().get("expression")
                                .equals("P_LINEORDER.LO_CUSTKEY + 1")
                        && !pair.isImportable() && pair.getSecondSchemaNode().getDetail().equals("CC1")
                        && pair.getSecondSchemaNode().getAttributes().get("expression")
                                .equals("P_LINEORDER.LO_CUSTKEY + 2")));

    }

    @Test
    public void testConflictWithModelAggUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_agg_update/model_agg_update_model_metadata_2020_11_14_17_16_54_CF515DB4597CBE5DAE244A6EBF5FF5F3.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(4, modelSchemaChange.getDifferences());

        Assert.assertEquals(4, modelSchemaChange.getReduceItems().size());
        Assert.assertEquals("10001,40001,60001,70001", modelSchemaChange.getReduceItems().stream()
                .map(SchemaChangeCheckResult.ChangedItem::getDetail).sorted().collect(Collectors.joining(",")));
    }

    @Test
    public void testConflictWithModelTableIndexUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_index_update/model_index_project_model_metadata_2020_11_16_02_37_33_1B8D602879F14297E132978D784C46EA.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(3, modelSchemaChange.getDifferences());

        Assert.assertEquals(0, modelSchemaChange.getMissingItems().size());
        Assert.assertEquals(1, modelSchemaChange.getReduceItems().size());

        Assert.assertTrue(modelSchemaChange.getReduceItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000000001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals(
                            "P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_ORDERDATE,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY");
                }));

        Assert.assertEquals(2, modelSchemaChange.getNewItems().size());
        Assert.assertEquals(0, modelSchemaChange.getUpdateItems().size());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000000001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals(
                            "P_LINEORDER.LO_LINENUMBER,P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY,P_LINEORDER.LO_PARTKEY,P_LINEORDER.LO_ORDERKEY,P_LINEORDER.LO_CUSTKEY,P_LINEORDER.LO_DISCOUNT,P_LINEORDER.LO_ORDERDATE");
                }));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX)
                .filter(schemaChange -> schemaChange.getDetail().equals("20000010001"))
                .filter(SchemaChangeCheckResult.BaseItem::isOverwritable).anyMatch(schemaChange -> {
                    String col_orders = String.join(",",
                            ((ArrayList<String>) schemaChange.getAttributes().get("col_orders")));
                    return col_orders.equals("P_LINEORDER.LO_SUPPKEY,P_LINEORDER.LO_QUANTITY");
                }));
    }

    @Test
    public void testModelWithDifferentColumnMeasureIdUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_measure_id_update/original_project_model_metadata_2020_11_14_15_24_56_4B2101A84E908397A8E711864FC8ADF2.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(0, modelSchemaChange.getDifferences());
    }

    @Test
    public void testConflictModelWithDifferentColumnDataTypeUpdate() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_datatype_update/original_project_datatype_model_metadata_2020_11_14_15_24_56_9BCCCB5D08F218EC86DD9D850017F5F5.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(pair -> pair.getType() == SchemaNodeType.TABLE_COLUMN && !pair.isImportable()
                        && pair.getFirstDetail().equals("SSB.CUSTOMER.C_CUSTKEY")
                        && pair.getFirstSchemaNode().getAttributes().get("datatype").equals("integer")
                        && pair.getSecondSchemaNode().getAttributes().get("datatype").equals("varchar(4096)")));
    }

    @Test
    public void testConflictModelWithMissingTable() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_missing_table_update/model_table_missing_update_model_metadata_2020_11_16_02_37_33_3182D4A7694DA64E3D725C140CF80A47.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());

        Assert.assertEquals(9, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getMissingItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_TABLE
                        && !schemaChange.isImportable() && schemaChange.getDetail().equals("SSB.CUSTOMER_NEW")));
    }

    @Test
    public void testNewModel() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_create/model_create_model_metadata_2020_11_14_17_11_19_B6A82E50A2B4A7EE5CD606F01045CA84.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get("ssb_model_new");

        Assert.assertEquals(35, modelSchemaChange.getDifferences());
        Assert.assertEquals(35, modelSchemaChange.getNewItems().size());
        Assert.assertTrue(modelSchemaChange.creatable());
        Assert.assertTrue(modelSchemaChange.importable());
        Assert.assertFalse(modelSchemaChange.overwritable());

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FACT
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIM
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_JOIN
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER-CUSTOMER")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_FILTER
                        && schemaChange.isCreatable()
                        && schemaChange.getDetail().equals("(P_LINEORDER.LO_CUSTKEY <> 1)")));

        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_PARTITION
                        && schemaChange.isCreatable() && schemaChange.getDetail().equals("P_LINEORDER.LO_ORDERDATE")));

        Assert.assertEquals(1, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_CC).count());

        Assert.assertEquals(8, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_DIMENSION).count());

        Assert.assertEquals(12, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.MODEL_MEASURE).count());

        Assert.assertEquals(8, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.RULE_BASED_INDEX).count());

        Assert.assertEquals(1, modelSchemaChange.getNewItems().stream()
                .filter(schemaChange -> schemaChange.getType() == SchemaNodeType.WHITE_LIST_INDEX).count());
    }

    @Test
    public void testExceptions() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_exception/model_exception_model_metadata_2020_11_19_15_13_11_4B823934CF76C0A124244456539C9296.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Import model failed, detail is:\n" + "Model [none_exists_column] is broken, can not export.\n"
                        + "Model [illegal_index] is broken, can not export.\n"
                        + "Model [none_exists_table] is broken, can not export.");
        new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
    }

    static Map<String, RawResource> getRawResourceFromUploadFile(File uploadFile) throws IOException {
        Map<String, RawResource> rawResourceMap = Maps.newHashMap();
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(uploadFile))) {
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

    static String getModelMetadataProjectName(Set<String> rawResourceList) {
        String anyPath = rawResourceList.stream().filter(
                resourcePath -> resourcePath.indexOf(File.separator) != resourcePath.lastIndexOf(File.separator))
                .findAny().orElse(null);
        if (StringUtils.isBlank(anyPath)) {
            throw new KylinException(MODEL_METADATA_FILE_ERROR, MsgPicker.getMsg().getMODEL_METADATA_PACKAGE_INVALID());
        }
        return anyPath.split(File.separator)[1];
    }
}