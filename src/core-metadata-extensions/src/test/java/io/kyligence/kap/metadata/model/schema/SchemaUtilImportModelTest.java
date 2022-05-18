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

import static io.kyligence.kap.metadata.model.schema.SchemaUtilTest.getModelMetadataProjectName;
import static io.kyligence.kap.metadata.model.schema.SchemaUtilTest.getRawResourceFromUploadFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class SchemaUtilImportModelTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project_model_import");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public String getTargetModel() {
        return "ssb_mdl";
    }

    @Test
    public void testDifferentDatabaseSameModelStruct() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_import_diff_db/34110_2_model_metadata_2022_05_03_21_43_58_620D9E784006043A4B6E9E5E36C9B06D.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String targetProject = "original_project_model_import";
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(targetProject, srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(targetProject, KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(newItem -> newItem.getType().equals(SchemaNodeType.MODEL_FACT)
                        && newItem.getDetail().equals("SSB4X.P_LINEORDER"))
                && modelSchemaChange.getReduceItems().stream()
                        .anyMatch(reduceItem -> reduceItem.getType().equals(SchemaNodeType.MODEL_FACT)
                                && reduceItem.getDetail().equals("SSB.P_LINEORDER"))
                && modelSchemaChange.getMissingItems().stream()
                        .anyMatch(missingItem -> missingItem.getType().equals(SchemaNodeType.MODEL_TABLE)
                                && missingItem.getDetail().equals("SSB4X.P_LINEORDER")));
    }

    @Test
    public void testCaseSensitiveCCExpression() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_import_casesensitive_cc_expr/35930_2_model_metadata_2022_05_09_16_53_38_F219D2C04B1792E7DB87B634DE058AD8.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String targetProject = "original_project_model_import_2";
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(targetProject, srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(targetProject, KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertEquals(1, modelSchemaChange.getDifferences());
        Assert.assertTrue(modelSchemaChange.getUpdateItems().stream()
                .anyMatch(updatedItem -> updatedItem.getType().equals(SchemaNodeType.MODEL_CC)
                        && updatedItem.getFirstAttributes().get("expression")
                                .equals("'ABC' || LINEORDER.LO_SHIPMODE || 'aBC' || LINEORDER.LO_ORDERPRIOTITY")
                        && updatedItem.getSecondAttributes().get("expression")
                                .equals("'aBc' || LINEORDER.LO_SHIPMODE || 'Abc' || LINEORDER.LO_ORDERPRIOTITY")));
    }

}
