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

public class SchemaUtilCCLineBreakTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project_cc_linebreak");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    public String getTargetProject() {
        return "original_project_cc_linebreak";
    }

    public String getTargetModel() {
        return "COR_KYL_MOD_PNL_RISK_RESULTS";
    }

    @Test
    public void testDifferentLineBreakInCC() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_cc_different_line_breaks/COR_KYL_MOD_PNL_RISK_RESULTS_b5c17c85a59e74f18a2b7f18c7575c16.zip");
        String expr = "(CASE WHEN PNL_RISK_RESULTS_VD.MEASURE = FX_FAMILY_ENRICHED.ATTRIBUTE_ID AND FX_FAMILY_ENRICHED.GCRS_PRODUCT_CODE = FX_FAMILY_ENRICHED.PRODUCTHIERARCHY_GCRS_CODEID \n"
                + "THEN 1 ELSE 0 END)";
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        val importModelContext = new ImportModelContext(getTargetProject(), srcProject, rawResourceMap);
        val difference = SchemaUtil.diff(getTargetProject(), KylinConfig.getInstanceFromEnv(),
                importModelContext.getTargetKylinConfig());

        val schemaChangeResponse = ModelImportChecker.check(difference, importModelContext);
        Assert.assertFalse(schemaChangeResponse.getModels().isEmpty());

        val modelSchemaChange = schemaChangeResponse.getModels().get(getTargetModel());
        Assert.assertTrue(modelSchemaChange.getNewItems().stream()
                .anyMatch(newItem -> newItem.getType().equals(SchemaNodeType.MODEL_CC)
                        && newItem.getAttributes().get("expression").equals(expr)));
    }

}
