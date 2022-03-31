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
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class ImportModelContextTest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() {
        this.createTestMetadata("src/test/resources/ut_meta/schema_utils/original_project");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testPrepareIdChangedMap() throws IOException, NoSuchMethodException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_create/model_create_model_metadata_2020_11_14_17_11_19_B6A82E50A2B4A7EE5CD606F01045CA84.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);

        NDataModelManager originalDataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "original_project");

        NDataModel originalModel = originalDataModelManager.getDataModelDescByAlias("ssb_model");

        KylinConfig importConfig = getImportConfig(rawResourceMap);
        NDataModelManager targetDataModelManager = NDataModelManager.getInstance(importConfig, "model_create");

        NDataModel targetDataModel = targetDataModelManager.getDataModelDescByAlias("ssb_model_new");

        Method prepareIdChangedMapMethod = ImportModelContext.class.getDeclaredMethod("prepareIdChangedMap",
                NDataModel.class, NDataModel.class);

        Unsafe.changeAccessibleObject(prepareIdChangedMapMethod, true);
        Map<Integer, Integer> idChangedMap = (Map<Integer, Integer>) ReflectionUtils
                .invokeMethod(prepareIdChangedMapMethod, ImportModelContext.class, originalModel, targetDataModel);

        Assert.assertEquals(0, idChangedMap.size());
    }

    @Test
    public void testPrepareIdChangedMapWithTombColumnMeasure() throws IOException, NoSuchMethodException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/model_different_column_measure_id_update/original_project_model_metadata_2020_11_14_15_24_56_4B2101A84E908397A8E711864FC8ADF2.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);

        NDataModelManager originalDataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "original_project");

        NDataModel originalModel = originalDataModelManager.getDataModelDescByAlias("ssb_model");

        KylinConfig importConfig = getImportConfig(rawResourceMap);
        NDataModelManager targetDataModelManager = NDataModelManager.getInstance(importConfig, "original_project");

        NDataModel targetDataModel = targetDataModelManager.getDataModelDescByAlias("ssb_model");

        Method prepareIdChangedMapMethod = ImportModelContext.class.getDeclaredMethod("prepareIdChangedMap",
                NDataModel.class, NDataModel.class);

        Unsafe.changeAccessibleObject(prepareIdChangedMapMethod, true);
        Map<Integer, Integer> idChangedMap = (Map<Integer, Integer>) ReflectionUtils
                .invokeMethod(prepareIdChangedMapMethod, ImportModelContext.class, originalModel, targetDataModel);

        Assert.assertEquals(21, idChangedMap.size());
        Assert.assertEquals(21, new HashSet<>(idChangedMap.values()).size());
    }

    @Test
    public void testTableNameContainsProjectName() throws IOException {
        val file = new File(
                "src/test/resources/ut_meta/schema_utils/table_name_contains_project_name/LINEORDER_model_metadata_2020_11_14_17_11_19_25E6007633A4793DB1790C2E5D3B940A.zip");
        Map<String, RawResource> rawResourceMap = getRawResourceFromUploadFile(file);
        String srcProject = getModelMetadataProjectName(rawResourceMap.keySet());
        Assert.assertEquals("LINEORDER", srcProject);
        val importModelContext = new ImportModelContext("original_project", srcProject, rawResourceMap);

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(importModelContext.getTargetKylinConfig());
        RawResource resource = resourceStore.getResource("/original_project/table/SSB.P_LINEORDER.json");
        Assert.assertNotNull(resource);
    }

    private KylinConfig getImportConfig(Map<String, RawResource> rawResourceMap) {
        KylinConfig importKylinConfig = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        ResourceStore importResourceStore = new InMemResourceStore(importKylinConfig);

        ResourceStore.setRS(importKylinConfig, importResourceStore);

        rawResourceMap.forEach((resPath, raw) -> {
            importResourceStore.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTimestamp(), 0);
        });

        return importKylinConfig;
    }
}