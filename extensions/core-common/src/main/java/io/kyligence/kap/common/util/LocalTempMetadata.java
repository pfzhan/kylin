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

package io.kyligence.kap.common.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class LocalTempMetadata {
    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String KYLIN_META_TEST_DATA = "../../kylin/examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    public static String prepareLocalTempMetadata() {
        try {
            FileUtils.deleteDirectory(new File(TEMP_TEST_METADATA));

            // KAP files will overwrite Kylin files
            FileUtils.copyDirectory(new File(KYLIN_META_TEST_DATA), new File(TEMP_TEST_METADATA));
            
            overrideEngineTypeAndStorageType(TEMP_TEST_METADATA);
            
            FileUtils.copyDirectory(new File(KAP_META_TEST_DATA), new File(TEMP_TEST_METADATA));
            
            return TEMP_TEST_METADATA;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void overrideEngineTypeAndStorageType(String tempMetadataDir) throws IOException {
        // append kylin.properties
        String kylinPropsAppend = "\n" //
                + "kylin.storage.provider.100=io.kyligence.kap.storage.parquet.ParquetStorage" + "\n" //
                + "kylin.engine.provider.100=io.kyligence.kap.engine.mr.KapMRBatchCubingEngine" + "\n" //
                + "kylin.engine.default=100" + "\n" //
                + "kylin.storage.default=100" + "\n" //
                + "kylin.metadata.realization-providers=org.apache.kylin.cube.CubeManager,org.apache.kylin.storage.hybrid.HybridManager,io.kyligence.kap.cube.raw.RawTableManager" + "\n"
                + "kylin.metadata.custom-measure-types.percentile=io.kyligence.kap.measure.percentile.PercentileMeasureTypeFactory" + "\n";
        FileUtils.writeStringToFile(new File(tempMetadataDir, "kylin.properties"), kylinPropsAppend, "UTF-8", true);
        
        // re-write cube_desc/*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        for (File f : cubeDescDir.listFiles()) {
            List<String> lines = FileUtils.readLines(f, "UTF-8");
            for (int i = 0, n = lines.size(); i < n; i++) {
                String l = lines.get(i);
                if (l.contains("\"engine_type\"")) {
                    lines.set(i, "  \"engine_type\" : 100,");
                }
                if (l.contains("\"storage_type\"")) {
                    lines.set(i, "  \"storage_type\" : 100,");
                }
            }
            FileUtils.writeLines(f, "UTF-8", lines);
        }
    }
}
