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
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.LocalFileMetadataTestCase.MetadataTestCaseHook;
import org.apache.kylin.common.util.LocalFileMetadataTestCase.OverlayMetaHook;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTempMetadata {
    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String KAP_SPARDER_META_TEST_DATA = "../examples/test_case_data/sparder_localmeta";
    public static final String KYLIN_META_TEST_DATA = "../../kylin/examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    private static final Logger logger = LoggerFactory.getLogger(LocalTempMetadata.class);

    private static boolean debug = false;

    public static String prepareLocalTempMetadataWithDebugInfo() {
        try {
            debug = true;
            return prepareLocalTempMetadata();
        } finally {
            debug = false;
        }
    }

    public static String prepareLocalTempMetadata(String... overlayMetadataDirs) {
        return prepareLocalTempMetadata(true, new OverlayMetaHook(overlayMetadataDirs));
    }

    public static String prepareLocalTempMetadata(boolean useDefaultMetadata, MetadataTestCaseHook hook) {
        cleanLocalTempMetadata();
        try {
            if (debug) {
                logger.info("Preparing local temp metadata");
                logger.info("KYLIN_META_TEST_DATA=" + new File(KYLIN_META_TEST_DATA).getCanonicalPath());
                logger.info("KAP_META_TEST_DATA=" + new File(KAP_META_TEST_DATA).getCanonicalPath());
                logger.info("TEMP_TEST_METADATA=" + new File(TEMP_TEST_METADATA).getCanonicalPath());
                logger.info("KAP_SPARDER_META_TEST_DATA=" + new File(KAP_SPARDER_META_TEST_DATA).getCanonicalPath());
            }

            // KAP files will overwrite Kylin files
            if (useDefaultMetadata) {
                FileUtils.copyDirectory(new File(KYLIN_META_TEST_DATA), new File(TEMP_TEST_METADATA));
                FileUtils.copyDirectory(new File(KAP_META_TEST_DATA), new File(TEMP_TEST_METADATA));
            }
            //some test cases may require customize metadata
            if (hook != null) {
                hook.hook();
            }

            if (useDefaultMetadata) {
                appendKylinProperties(TEMP_TEST_METADATA);

                // NOTEICE: MUST ENSURE COVERAGE OF ENGINE COMBINATIONS!!
                
                // build-engine=100, storage-engine=99, for streaming cube and all others (this is the out-of-box default)
                overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, new Pair<>(100, 99), null);
                // build-engine=100, storage-engine=100, for ci_left_join_cube
                overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, new Pair<>(100, 100),
                        Collections.singletonList("ci_left_join_cube.json"));
                // build-engine=98, storage-engine=99, for ci_left_join_cube
                overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, new Pair<>(98, 99),
                        Collections.singletonList("ci_inner_join_cube.json"));
                // build-engine=96, storage-engine=99, for mock_cube
                overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, new Pair<>(96, 99),
                        Collections.singletonList("ad_feeds.json"));

                // CHECK MODIFIED CUBE JSONS
                checkEngineTypeAndStorageType(TEMP_TEST_METADATA, "test_streaming_table_cube.json", 100, 99);
                checkEngineTypeAndStorageType(TEMP_TEST_METADATA, "ci_left_join_cube.json", 100, 100);
                checkEngineTypeAndStorageType(TEMP_TEST_METADATA, "ci_inner_join_cube.json", 98, 99);

                // Let CI cube be faster
                overrideDimCapOnCiCubes(TEMP_TEST_METADATA, 1);
            }

            if (debug) {
                File copy = new File(TEMP_TEST_METADATA + ".debug");
                FileUtils.deleteDirectory(copy);
                FileUtils.copyDirectory(new File(TEMP_TEST_METADATA), copy);
                logger.info("Make copy for debug: " + copy.getCanonicalPath());
            }

            return TEMP_TEST_METADATA;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void cleanLocalTempMetadata() {
        File directory = new File(TEMP_TEST_METADATA);
        LocalFileMetadataTestCase.deleteDirectoryWithErrMsg(directory);
    }

    private static void checkEngineTypeAndStorageType(String tempMetadataDir, String fname, int engineType,
            int storageType) {
        File jsonFile = new File(tempMetadataDir + "/cube_desc", fname);
        String str;
        try {
            str = FileUtils.readFileToString(jsonFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!str.contains("\"engine_type\" : " + engineType))
            throw new RuntimeException();
        if (!str.contains("\"storage_type\" : " + storageType))
            throw new RuntimeException();
    }

    private static void appendKylinProperties(String tempMetadataDir) throws IOException {
        // append kylin.properties
        File appendFile = new File(tempMetadataDir, "kylin.properties.append");
        if (appendFile.exists()) {
            if (debug) {
                logger.info("Appending kylin.properties from " + appendFile.getCanonicalPath());
            }

            String appendStr = FileUtils.readFileToString(appendFile, "UTF-8");
            FileUtils.writeStringToFile(new File(tempMetadataDir, "kylin.properties"), appendStr, "UTF-8", true);
            appendFile.delete();
        }
    }

    private static void overrideEngineTypeAndStorageType(String tempMetadataDir, Pair<Integer, Integer> typePair,
            List<String> includeFiles) throws IOException {
        int engineType = typePair.getFirst();
        int storageType = typePair.getSecond();

        if (debug) {
            logger.info("Override engine type to be " + engineType);
            logger.info("Override storage type to be " + storageType);
        }

        // re-write cube_desc/*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        for (File f : cubeDescDir.listFiles()) {
            if (includeFiles != null && !includeFiles.contains(f.getName())) {
                continue;
            }
            if (debug) {
                logger.info("Process override " + f.getCanonicalPath());
            }
            List<String> lines = FileUtils.readLines(f, "UTF-8");
            for (int i = 0, n = lines.size(); i < n; i++) {
                String l = lines.get(i);
                if (l.contains("\"engine_type\"")) {
                    lines.set(i, "  \"engine_type\" : " + engineType + ",");
                }
                if (l.contains("\"storage_type\"")) {
                    lines.set(i, "  \"storage_type\" : " + storageType + ",");
                }
            }
            FileUtils.writeLines(f, "UTF-8", lines);
        }
    }

    private static void overrideDimCapOnCiCubes(String tempMetadataDir, int dimCap) throws IOException {
        if (debug) {
            logger.info("Override dim cap to be " + dimCap);
        }

        // re-write cube_desc/ci_*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        for (File f : cubeDescDir.listFiles()) {
            if (!f.getName().startsWith("ci_")) {
                continue;
            }
            if (debug) {
                logger.info("Process override " + f.getCanonicalPath());
            }
            List<String> lines = FileUtils.readLines(f, "UTF-8");
            for (int i = 0, n = lines.size(); i < n; i++) {
                String l = lines.get(i);
                if (l.contains("\"dim_cap\"")) {
                    lines.set(i, "        \"dim_cap\" : " + dimCap);
                }
            }
            FileUtils.writeLines(f, "UTF-8", lines);
        }
    }

}
