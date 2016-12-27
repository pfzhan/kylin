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
import org.apache.kylin.common.util.Pair;

public class LocalTempMetadata {
    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String KYLIN_META_TEST_DATA = "../../kylin/examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    public static String prepareLocalTempMetadata() {
        try {
            FileUtils.deleteDirectory(new File(TEMP_TEST_METADATA));

            // KAP files will overwrite Kylin files
            FileUtils.copyDirectory(new File(KYLIN_META_TEST_DATA), new File(TEMP_TEST_METADATA));
            FileUtils.copyDirectory(new File(KAP_META_TEST_DATA), new File(TEMP_TEST_METADATA));

            appendKylinProperties(TEMP_TEST_METADATA);
            overrideEngineTypeAndStorageType(TEMP_TEST_METADATA);

            return TEMP_TEST_METADATA;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void appendKylinProperties(String tempMetadataDir) throws IOException {
        // append kylin.properties
        File appendFile = new File(tempMetadataDir, "kylin.properties.append");
        if (appendFile.exists()) {
            String appendStr = FileUtils.readFileToString(appendFile, "UTF-8");
            FileUtils.writeStringToFile(new File(tempMetadataDir, "kylin.properties"), appendStr, "UTF-8", true);
            appendFile.delete();
        }
    }

    private static void overrideEngineTypeAndStorageType(String tempMetadataDir) throws IOException {
        Pair<Integer, Integer> pair = grabDefaultEngineTypes(tempMetadataDir);
        int engineType = pair.getFirst();
        int storageType = pair.getSecond();

        // re-write cube_desc/*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        for (File f : cubeDescDir.listFiles()) {
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

    private static Pair<Integer, Integer> grabDefaultEngineTypes(String tempMetadataDir) throws IOException {
        int engineType = -1;
        int storageType = -1;
        
        List<String> lines = FileUtils.readLines(new File(tempMetadataDir, "kylin.properties"), "UTF-8");
        for (String l : lines) {
            if (l.startsWith("kylin.engine.default")) {
                engineType = Integer.parseInt(l.substring(l.lastIndexOf('=') + 1).trim());
            }
            if (l.startsWith("kylin.storage.default")) {
                storageType = Integer.parseInt(l.substring(l.lastIndexOf('=') + 1).trim());
            }
        }
        
        String tmp = System.getProperty("kylin.engine");
        if (tmp != null) {
            engineType = Integer.parseInt(tmp.trim());
        }
        tmp = System.getProperty("kylin.storage");
        if (tmp != null) {
            storageType = Integer.parseInt(tmp.trim());
        }
        
        if (engineType < 0 || storageType < 0)
            throw new IllegalStateException();
        
        return Pair.newPair(engineType, storageType);
    }
}
