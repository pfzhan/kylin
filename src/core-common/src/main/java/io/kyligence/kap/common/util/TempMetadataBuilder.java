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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempMetadataBuilder {
    private static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    private static final String KAP_SPARDER_META_TEST_DATA = "../examples/test_case_data/sparder_localmeta";

    private static final String N_KAP_META_TEST_DATA = "../examples/test_case_data/localmeta_n";
    private static final String TEMP_TEST_METADATA = "../examples/test_metadata";

    private static final Logger logger = LoggerFactory.getLogger(TempMetadataBuilder.class);

    public static String prepareLocalTempMetadata() {
        return prepareLocalTempMetadata(true);
    }

    public static String prepareLocalTempMetadata(boolean debug, String... extraMetaOverlays) {
        ArrayList<String> list = new ArrayList<>();
        list.add(KAP_META_TEST_DATA);

        if (Boolean.parseBoolean(System.getProperty("sparder.enabled"))) {
            list.add(KAP_SPARDER_META_TEST_DATA);
        }

        if (extraMetaOverlays != null) {
            list.addAll(Arrays.asList(extraMetaOverlays));
        }

        return new TempMetadataBuilder(debug, (String[]) list.toArray(new String[list.size()])).build();
    }

    public static String prepareNLocalTempMetadata() {
        return prepareNLocalTempMetadata(false);
    }

    public static String prepareNLocalTempMetadata(boolean debug) {
        return new TempMetadataBuilder(debug, N_KAP_META_TEST_DATA).build();
    }

    public static String prepareNLocalTempMetadata(boolean debug, String... overlay) {
        return new TempMetadataBuilder(debug, overlay).build();
    }

    // ============================================================================

    private String[] metaSrcs = null;
    private boolean debug = false;

    private TempMetadataBuilder(boolean debug, String... metaSrcs) {
        this.metaSrcs = metaSrcs;
        this.debug = debug;
    }

    public String build() {
        try {
            if (debug) {
                logger.info("Preparing local temp metadata");
                for (String metaSrc : metaSrcs) {
                    logger.info("Found one META_TEST_SRC: " + new File(metaSrc).getCanonicalPath());
                }
                logger.info("TEMP_TEST_METADATA=" + new File(TEMP_TEST_METADATA).getCanonicalPath());
            }

            FileUtils.deleteDirectory(new File(TEMP_TEST_METADATA));

            // KAP files will overwrite Kylin files
            for (String metaSrc : metaSrcs) {
                FileUtils.copyDirectory(new File(metaSrc), new File(TEMP_TEST_METADATA));
            }

            appendKylinProperties(TEMP_TEST_METADATA);
            overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, grabDefaultEngineTypes(TEMP_TEST_METADATA), null);

            // It's a hack for engine 99
            List<String> engine99File = new ArrayList<>();
            engine99File.add("ci_left_join_cube.json");
            overrideEngineTypeAndStorageType(TEMP_TEST_METADATA, new Pair<>(98, 99), engine99File);

            // Let CI cube be faster
            overrideDimCapOnCiCubes(TEMP_TEST_METADATA, 1);

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

    private void appendKylinProperties(String tempMetadataDir) throws IOException {
        File propsFile = new File(tempMetadataDir, "kylin.properties");

        // append kylin.properties
        File appendFile = new File(tempMetadataDir, "kylin.properties.append");
        if (appendFile.exists()) {
            if (debug) {
                logger.info("Appending kylin.properties from " + appendFile.getCanonicalPath());
            }

            String appendStr = FileUtils.readFileToString(appendFile, "UTF-8");
            FileUtils.writeStringToFile(propsFile, appendStr, "UTF-8", true);
            appendFile.delete();
        }
    }

    private void overrideEngineTypeAndStorageType(String tempMetadataDir, Pair<Integer, Integer> typePair,
                                                  List<String> includeFiles) throws IOException {
        int engineType = typePair.getFirst();
        int storageType = typePair.getSecond();

        if (debug) {
            logger.info("Override engine type to be " + engineType);
            logger.info("Override storage type to be " + storageType);
        }

        // re-write cube_desc/*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        File[] cubeDescFiles = cubeDescDir.listFiles();
        if (cubeDescFiles == null)
            return;

        for (File f : cubeDescFiles) {
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

    private void overrideDimCapOnCiCubes(String tempMetadataDir, int dimCap) throws IOException {
        if (debug) {
            logger.info("Override dim cap to be " + dimCap);
        }

        // re-write cube_desc/ci_*.json
        File cubeDescDir = new File(tempMetadataDir, "cube_desc");
        File[] cubeDescFiles = cubeDescDir.listFiles();
        if (cubeDescFiles == null)
            return;

        for (File f : cubeDescFiles) {
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

    private Pair<Integer, Integer> grabDefaultEngineTypes(String tempMetadataDir) throws IOException {
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

        if (debug) {
            logger.info("Grap from kylin.properties, engine type is " + engineType);
            logger.info("Grap from kylin.properties, storage type is " + storageType);
        }

        String tmp = System.getProperty("kylin.engine");
        if (!StringUtils.isBlank(tmp)) {
            engineType = Integer.parseInt(tmp.trim());
            if (debug) {
                logger.info("By system property, engine type is " + engineType);
            }
        }
        tmp = System.getProperty("kylin.storage");
        if (!StringUtils.isBlank(tmp)) {
            storageType = Integer.parseInt(tmp.trim());
            if (debug) {
                logger.info("By system property, storage type is " + storageType);
            }
        }

        if (engineType < 0 || storageType < 0)
            throw new IllegalStateException();

        return Pair.newPair(engineType, storageType);
    }

}
