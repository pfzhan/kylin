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

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempMetadataBuilder {

    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String SPARK_PROJECT_KAP_META_TEST_DATA = "../../examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_data/"
            + ProcessUtil.getProcessId(System.currentTimeMillis() + "");

    private static final Logger logger = LoggerFactory.getLogger(TempMetadataBuilder.class);

    public static String prepareLocalTempMetadata() {
        return prepareLocalTempMetadata(false);
    }

    public static String prepareLocalTempMetadata(boolean debug) {
        // for spark-project
        if (!new File(KAP_META_TEST_DATA).exists()) {
            return new TempMetadataBuilder(debug, SPARK_PROJECT_KAP_META_TEST_DATA).build();
        }
        return new TempMetadataBuilder(debug, KAP_META_TEST_DATA).build();
    }

    public static String prepareLocalTempMetadata(boolean debug, String... overlay) {
        String[] nOverlay = new String[overlay.length + 1];
        nOverlay[0] = KAP_META_TEST_DATA;
        System.arraycopy(overlay, 0, nOverlay, 1, overlay.length);
        // for spark-project
        if (!new File(nOverlay[0]).exists()) {
            nOverlay[0] = "../" + nOverlay[0];
        }
        return new TempMetadataBuilder(debug, nOverlay).build();
    }

    // ============================================================================

    private final String[] metaSrcs;
    private final boolean debug;

    private TempMetadataBuilder(boolean debug, String... metaSrcs) {
        this.metaSrcs = metaSrcs;
        this.debug = debug;
    }

    public String build() {

        if ("true".equals(System.getProperty("skipMetaPrep"))) {
            return TEMP_TEST_METADATA;
        }

        try {
            String tempTestMetadataDir = TEMP_TEST_METADATA;
            if (debug) {
                logger.info("Preparing local temp metadata");
                for (String metaSrc : metaSrcs) {
                    logger.info("Found one META_TEST_SRC: {}", new File(metaSrc).getCanonicalPath());
                }
                logger.info("TEMP_TEST_METADATA={}", new File(tempTestMetadataDir).getCanonicalPath());
            }

            FileUtils.deleteQuietly(new File(tempTestMetadataDir));

            // KAP files will overwrite Kylin files
            for (String metaSrc : metaSrcs) {
                FileUtils.copyDirectory(new File(metaSrc), new File(tempTestMetadataDir));
            }

            appendKylinProperties(tempTestMetadataDir);

            if (debug) {
                File copy = new File(tempTestMetadataDir + ".debug");
                FileUtils.deleteDirectory(copy);
                FileUtils.copyDirectory(new File(tempTestMetadataDir), copy);
                logger.info("Make copy for debug: {}", copy.getCanonicalPath());
            }

            return tempTestMetadataDir;
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
                logger.info("Appending kylin.properties from {}", appendFile.getCanonicalPath());
            }

            String appendStr = FileUtils.readFileToString(appendFile, Charsets.UTF_8);
            FileUtils.writeStringToFile(propsFile, appendStr, Charsets.UTF_8, true);
            FileUtils.deleteQuietly(appendFile);
        }
    }

}
