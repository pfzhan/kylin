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
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;

@RequiredArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class TempMetadataBuilder {

    public static final String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static final String SPARK_PROJECT_KAP_META_TEST_DATA = "../../examples/test_case_data/localmeta";
    public static final String TEMP_TEST_METADATA = "../examples/test_data/"
            + ProcessUtils.getCurrentId(System.currentTimeMillis() + "_"
            + UUID.randomUUID().toString());

    public static String prepareLocalTempMetadata() {
        return prepareLocalTempMetadata(Lists.newArrayList());
    }

    public static String prepareLocalTempMetadata(List<String> overlay) {
        return createBuilder(overlay).build();
    }

    public static TempMetadataBuilder createBuilder(List<String> overlay) {
        overlay.add(0, KAP_META_TEST_DATA);
        if (!new File(overlay.get(0)).exists()) {
            overlay.set(0, "../" + overlay.get(0));
        }
        return new TempMetadataBuilder(overlay);
    }

    // ============================================================================
    private final List<String> metaSrcs;
    private String project;
    private boolean onlyProps = false;

    public String build() {
        try {
            String tempTestMetadataDir = TEMP_TEST_METADATA;
            FileUtils.deleteQuietly(new File(tempTestMetadataDir));

            for (String metaSrc : metaSrcs) {
                if (onlyProps) {
                    FileUtils.copyFile(new File(metaSrc, "kylin.properties"),
                            new File(tempTestMetadataDir, "kylin.properties"));
                } else {
                    FileUtils.copyDirectory(new File(metaSrc),
                            new File(tempTestMetadataDir), pathname -> {
                        if (pathname.isDirectory()) {
                            return true;
                        }
                        try {
                            val name = pathname.getCanonicalPath();
                            return project == null || name.contains(project) || name.endsWith(".properties");
                        } catch (IOException ignore) {
                            // ignore it
                        }
                        return false;
                    }, true);
                }
            }

            appendKylinProperties(tempTestMetadataDir);
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
            String appendStr = FileUtils.readFileToString(appendFile, Charsets.UTF_8);
            FileUtils.writeStringToFile(propsFile, appendStr, Charsets.UTF_8, true);
            FileUtils.deleteQuietly(appendFile);
        }
    }

}
