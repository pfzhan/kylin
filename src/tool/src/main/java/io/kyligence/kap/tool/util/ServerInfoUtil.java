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
package io.kyligence.kap.tool.util;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerInfoUtil {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private ServerInfoUtil() {
    }

    private static final String UNKNOWN = "UNKNOWN";

    private static final String COMMIT_SHA1_V15 = "commit_SHA1";
    private static final String COMMIT_SHA1_V13 = "commit.sha1";

    public static String getKylinClientInformation() {
        StringBuilder buf = new StringBuilder();

        String gitCommit = getGitCommitInfo();
        String kylinHome = KylinConfig.getKylinHome();

        buf.append("kylin.home: ").append(kylinHome == null ? UNKNOWN : new File(kylinHome).getAbsolutePath())
                .append("\n");

        // kap versions
        String kapVersion = null;
        try {
            File versionFile = new File(kylinHome, "VERSION");
            if (versionFile.exists()) {
                kapVersion = FileUtils.readFileToString(versionFile).trim();
            }
        } catch (Exception e) {
            logger.error("Failed to get kap.version. ", e);
        }
        buf.append("kap.version:").append(kapVersion == null ? UNKNOWN : kapVersion).append("\n");

        // others
        buf.append("commit:").append(gitCommit).append("\n");
        buf.append("os.name:").append(System.getProperty("os.name")).append("\n");
        buf.append("os.arch:").append(System.getProperty("os.arch")).append("\n");
        buf.append("os.version:").append(System.getProperty("os.version")).append("\n");
        buf.append("java.version:").append(System.getProperty("java.version")).append("\n");
        buf.append("java.vendor:").append(System.getProperty("java.vendor"));

        return buf.toString();
    }

    public static String getGitCommitInfo() {
        try {
            File commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_V15);
            if (!commitFile.exists()) {
                commitFile = new File(KylinConfig.getKylinHome(), COMMIT_SHA1_V13);
            }
            List<String> lines = FileUtils.readLines(commitFile);
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                if (!line.startsWith("#")) {
                    sb.append(line).append(";");
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }
}