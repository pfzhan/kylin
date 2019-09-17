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
package io.kyligence.kap.tool;

import com.google.common.collect.Sets;
import io.kyligence.kap.tool.util.ToolUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

public class ConfTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Set<String> KYLIN_BIN_INCLUSION = Sets.newHashSet("kylin.sh");

    private ConfTool() {
    }

    public static void extractConf(File exportDir) {
        try {
            File confDir = new File(ToolUtil.getConfFolder());
            if (confDir.exists()) {
                FileUtils.copyDirectoryToDirectory(confDir, exportDir);
            } else {
                logger.error("Can not find the /conf dir: {}!", confDir.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.warn("Failed to copy /conf, ", e);
        }
    }

    public static void extractHadoopConf(File exportDir) {
        try {
            File hadoopConfDir = new File(ToolUtil.getHadoopConfFolder());
            if (hadoopConfDir.exists()) {
                FileUtils.copyDirectoryToDirectory(hadoopConfDir, exportDir);
            } else {
                logger.error("Can not find the hadoop_conf: {}!", hadoopConfDir.getAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("Failed to copy /hadoop_conf, ", e);
        }
    }

    public static void extractBin(File exportDir) {
        File destBinDir = new File(exportDir, "bin");

        try {
            FileUtils.forceMkdir(destBinDir);

            File srcBinDir = new File(ToolUtil.getBinFolder());
            if (srcBinDir.exists()) {
                File[] binFiles = srcBinDir.listFiles();
                if (null != binFiles) {
                    for (File binFile : binFiles) {
                        String binFileName = binFile.getName();
                        if (KYLIN_BIN_INCLUSION.contains(binFileName)) {
                            FileUtils.copyFileToDirectory(binFile, destBinDir);
                            logger.info("copy file: {} {}", binFiles, destBinDir);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to export bin.", e);
        }
    }

}
