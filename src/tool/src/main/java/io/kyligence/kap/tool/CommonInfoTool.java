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

import io.kyligence.kap.tool.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class CommonInfoTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static ClientEnvTool clientEnvTool = new ClientEnvTool();

    private CommonInfoTool() {
    }

    public static void exportClientInfo(File exportDir) {
        try {
            String[] clientArgs = { "-destDir", new File(exportDir, "client").getAbsolutePath(), "-compress", "false",
                    "-submodule", "true" };

            clientEnvTool.execute(clientArgs);
        } catch (Exception e) {
            logger.error("Failed to extract client env, ", e);
        }
    }

    public static void exportHadoopEnv(File exportDir) {
        try {
            File file = new File(exportDir, "hadoop_env");
            clientEnvTool.extractInfoByCmd("env>" + file.getAbsolutePath(), file);
        } catch (Exception e) {
            logger.warn("Error in export hadoop env, ", e);
        }
    }

    public static void exportKylinHomeDir(File exportDir) {
        try {
            File file = new File(exportDir, "catalog_info");
            String cmd = String.format("ls -lR %s>%s", ToolUtil.getKylinHome(), file.getAbsolutePath());
            clientEnvTool.extractInfoByCmd(cmd, file);
        } catch (Exception e) {
            logger.error("Error in export KYLIN_HOME dir, ", e);
        }
    }
}
