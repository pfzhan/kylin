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

package io.kyligence.kap.modeling.auto;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;

/**
 * Before run this CLI, need to replase "PROVIDED" with "COMPILE" in kap-auto-modeling.iml
 */
public class AutoModelingTestCLI {

    private static void updateConfig(File metaDir) {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.job.use-remote-cli", "false");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.url", metaDir.getAbsolutePath());
    }

    private static CubeDesc generateCube(String metaDir, String sqlDir, String modelName, String project) throws Exception {
        File metaRoot = new File(metaDir);
        if (!metaRoot.exists()) {
            throw new RuntimeException("metadata dir not found at: " + metaDir);
        }
        updateConfig(metaRoot);

        File sqlRoot = new File(sqlDir);
        String[] sqls = null;
        if (sqlRoot.exists()) {
            File[] sqlFiles = sqlRoot.listFiles();
            if (sqlFiles == null) {
                System.out.println("Must specify a dir for sqls.");
                System.exit(1);
            }

            sqls = new String[sqlFiles.length];
            for (int i = 0; i < sqlFiles.length; i++) {
                sqls[i] = FileUtils.readFileToString(sqlFiles[i], "utf-8");
            }
        }

        CubeDesc autoCube = AutoModelingService.getInstance().generateCube(modelName, sqls, project);
        return autoCube;
    }

    public static void main(String[] args) throws Exception {
        final String metaDir = "extensions/auto-modeling/src/main/resources/ssb/meta";
        final String sqlDir = "extensions/auto-modeling/src/main/resources/ssb/sql";
        final String modelName = "ssb";
        final String project = "ssb";

        System.setProperty("KYLIN_CONF", new File("extensions/examples/test_case_data/sandbox").getAbsolutePath());

        CubeDesc cubeDesc = generateCube(metaDir, sqlDir, modelName, project);

        System.out.println("==============================================");
        System.out.println(JsonUtil.writeValueAsIndentString(cubeDesc));
    }
}
