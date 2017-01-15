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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.CubeDesc;

/**
 * Before run this CLI, need to replace "PROVIDED" with "COMPILE" in kap-auto-modeling.iml
 */
public class AutoModelingDebug {

    private static void setupSandboxEnv(String metaDir, String sqlDir, String modelName, String project) throws Exception {
        File metaRoot = new File(metaDir);
        if (!metaRoot.exists()) {
            throw new RuntimeException("metadata dir not found at: " + metaDir);
        }

        System.setProperty("KYLIN_CONF", new File("extensions/examples/test_case_data/sandbox").getAbsolutePath());
        KylinConfig.getInstanceFromEnv().setProperty("kylin.job.use-remote-cli", "false");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.url", metaRoot.getAbsolutePath());
    }

    public static void main(String[] args) throws Exception {
        final String metaDir = "extensions/auto-modeling/src/main/resources/ssb/meta";
        final String sqlDir = "extensions/auto-modeling/src/main/resources/ssb/sql";
        final String modelName = "ssb";
        final String project = "ssb";
        final String cubeName = "ssb";

        setupSandboxEnv(metaDir, sqlDir, modelName, project);

        CubeDesc cubeDesc = AutoModelingCLI.generateCube(sqlDir, modelName, cubeName, project);

        System.out.println("==============================================");
        System.out.println(JsonUtil.writeValueAsIndentString(cubeDesc));
    }
}
