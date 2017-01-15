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

package io.kyligence.kap.modeling.auto.proposer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public abstract class AbstractProposer implements IProposer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractProposer.class);

    @Override
    public CubeDesc propose(String[] queries, String project) {
        CubeDesc workCubeDesc = propose();
        if (queries != null) {
            workCubeDesc = pruneCubeWithQueries(queries, workCubeDesc, project);
        }
        return workCubeDesc;
    }

    private static CubeDesc pruneCubeWithQueries(String[] queries, CubeDesc srcCubeDesc, String project) {
        CubeDesc result = null;

        File tmpMetaDir = Files.createTempDir();
        String dstPath = tmpMetaDir.getAbsolutePath();
        logger.info("Temp metadata path: {}", dstPath);

        File tempMetaDir = new File(dstPath);
        try {
            /* prepare input */
            ResourceTool.copy(KylinConfig.getInstanceFromEnv(), KylinConfig.createInstanceFromUri(dstPath));
            if (!tempMetaDir.isDirectory()) {
                throw new RuntimeException("Unable to dump metadata to " + dstPath);
            }
            // drop all current cubes
            File cubeDir = new File(tempMetaDir, "cube");
            FileUtils.deleteQuietly(cubeDir);
            FileUtils.forceMkdir(cubeDir);
            File cubeDescDir = new File(tempMetaDir, "cube_desc");
            FileUtils.deleteQuietly(cubeDescDir);
            FileUtils.forceMkdir(cubeDescDir);
            // provide sample queries
            File sqlDir = new File(tempMetaDir, "sql");
            FileUtils.deleteQuietly(sqlDir);
            FileUtils.forceMkdir(sqlDir);
            for (int i = 0; i < queries.length; i++) {
                String sql = queries[i];
                FileUtils.writeStringToFile(new File(sqlDir, (i + 1) + ".sql"), sql, Charset.defaultCharset(), false);
            }
            // the input cube
            String cubeJson = JsonUtil.writeValueAsIndentString(srcCubeDesc);
            System.out.println(cubeJson);
            File input = new File(tempMetaDir, "input.json");
            FileUtils.writeStringToFile(input, cubeJson, Charset.defaultCharset(), false);

            /* Fork Executer process */
            String diagCmd = "java -cp \"" + System.getProperty("java.class.path") + "\" " + io.kyligence.kap.modeling.auto.mockup.MockupRunner.class.getName() + " \"" + dstPath + "\" " + project;
            CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            Pair<Integer, String> cmdOutput = executor.execute(diagCmd, new org.apache.kylin.common.util.Logger() {
                @Override
                public void log(String message) {
                    System.out.println(message);
                }
            });

            // TODO Read output file
            File output = new File(tempMetaDir, "output.json");
            if (output.isFile()) {
                // String outputJson = FileUtils.readFileToString(output, Charset.defaultCharset());
                return JsonUtil.readValue(output, CubeDesc.class);
            }
        } catch (IOException e) {
            // TODO: handle exception
            logger.error("", e);
        } finally {
            // TODO cleanup
            FileUtils.deleteQuietly(tempMetaDir);
        }

        return result;
    }
}
