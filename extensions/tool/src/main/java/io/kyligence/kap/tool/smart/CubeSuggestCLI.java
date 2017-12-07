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

package io.kyligence.kap.tool.smart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.smart.common.MasterFactory;
import io.kyligence.kap.smart.cube.CubeMaster;

public class CubeSuggestCLI implements IKeep {

    private static final Logger logger = LoggerFactory.getLogger(CubeSuggestCLI.class);

    public static void main(String[] args) throws IOException {
        if (args == null || args.length != 4) {
            System.out.println(
                    "Usage: java io.kyligence.kap.tool.smart.CubeSuggestCLI <model> <sql_dir> <should_import> <name>");
            System.out.println(
                    "eg. java io.kyligence.kap.tool.smart.CubeSuggestCLI kylin_sales_model /tmp/sql/ true kylin_sales_cube");
            System.exit(1);
        }

        String modelName = args[0];
        File sqlFile = new File(args[1]);
        boolean shouldImport = Boolean.parseBoolean(args[2]);
        String cubeName = args[3];

        logger.debug("Parameters: modelName={}, sqlDir={}, shouldImport={}, cubeName={}", modelName,
                sqlFile.getAbsolutePath(), shouldImport, cubeName);
        suggestCube(modelName, sqlFile, shouldImport, cubeName);
    }

    private static void suggestCube(String modelName, File sqlFile, boolean shouldImport, String cubeName)
            throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        Preconditions.checkArgument(cubeManager.getCube(cubeName) == null,
                "Cube " + cubeName + " already exists, please use another name.");

        DataModelManager metadataManager = DataModelManager.getInstance(kylinConfig);
        DataModelDesc modelDesc = metadataManager.getDataModelDesc(modelName);

        Preconditions.checkNotNull(modelDesc, "Model not found with name " + modelName);
        Preconditions.checkArgument(sqlFile.exists(), "SQL File not exists at " + sqlFile.getAbsolutePath());

        List<String> sqlList = Lists.newArrayList();
        if (sqlFile.isDirectory()) {
            File[] sqlFiles = sqlFile.listFiles();
            Preconditions.checkArgument(sqlFiles != null && sqlFiles.length > 0,
                    "SQL files not found under " + sqlFile.getAbsolutePath());

            for (File file : sqlFiles) {
                sqlList.add(FileUtils.readFileToString(file, Charset.defaultCharset()));
            }
        } else if (sqlFile.isFile()) {
            BufferedReader br = new BufferedReader(new FileReader(sqlFile));
            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                if (line.endsWith(";")) {
                    sb.append(line);
                    sb.deleteCharAt(sb.length() - 1);
                    sqlList.add(sb.toString());
                    sb = new StringBuilder();
                } else {
                    sb.append(line);
                }
            }
        }

        String[] sqls = sqlList.toArray(new String[0]);
        logger.info("CubeSuggestion started with {} SQLs, for example:", sqls.length);
        for (int i = 0; i < Math.min(sqls.length, 5); i++) {
            logger.info(sqls[i]);
        }

        CubeMaster master = MasterFactory.createCubeMaster(kylinConfig, modelDesc, sqls);

        // propose initial cube
        CubeDesc initCube = master.proposeInitialCube();
        initCube.setName(cubeName);

        // get dimension and measure
        CubeDesc dimMeasCube = master.proposeDerivedDimensions(initCube);

        // get rowkey
        CubeDesc rowkeyCube = master.proposeRowkey(dimMeasCube);

        // get aggr groups
        CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);

        // get override cube
        CubeDesc configOverrideCube = master.proposeConfigOverride(aggGroupCube);

        if (shouldImport) {
            saveCube(configOverrideCube, modelDesc.getProject(), modelDesc.getOwner(), kylinConfig);
            logger.info("Cube suggestion was saved in project[name={}], please login to WebUI and reload metadata.",
                    modelDesc.getProject());
        } else {
            logger.info("Cube suggestion finished, following is output: \n{}",
                    JsonUtil.writeValueAsIndentString(configOverrideCube));
        }
    }

    private static void saveCube(CubeDesc cubeDesc, String projectName, String owner, KylinConfig kylinConfig)
            throws IOException {
        // save CubeDesc
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        cubeDescManager.createCubeDesc(cubeDesc);

        // save CubeInstance
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        cubeManager.createCube(cubeDesc.getName(), projectName, cubeDesc, owner);

        kylinConfig.clearManagers();
        CubeDescManager.getInstance(kylinConfig);
        CubeManager.getInstance(kylinConfig);

        logger.info("Cube is created and saved successfully: name={}", cubeDesc.getName());
    }
}
