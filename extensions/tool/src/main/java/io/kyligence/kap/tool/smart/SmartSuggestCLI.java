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
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
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
import io.kyligence.kap.smart.model.ModelMaster;

public class SmartSuggestCLI implements IKeep {
    private static Logger logger = LoggerFactory.getLogger(SmartSuggestCLI.class);;

    public static void main(String[] args) throws Exception {
        //        args = new String[] { "TPC_DS_2", "/Users/dong/Projects/bigjohn/kap/tpcds/queries_filtered" };
        //
        if (args == null || args.length != 2) {
            System.out.println("Usage: java io.kyligence.kap.tool.smart.SmartSuggestCLI <project> <sql_dir>");
            System.out.println("eg. java io.kyligence.kap.tool.smart.SmartSuggestCLI learn_kylin /tmp/sql/ true");
            System.exit(1);
        }

        String projectName = args[0];
        File sqlFile = new File(args[1]);

        logger.debug("Parameters: project={}, sqlDir={}", projectName, sqlFile.getAbsolutePath());

        doSuggest(projectName, sqlFile);
    }

    private static String[] readSqls(File sqlFile) throws Exception {
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

        return sqlList.toArray(new String[0]);
    }

    private static void doSuggest(String projectName, File sqlFile) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        //        KylinConfig kylinConfig = Utils.newKylinConfig("/Users/dong/Projects/bigjohn/kap/tpcds/meta");
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.aggGroup.strategy", "whitelist");
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        kylinConfig.setProperty("kap.smart.strategy", "batch");
        //        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        Preconditions.checkArgument(sqlFile.exists(), "SQL File not exists at " + sqlFile.getAbsolutePath());
        String[] sqls = readSqls(sqlFile);
        logger.info("Smart Suggestion started with {} SQLs, for example:", sqls.length);

        List<ModelMaster> modelMasters = MasterFactory.createModelMasters(kylinConfig, projectName, sqls);
        logger.info("There will be {} models.", modelMasters.size());

        // creating models
        DataModelManager modelManager = DataModelManager.getInstance(kylinConfig);
        for (int i = 0; i < modelMasters.size(); i++) {
            logger.info("Generating the {}th model.", i);
            DataModelDesc modelDesc = modelMasters.get(i).proposeAll();
            modelManager.createDataModelDesc(modelDesc, projectName, null);
        }

        List<CubeMaster> cubeMasters = MasterFactory.createCubeMasters(kylinConfig, projectName, sqls);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        for (int i = 0; i < cubeMasters.size(); i++) {
            logger.info("Generating the {}th cube.", i);
            CubeDesc cube = cubeMasters.get(i).proposeAll();
            cubeDescManager.createCubeDesc(cube);
            cubeManager.createCube(cube.getName(), projectName, cube, null);
        }

        logger.info("{} Models and {} cubes are created.", modelMasters.size(), cubeMasters.size());
    }
}
