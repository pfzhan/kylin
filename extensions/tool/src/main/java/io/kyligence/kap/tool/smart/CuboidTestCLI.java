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
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.smart.query.Utils;

public class CuboidTestCLI implements IKeep {
    public static void main(String[] args) throws Exception {
        // Uncomment these lines to debug
        // =======
        //        System.setProperty("log4j.configuration", "file:build/conf/kylin-tools-log4j.properties");
        //        args = new String[2];
        //        args[0] = "POC_COMMISSION";
        //        args[1] = "/Users/dong/Desktop/POC_COMMISSION.sql";
        //        KylinConfig.setKylinConfigThreadLocal(
        //                Utils.newKylinConfig("/Users/dong/Downloads/kybot_2017_09_05_12_02_49/metadata"));
        // ========

        Logger logger = LoggerFactory.getLogger(CuboidTestCLI.class);

        if (args == null || args.length != 2) {
            System.out.println("Usage: java io.kyligence.kap.tool.smart.CuboidTestCLI <cube> <sql_file>");
            System.out.println("eg. java java io.kyligence.kap.tool.smart.CuboidTestCLI kylin_sales_cube /tmp/sql.txt");
            System.exit(1);
        }

        String cubeName = args[0];
        File sqlFile = new File(args[1]);
        Preconditions.checkState(sqlFile.exists(), "SQL File does not exsits at " + sqlFile.getAbsolutePath());

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
                    sb.append("\n");
                }
            }
        }

        String[] sqls = sqlList.toArray(new String[0]);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config = Utils.newKylinConfig(config.getMetadataUrl().toString());
        KylinConfig.setKylinConfigThreadLocal(config);

        CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(cubeName);
        CuboidScheduler scheduler = CuboidScheduler.getInstance(cubeDesc);

        //        try (AbstractQueryRunner runner = QueryRunnerFactory.createForCubeSuggestion(config, sqls, 1, cubeDesc)) {
        //            runner.execute();
        //            QueryStats stats = runner.getQueryStats();
        //
        //            logger.info("==========================");
        //            for (Set<String> cuboidCols : stats.getCuboids()) {
        //                Set<TblColRef> dims = Sets.newHashSet();
        //                for (String col : cuboidCols) {
        //                    String[] pairs = col.split("\\.");
        //                    TblColRef colRef = cubeDesc.findColumnRef(pairs[0], pairs[1]);
        //                    dims.add(colRef);
        //                }
        //                long queryCuboidId = Cuboid.identifyCuboidId(cubeDesc, dims, Lists.<FunctionDesc> newArrayList());
        //                boolean isValid = scheduler.isValid(queryCuboidId);
        //                if (!isValid) {
        //                    long validCuboidId = Cuboid.findById(scheduler, queryCuboidId).getId();
        //                    logger.info("Cuboid does not exactly match: exact={}, valid={}", queryCuboidId, validCuboidId);
        //                } else {
        //                    logger.info("Cuboid exactly matches: exact={}, valid={}", queryCuboidId, queryCuboidId);
        //                }
        //            }
        //            logger.info("==========================");
        //        }
    }
}
