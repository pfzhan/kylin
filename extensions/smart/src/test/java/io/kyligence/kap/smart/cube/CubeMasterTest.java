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

package io.kyligence.kap.smart.cube;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.After;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.smart.common.MasterFactory;
import io.kyligence.kap.smart.query.Utils;

public class CubeMasterTest {
    private static String TEST_META_BASE = "src/test/resources/smart/";
    public static String aggrStrategy = "mixed";

    @After
    public void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws IOException {
        testInternal(TEST_META_BASE + "learn_kylin/meta", "kylin_sales_model", TEST_META_BASE + "learn_kylin/sql");
    }

    @Test
    public void testE2E_SSB() throws IOException {
        testInternal(TEST_META_BASE + "ssb/meta", "ssb", TEST_META_BASE + "ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws IOException {
        testInternal(TEST_META_BASE + "tpch/meta", "lineitem_model", TEST_META_BASE + "tpch/sql_lineitem");
    }

    @Test
    public void testE2E_Airline() throws IOException {
        testInternal(TEST_META_BASE + "airline/meta", "airline_model", null);
    }

    private void testInternal(String metaDir, String modelName, String sqlDir) throws IOException {
        List<String> sqlList = Lists.newArrayList();
        if (sqlDir != null) {
            File sqlFile = new File(sqlDir);
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
        }

        String[] sqls = sqlList.toArray(new String[0]);

        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.aggGroup.strategy", aggrStrategy);
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        kylinConfig.setProperty("kap.smart.strategy", "batch");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);

        CubeMaster master = MasterFactory.createCubeMaster(kylinConfig, modelDesc, sqls);

        CubeDesc cubeDesc = master.proposeAll();
        cubeDesc.init(KylinConfig.getInstanceFromEnv());
        System.out.println(JsonUtil.writeValueAsIndentString(cubeDesc));

        for (AggregationGroup aggGroup : cubeDesc.getAggregationGroups()) {
            System.out.println("Aggregation Group Combination:" + aggGroup.calculateCuboidCombination());
        }
        System.out.println(cubeDesc.getAllCuboids().size());
    }
}
