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

package io.kyligence.kap.smart.common;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.Utils;

//@Ignore("Ignore because this is only used for demo.")
public class SmartMasterTest {
    public static String aggrStrategy = "mixed";

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Ignore
    @Test
    public void testE2E_LearnKylin() throws IOException {
        testInternal("src/test/resources/learn_kylin/meta", "kylin_sales_model", "src/test/resources/learn_kylin/sql");
    }

    //    @Ignore
    @Test
    public void testE2E_SSB() throws IOException {
        testInternal("src/test/resources/ssb/meta", "ssb", "src/test/resources/ssb/sql");
    }

    @Ignore
    @Test
    public void testE2E_TPCH_LineItem() throws IOException {
        testInternal("src/test/resources/tpch/meta", "lineitem_model", "src/test/resources/tpch/sql_lineitem");
    }

    @Ignore
    @Test
    public void testE2E_Airline() throws IOException {
        testInternal("src/test/resources/airline/meta", "airline_model", new String[0]);
    }

    @Ignore
    @Test
    public void testE2E_TPCDS_ss() throws Exception {
        //        testInternal("src/test/resources/tpcds/meta", "src/test/resources/tpcds/sql_ss");
        //        testInternal("src/test/resources/tpch/meta", "src/test/resources/tpch/sql_lineitem");
    }

    private void testInternal(String metaDir, String sqlDir) throws Exception {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.aggGroup.strategy", aggrStrategy);
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        File[] sqlFiles = new File[0];
        if (sqlDir != null) {
            File sqlDirF = new File(sqlDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(sqlDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".sql")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }

        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }

        ModelMaster modelMaster = MasterFactory.createModelMaster(kylinConfig, "TBD", sqls, "TBD");

        DataModelDesc modelDesc = modelMaster.proposeAll();

        CubeMaster cubeMaster = MasterFactory.createCubeMaster(kylinConfig, modelDesc, sqls);

        // TODO ...

    }

    private void testInternal(String metaDir, String modelName, String sqlDir) throws IOException {
        File[] sqlFiles = new File[0];
        if (sqlDir != null) {
            File sqlDirF = new File(sqlDir);
            if (sqlDirF.exists() && sqlDirF.listFiles() != null) {
                sqlFiles = new File(sqlDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".sql")) {
                            return true;
                        }
                        return false;
                    }
                });
            }
        }

        String[] sqls = new String[sqlFiles.length];
        for (int i = 0; i < sqlFiles.length; i++) {
            sqls[i] = FileUtils.readFileToString(sqlFiles[i], "UTF-8");
        }

        testInternal(metaDir, modelName, sqls);
    }

    private void testInternal(String metaDir, String modelName, String[] sqls) throws IOException {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.cube.aggrgroup.max-combination", "4096");
        kylinConfig.setProperty("kap.smart.conf.domain.query-enabled", "true");
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.AdHocRunnerSparkImpl");

        KylinConfig.setKylinConfigThreadLocal(kylinConfig);

        DataModelDesc modelDesc = DataModelManager.getInstance(kylinConfig).getDataModelDesc(modelName);

        CubeMaster master = MasterFactory.createCubeMaster(kylinConfig, modelDesc, sqls);

        // get initial cube
        CubeDesc initCube = master.proposeInitialCube();

        // get dimension and measure
        CubeDesc dimMeasCube = master.proposeDerivedDimensions(initCube);

        // get rowkey
        CubeDesc rowkeyCube = master.proposeRowkey(dimMeasCube);

        // get aggr groups
        CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);

        // get override cube
        CubeDesc configOverrideCube = master.proposeConfigOverride(aggGroupCube);
        System.out.println(JsonUtil.writeValueAsIndentString(configOverrideCube));

        for (AggregationGroup aggGroup : configOverrideCube.getAggregationGroups()) {
            System.out.println("Aggregation Group Combination:" + aggGroup.calculateCuboidCombination());
        }
    }
}
