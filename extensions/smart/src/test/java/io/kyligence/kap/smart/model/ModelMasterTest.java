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

package io.kyligence.kap.smart.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.query.util.CognosParenthesesEscape;
import io.kyligence.kap.smart.common.MasterFactory;
import io.kyligence.kap.smart.query.Utils;

//@Ignore("Ignore because this is only used for demo.")
public class ModelMasterTest {
    private static final Logger logger = LoggerFactory.getLogger(ModelMasterTest.class);

    @AfterClass
    public static void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws Exception {
        testInternal("src/test/resources/learn_kylin/meta", "learn_kylin", "kylin_sales",
                "src/test/resources/learn_kylin/sql");
    }

    @Test
    public void testE2E_LearnKylin_dupJoins_1() throws Exception {
        DataModelDesc modelDesc = testInternal("src/test/resources/learn_kylin/meta", "learn_kylin", "kylin_sales",
                "src/test/resources/learn_kylin/sql_dupJoins_1");
        Assert.assertNotNull(modelDesc);
        Assert.assertEquals(modelDesc.getJoinTables().length, 6);
    }

    @Test
    public void testE2E_LearnKylin_dupJoins_2() throws Exception {
        DataModelDesc modelDesc = testInternal("src/test/resources/learn_kylin/meta", "learn_kylin", "kylin_sales",
                "src/test/resources/learn_kylin/sql_dupJoins_2");
        Assert.assertNotNull(modelDesc);
        Assert.assertEquals(modelDesc.getJoinTables().length, 6);
    }

    @Test
    public void testE2E_SSB() throws Exception {
        testInternal("src/test/resources/ssb/meta", "ssb", "src/test/resources/ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws Exception {
        testInternal("src/test/resources/tpch/meta", "tpch", "src/test/resources/tpch/sql_lineitem");
    }

    @Test
    public void testE2E_TPCDS_ss() throws Exception {
        testInternal("src/test/resources/tpcds/meta", "TPC_DS_2", "TPCDS_BIN_PARTITIONED_ORC_2.STORE_SALES",
                "src/test/resources/tpcds/sql_ss");
    }

    @Test
    public void testE2E_TPCDS_MultiFact() throws Exception {
        testInternal("src/test/resources/tpcds/meta", "TPC_DS_2", "src/test/resources/tpcds/sql_ss");
    }

    @Test
    public void testE2E_TPCDS_SelfJoin() throws Exception {
        testInternal("src/test/resources/tpcds/meta", "TPC_DS_2", "src/test/resources/tpcds/sql_ss_selfjoin");
    }

    private DataModelDesc testInternal(String metaDir, String project, String factTable, String sqlDir)
            throws Exception {
        KylinConfig kylinConfig = prepareConfig(metaDir);
        String[] sqls = loadQueries(sqlDir);

        logger.info("Test auto modeling with fact table {}.", factTable);

        ModelMaster master = MasterFactory.createModelMaster(kylinConfig, project, sqls, factTable);
        DataModelDesc modelDesc = master.proposeAll();

        System.out.println(JsonUtil.writeValueAsIndentString(modelDesc));
        return modelDesc;
    }

    private void testInternal(String metaDir, String project, String sqlDir) throws Exception {
        KylinConfig kylinConfig = prepareConfig(metaDir);
        String[] sqls = loadQueries(sqlDir);

        logger.info("Test auto modeling without fact table, will generate multiple models.");
        Collection<ModelMaster> masters = MasterFactory.createModelMasters(kylinConfig, project, sqls);

        int cnt = 0;
        for (ModelMaster master : masters) {
            logger.info("Auto generating model {}", ++cnt);
            DataModelDesc modelDesc = master.proposeAll();
            System.out.println(JsonUtil.writeValueAsIndentString(modelDesc));
        }
        logger.info("{} models created.", cnt);
    }

    protected KylinConfig prepareConfig(String metaDir) {
        KylinConfig kylinConfig = Utils.newKylinConfig(metaDir);
        kylinConfig.setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.storage.parquet.adhoc.AdHocRunnerSparkImpl");
        kylinConfig.setProperty("kap.smart.conf.model.scope.strategy", "query");
        kylinConfig.setProperty("kylin.query.transformers", CognosParenthesesEscape.class.getName());
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        return kylinConfig;
    }

    protected String[] loadQueries(String sqlDir) throws Exception {
        List<String> sqlList = Lists.newArrayList();
        File sqlFile = new File(sqlDir);
        if (sqlFile.isDirectory()) {
            File[] sqlFiles = sqlFile.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".sql");
                }
            });
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
                if (line.startsWith("--")) {
                    continue;
                }
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

        return sqlList.toArray(new String[0]);
    }
}
