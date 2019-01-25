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

package io.kyligence.kap.smart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.kyligence.kap.common.util.KylinConfigUtils;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.smart.query.Utils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSmartDemoTest {
    private static String TEST_META_BASE = "src/test/resources/nsmart/";

    @After
    public void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws IOException {
        testInternal(TEST_META_BASE + "learn_kylin/meta", "learn_kylin", TEST_META_BASE + "learn_kylin/sql");
    }

    @Test
    public void testE2E_SSB() throws IOException {
        testInternal(TEST_META_BASE + "ssb/meta", "ssb", TEST_META_BASE + "ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws IOException {
        testInternal(TEST_META_BASE + "tpch/meta", "tpch", TEST_META_BASE + "tpch/sql_tmp");
    }

    @Test
    public void testE2E_Airline() throws IOException {
        testInternal(TEST_META_BASE + "airline/meta", "airline", TEST_META_BASE + "airline/sql");
    }

    @Test
    public void testE2E_TPCDS() throws IOException {
        testInternal(TEST_META_BASE + "tpcds/meta", "TPC_DS_2", TEST_META_BASE + "tpcds/sql_ss");
    }

    private void testInternal(String metaDir, String projectName, String sqlDir) throws IOException {
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
                String line;
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

        File tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);
        KylinConfig kylinConfig = Utils.newKylinConfig(tmpMeta.getAbsolutePath());
        kylinConfig.setProperty("kylin.env", "UT");
        KylinConfigUtils.setH2DriverAsFavoriteQueryStorageDB(kylinConfig);
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            NSmartMaster smartMaster = new NSmartMaster(kylinConfig, projectName, sqls);
            smartMaster.runAll();

            NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, projectName);
            Assert.assertFalse(dataflowManager.listUnderliningDataModels().isEmpty());
            log.info("Number of models: " + dataflowManager.listUnderliningDataModels().size());

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, projectName);
            Assert.assertFalse(indexPlanManager.listAllIndexPlans().isEmpty());
            log.info("Number of cubes: " + indexPlanManager.listAllIndexPlans().size());
        }

        FileUtils.forceDelete(tmpMeta);
    }
}
