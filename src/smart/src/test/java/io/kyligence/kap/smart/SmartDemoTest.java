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

import static io.kyligence.kap.smart.model.GreedyModelTreesBuilderTest.smartUtHook;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.util.AbstractTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.smart.util.AccelerationContextUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmartDemoTest extends AbstractTestCase {
    private static final String TEST_META_BASE = "src/test/resources/nsmart/";

    @After
    public void afterClass() {
        KylinConfig.destroyInstance();
    }

    @Test
    public void testE2E_LearnKylin() throws Exception {
        testInternal(TEST_META_BASE + "learn_kylin/meta", "learn_kylin", TEST_META_BASE + "learn_kylin/sql");
    }

    @Test
    public void testE2E_SSB() throws Exception {
        testInternal(TEST_META_BASE + "ssb/meta", "ssb", TEST_META_BASE + "ssb/sql");
    }

    @Test
    public void testE2E_TPCH_LineItem() throws Exception {
        testInternal(TEST_META_BASE + "tpch/meta", "tpch", TEST_META_BASE + "tpch/sql_tmp");
    }

    @Test
    public void testE2E_Airline() throws Exception {
        testInternal(TEST_META_BASE + "airline/meta", "airline", TEST_META_BASE + "airline/sql");
    }

    @Test
    public void testE2E_TPCDS() throws Exception {
        testInternal(TEST_META_BASE + "tpcds/meta", "TPC_DS_2", TEST_META_BASE + "tpcds/sql_ss");
    }

    private void testInternal(String metaDir, String projectName, String sqlDir) throws Exception {
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
                try (InputStream os = new FileInputStream(sqlFile);
                        BufferedReader br = new BufferedReader(new InputStreamReader(os, Charset.defaultCharset()))) {
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
        }

        File tmpHome = Files.createTempDir();
        File tmpMeta = new File(tmpHome, "metadata");
        overwriteSystemProp("KYLIN_HOME", tmpHome.getAbsolutePath());
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);
        FileUtils.touch(new File(tmpHome.getAbsolutePath() + "/kylin.properties"));
        KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.query.security.acl-tcr-enabled", "false");
        kylinConfig.setProperty("kylin.smart.conf.propose-runner-type", "in-memory");
        kylinConfig.setProperty("kylin.env", "UT");
        Class.forName("org.h2.Driver");

        try (SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig)) {
            AbstractContext context = AccelerationContextUtil.newSmartContext(kylinConfig, projectName,
                    sqlList.toArray(new String[0]));
            SmartMaster smartMaster = new SmartMaster(context);
            smartMaster.runUtWithContext(smartUtHook);

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
