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

package io.kyligence.kap;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.ICompareQueryTranslator;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.spark.sql.QueryTest;
import org.apache.spark.sql.common.SparderContext;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.junit.SparkTestRunner;

@RunWith(SparkTestRunner.class)
public class ITKapSparderAsyncQueryTest extends ITKapKylinQueryTest {
    private static final Logger logger = LoggerFactory.getLogger(ITKapSparderAsyncQueryTest.class);

    @Test
    @Ignore
    public void testSingleRunQuery() throws Exception {
        if (!"true".equals(System.getProperty("sparder.enabled"))) {
            return;
        }
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");
        String queryFileName = "src/test/resources/query/sparder/pass/query10.sql";
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    @Test
    public void testSparderAsyncResultCube() throws Exception {
        if (!"true".equals(System.getProperty("sparder.enabled"))) {
            return;
        }
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        String queryFileName = "src/test/resources/query/sparder/asyncresult/cube";

        File sqlFile = new File(queryFileName);

        if (sqlFile.exists()) {
            this.execAndCompQuery(queryFileName, null, true);
        }
    }

    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";

    @Test
    public void testSparderAsyncResultPushDown() throws Exception {
        if (!"true".equals(System.getProperty("sparder.enabled"))) {
            return;
        }
        System.setProperty(PUSHDOWN_RUNNER_KEY, "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
        System.setProperty("sparder.asyncresult.ispushdown", "true");
        String origin = System.getProperty("needCheckCC");
        System.setProperty("needCheckCC", "true");
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_left_join_cube]");
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        String queryFileName = "src/test/resources/query/sparder/asyncresult/pushdown";
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            this.execAndCompQuery(queryFileName, null, true);
        }
        if (origin != null) {
            System.setProperty("needCheckCC", origin);
        }
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_left_join_cube]");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY, "");
    }

    protected int runSQL(File sqlFile, boolean debug, boolean explain) throws Exception {
        if (debug) {
            System.setProperty("calcite.debug", "true");
            InputStream inputStream = new FileInputStream("src/test/resources/logging.properties");
            LogManager.getLogManager().readConfiguration(inputStream);
        }
        QueryContext.reset();
        SparderContext.setResultRef(new AtomicReference<Boolean>());
        SparderContext.setAsAsyncQuery();
        String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
        logger.info("Testing Query " + queryName);
        String sql = getTextFromFile(sqlFile);
        if (explain) {
            sql = "explain plan for " + sql;
        }
        executeQuery(sql, true);

        try {
            QueryTest.checkAsyncResultData();
        } catch (Throwable t) {
            logger.info("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
            throw t;
        }
        if (debug) {
            System.clearProperty("calcite.debug");
        }
        return 0;
    }

    protected void execAndCompQuery(String queryFolder, String[] exclusiveQuerys, boolean needSort,
            ICompareQueryTranslator translator) throws Exception {
        logger.info("---------- test folder: " + new File(queryFolder).getAbsolutePath());
        Set<String> exclusiveSet = buildExclusiveSet(exclusiveQuerys);
        SparderContext.setAsAsyncQuery();
        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            QueryContext.reset();
            SparderContext.setResultRef(new AtomicReference<Boolean>());
            String queryName = StringUtils.split(sqlFile.getName(), '.')[0];
            if (exclusiveSet.contains(queryName)) {
                continue;
            }
            String sql1 = getTextFromFile(sqlFile);
            // execute Kylin
            logger.info("Query Result from Kylin - " + queryName + "  (" + queryFolder + ")");
            IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
            executeQuery(kylinConn, queryName, sql1, needSort);
            try {
                if ("true".equalsIgnoreCase(System.getProperty("sparder.asyncresult.ispushdown"))) {
                    QueryTest.checkPushDownAsyncResultData(sql1);
                } else {
                    QueryTest.checkAsyncResultData();
                }
            } catch (Throwable t) {
                logger.info("execAndCompQuery failed on: " + sqlFile.getAbsolutePath());
                throw t;
            }
        }
    }

    @Ignore
    public void testH2Uncapable() throws Exception {
    }

    @Ignore
    public void testSubQuery() throws Exception {
    }

    @Ignore
    public void testLookupQuery() throws Exception {
    }

    @Ignore
    public void testDateTimeQuery() throws Exception {
    }

    @Ignore
    public void testDynamicQuery() throws Exception {
    }

    @Ignore
    public void testDimDistinctCountQuery() throws Exception {
    }

    @Ignore
    public void testLikeQuery() throws Exception {
    }

    @Ignore
    public void testPreciselyDistinctCountQuery() throws Exception {
    }

    @Ignore
    public void testDerivedColumnQuery() throws Exception {
    }

    @Ignore
    public void testVerifyCountQuery() throws Exception {
    }

    @Ignore
    public void testVerifyCountQueryWithPrepare() throws Exception {
    }

    @Ignore
    public void testPowerBiQuery() throws Exception {
    }

    @Ignore
    public void testRawTableQuery() throws Exception {
    }

    @Ignore
    public void testDistinctCountQueryExt() throws Exception {
    }

    @Ignore
    public void testTopNQueryExt() throws Exception {
    }

    @Ignore
    public void testPreciselyDistinctCountQueryExt() throws Exception {
    }

    @Ignore
    public void testIntersectCountQuery() throws Exception {
    }

    @Ignore
    public void testRawTablePrecedesCubeOnRawQueries() throws Exception {
    }

    @Ignore
    public void testComputedColumnsQuery() throws Exception {
    }

    @Ignore
    public void testLimitEnabled() throws Exception {
    }

    @Ignore
    public void testTimeoutQuery() throws Exception {
    }

    @Ignore
    public void testTimeoutQuery2() throws Exception {
    }

    @Ignore
    public void testPercentileQuery() throws Exception {
    }

    @Ignore
    public void testDisableCubeForNoAggQuery() throws Exception {
    }

    @Ignore
    public void testKAPSingleInternalQuery() throws Exception {
    }

    @Ignore
    public void testKAPSinglePublicQuery() throws Exception {
    }

    @Ignore
    public void testTempQuery() throws Exception {
    }

    @Ignore
    public void testSnowflakeQuery() throws Exception {
    }

    @Ignore
    public void testUnionQuery() throws Exception {
    }

    @Ignore
    public void testSingleExecuteQuery() throws Exception {
    }

    @Ignore
    public void testTableauProbing() throws Exception {
    }

    @Ignore
    public void testCommonQuery() throws Exception {
    }

    @Ignore
    public void testExtendedColumnQuery() throws Exception {
    }

    @Ignore
    public void testVerifyContentQuery() throws Exception {
    }

    @Ignore
    public void testOrderByQuery() throws Exception {
    }

    @Ignore
    public void testJoinCastQuery() throws Exception {
    }

    @Ignore
    public void testCachedQuery() throws Exception {
    }

    @Ignore
    public void testDistinctCountQuery() throws Exception {
    }

    @Ignore
    public void testTopNQuery() throws Exception {
    }

    @Ignore
    public void testMultiModelQuery() throws Exception {
    }

    @Ignore
    public void testStreamingTableQuery() throws Exception {
    }

    @Ignore
    public void testTableauQuery() throws Exception {
    }

    @Ignore
    public void testCaseWhen() throws Exception {
    }

    @Ignore
    public void testHiveQuery() throws Exception {
    }

    @Ignore
    public void testH2Query() throws Exception {
    }

    @Ignore
    public void testInvalidQuery() throws Exception {
    }

    @Ignore
    public void testLimitCorrectness() throws Exception {
    }

    @Ignore
    public void testRawQuery() throws Exception {
    }

    @Ignore
    public void testGroupingQuery() throws Exception {
    }

    @Ignore
    public void testWindowQuery() throws Exception {
    }

    @Ignore
    public void testVersionQuery() throws Exception {
    }

    @Ignore
    public void testTimeStampAdd() throws Exception {
    }
}
