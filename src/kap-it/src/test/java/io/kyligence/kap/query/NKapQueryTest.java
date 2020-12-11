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

package io.kyligence.kap.query;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.data.QueryResult;
import jersey.repackaged.com.google.common.collect.Lists;

/**
 * if a query test does not contribute to verifying the correctness of cube data, it should be here
 * otherwise, better consider NAutoBuildAndQueryTest or NManualBuildAndQueryTest
 */
public class NKapQueryTest extends NKylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(NKapQueryTest.class);
    protected static final String KYLIN_SQL_BASE_DIR = "../../kylin/kylin-it/src/test/resources/query";
    protected static final String KAP_SQL_BASE_DIR = "../kap-it/src/test/resources/query";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        logger.info("setUp in NKapQueryTest");
        joinType = "left";
        System.setProperty("kylin.query.engine.sparder-enabled", "false");
        NKylinTestBase.setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        logger.info("tearDown in NKapQueryTest");
        System.clearProperty("kylin.query.engine.sparder-enabled");
        NKylinTestBase.clean();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testQuery_cubeNonAggDisabled_throwNoRealization() throws Exception {
        thrown.expect(SQLException.class);
        thrown.expectMessage("No realization");
        String x = KylinConfig.getInstanceFromEnv().getPushDownRunnerClassName();
        try {
            System.setProperty("kylin.query.engine.sparder-enabled", "true");
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.runner-class-name", "");
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown-enabled", "false");

            File tempFile = File.createTempFile("testQuery_cubeNonAggDisabled_throwNoRealization", "sqlfile");
            tempFile.deleteOnExit();
            FileUtils.writeStringToFile(tempFile, "select * from test_kylin_fact", false);
            runSQL(tempFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.runner-class-name", x);
            if (Strings.isEmpty(x)) {
                KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown-enabled", "false");
            } else {
                KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown-enabled", "true");
            }
        }
    }

    @Test
    public void testQuery_validSql_fail() throws Exception {

        logger.info("-------------------- Test Invalid Query --------------------");
        String queryFolder = KAP_SQL_BASE_DIR + File.separator + "sql_invalid";
        List<File> sqlFiles = getFilesFromFolder(new File(queryFolder), ".sql");
        for (File sqlFile : sqlFiles) {
            logger.info("Testing Invalid Query: " + sqlFile.getCanonicalPath());
            try {
                runSQL(sqlFile, false, false);
            } catch (Throwable t) {
                continue;
            }
            throw new IllegalStateException(sqlFile.getName() + " should not pass!");
        }
    }

    @Test
    public void testSpecialPurposeQueries() throws Exception {

        List<String> queries = Lists.newArrayList("SELECT VERSION() AS ret", "SELECT 'Hello world' AS ret");
        List<String> expectedAnswers = Lists.newArrayList(KylinVersion.getCurrentVersion().toString(), "Hello world");

        for (int i = 0; i < queries.size(); i++) {

            String query = queries.get(i);
            // execute Kylin
            logger.info("Comparing query at position {} ", i);
            QueryResult queryResult = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv()).executeQuery(query);

            // compare the result
            Assert.assertEquals(expectedAnswers.get(i), queryResult.getRows().get(0).get(0));
        }
    }

    //
    //    @Test
    //    public void testVerifyCountQuery() throws Exception {
    //        verifyResultRowColCount(KYLIN_SQL_BASE_DIR + File.separator + "sql_verifyCount");
    //    }

    @Test
    public void testFloorConstantQuery() throws Exception {
        val query0 = "select {FN WEEK(CEIL( FLOOR(date'2020-11-10' TO week  ) TO DAY    )) }";
        val query1 = "select floor(date'2020-11-10' to week)";
        val query2 = "select ceil(date'2020-11-10' to month)";
        val queries = Lists.newArrayList(query0, query1, query2);
        val expectedAnswers = Lists.newArrayList("46", "2020-11-09 00:00:00", "2020-12-01 00:00:00");

        for (int i = 0; i < queries.size(); i++) {
            val query = queries.get(i);
            val queryResult = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv()).executeQuery(query);
            Assert.assertEquals(expectedAnswers.get(i), queryResult.getRows().get(0).get(0));
        }
    }
}