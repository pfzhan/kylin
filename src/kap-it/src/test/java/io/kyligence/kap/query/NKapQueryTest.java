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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
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
        System.setProperty("kap.query.engine.sparder-enabled", "false");
        NKylinTestBase.setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (Shell.MAC)
            System.clearProperty("org.xerial.snappy.lib.name");//reset

        logger.info("tearDown in NKapQueryTest");
        System.clearProperty("kap.query.engine.sparder-enabled");
        NKylinTestBase.clean();
        FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
    }

    @Test
    public void testQuery_cubeNonAggDisabled_throwNoRealization() throws Exception {
        thrown.expect(SQLException.class);
        thrown.expectMessage("No realization");
        String x = KylinConfig.getInstanceFromEnv().getPushDownRunnerClassName();
        try {
            System.setProperty("kap.query.engine.sparder-enabled", "true");
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.disable-cube-noagg-sql", "true");
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.runner-class-name", "");

            File tempFile = File.createTempFile("testQuery_cubeNonAggDisabled_throwNoRealization", "sqlfile");
            tempFile.deleteOnExit();
            FileUtils.writeStringToFile(tempFile, "select * from test_kylin_fact", false);
            runSQL(tempFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.disable-cube-noagg-sql", "false");
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.runner-class-name", x);
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

    @Test
    //ref:KE-9015
    public void testConstantUdfQuery() throws Exception {
        System.setProperty("kap.query.engine.run-constant-query-locally", "true");
        System.setProperty("kap.query.engine.sparder-enabled", "true");

        List<String> queries = Arrays.asList(
                //sql-1
                "select\n" + "  trunc('2009-02-12', 'MM'),trunc('2015-10-27', 'YEAR'),\n"
                        + "  trunc(date'2009-02-12', 'MM'),trunc(timestamp'2009-02-12 00:00:00', 'MM'),\n"
                        + "  add_months('2016-08-31', 1),add_months(date'2016-08-31', 2),add_months(timestamp'2016-08-31 00:00:00', 1),\n"
                        + "  date_add('2016-07-30', 1),date_add(date'2016-07-30', 1),date_add(timestamp'2016-07-30 00:00:00', 1),\n"
                        + "  date_sub('2016-07-30', 1),date_sub(date'2016-07-30', 1),date_sub(timestamp'2016-07-30 00:00:00', 1),\n"
                        + "  from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'),\n"
                        + "  from_utc_timestamp('2016-08-31', 'Asia/Seoul'),from_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),from_utc_timestamp(date'2016-08-31', 'Asia/Seoul'),\n"
                        + "  months_between('1997-02-28 10:30:00', '1996-10-30'),months_between(timestamp'1997-02-28 10:30:00', date'1996-10-30'),\n"
                        + "  to_utc_timestamp('2016-08-31', 'Asia/Seoul'),to_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),to_utc_timestamp(date'2016-08-31', 'Asia/Seoul')\n",
                //sql2
                "select 1, sum(case when months_between('1997-02-28 10:30:00',FROM_UTC_TIMESTAMP(date'2016-08-31', 'Asia/Seoul')) > 100 then 100 else 2 end)",
                //sql3
                "select 1,2,3");

        List<List<String>> expectedAnswers = new ArrayList<List<String>>();
        expectedAnswers.add(Arrays.asList("2009-02-01", "2015-01-01", "2009-02-01", "2009-02-01", "2016-09-30",
                "2016-10-31", "2016-09-30", "2016-07-31", "2016-07-31", "2016-07-31", "2016-07-29", "2016-07-29",
                "2016-07-29", "1970-01-01 08:00:00", "2016-08-31 09:00:00", "2016-08-31 09:00:00",
                "2016-08-31 09:00:00", "3.94959677", "3.94959677", "2016-08-30 15:00:00", "2016-08-30 15:00:00",
                "2016-08-30 15:00:00"));
        expectedAnswers.add(Arrays.asList("1", "2"));
        expectedAnswers.add(Arrays.asList("1", "2", "3"));

        for (int i = 0; i < queries.size(); i++) {
            QueryResult queryResult = new QueryExec(getProject(), KylinConfig.getInstanceFromEnv())
                    .executeQuery(queries.get(i));

            List<String> resultList = queryResult.getRows().get(0);
            List<String> expectList = expectedAnswers.get(i);

            Assert.assertTrue(listElementEquals(resultList, expectList));
        }
    }

    private boolean listElementEquals(List<String> a, List<String> b) {
        if (Objects.isNull(a)) {
            return Objects.isNull(b);
        } else if (Objects.isNull(b) || a.size() != b.size()) {
            return false;
        }

        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equals(b.get(i))) {
                return false;
            }
        }

        return true;

    }
    //
    //    @Test
    //    public void testVerifyCountQuery() throws Exception {
    //        verifyResultRowColCount(KYLIN_SQL_BASE_DIR + File.separator + "sql_verifyCount");
    //    }

}
