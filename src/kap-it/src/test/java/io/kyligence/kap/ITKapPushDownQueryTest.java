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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import io.kyligence.kap.junit.SparkTestRunner;

@Ignore
@RunWith(SparkTestRunner.class)
public class ITKapPushDownQueryTest extends KapTestBase {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private static final Logger logger = LoggerFactory.getLogger(ITKapPushDownQueryTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITKapPushDownQueryTest");
        setupAll();
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_left_join_cube]");
    }

    @AfterClass
    public static void tearDown() {
        logger.info("tearDown in ITKapPushDownQueryTest");
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_left_join_cube]");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY, "");
    }

    @Test
    public void testFilterOnMeasureQuery() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query00.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();

            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            int resultCount = runSQL(sqlFile, true, false);
            Assert.assertEquals(resultCount, 1);
        }
    }

    @Test
    public void testNoMeasureQuery() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query02.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            int resultCount = runSQL(sqlFile, true, false);
            Assert.assertEquals(resultCount, 1);
        }
    }

    @Test
    public void testAggOnDimension() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query03.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            int resultCount = runSQL(sqlFile, true, false);
            Assert.assertEquals(resultCount, 1);
        }
    }

    @Test
    public void testUnMatchedJoin() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query04.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            int resultCount = runSQL(sqlFile, true, false);
            Assert.assertEquals(resultCount, 1);
        }
    }

    @Test
    public void testCalciteCostBasedRouting() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query05.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(RoutingIndicatorException.class, Throwables.getRootCause(e).getClass());
            }
        }
    }

    @Test
    public void testComputedColumnExpand() throws Exception {
        try {
            RemoveBlackoutRealizationsRule.blackList.add("CUBE[name=ci_inner_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.add("CUBE[name=ci_left_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.add("HYBRID[name=ci_inner_join_hybrid]");

            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");

            List<File> sqlFiles = getFilesFromFolder(new File("src/test/resources/query/sql_computedcolumn"), ".sql");
            for (File sqlFile : sqlFiles) {
                int resultCount = runSQL(sqlFile, false, false);
                Assert.assertTrue(resultCount > 1);
            }

        } finally {
            RemoveBlackoutRealizationsRule.blackList.remove("CUBE[name=ci_inner_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.remove("CUBE[name=ci_left_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.add("HYBRID[name=ci_inner_join_hybrid]");
        }
    }

    @Test
    public void testResultWithNull() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");

        String testSql = getTextFromFile(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_pushdown/query07.sql"));
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = tryPushDownSelectQuery(testSql);
        Assert.assertNotNull(result.getFirst());
        Assert.assertTrue(result.getFirst().size() == 1);
        Assert.assertTrue(result.getFirst().get(0).get(0) == null);
    }

    @Test
    public void testConcurrentPushDownQuery() throws Exception {
        ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<>();
        List<Thread> threadList = new ArrayList<>();

        int ThreadCount = 3;
        for (int i = 0; i < ThreadCount; i++) {
            ConcurrentPushDownQueryThread con = new ConcurrentPushDownQueryThread();
            Thread thread = new Thread(con);
            thread.start();
            threadList.add(thread);
        }

        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println(queue.size());
    }

    @Test
    public void testPushDownQueryFailed() throws Exception {
        String queryFileName = "src/test/resources/query/sql_pushdown/query06.sql";
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        File sqlFile = new File(queryFileName);

        if (sqlFile.exists()) {
            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (Exception e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertTrue(Throwables.getRootCause(e).getClass().getName().contains("StatusRuntimeException"));
            }
        }
    }

    public class ConcurrentPushDownQueryThread implements Runnable {
        @Override
        public void run() {
            String queryFileName = "src/test/resources/query/sql_pushdown/query04.sql";
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            File sqlFile = new File(queryFileName);
            if (sqlFile.exists()) {
                //runSQL(sqlFile, true, true);
                kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY, "");
                try {
                    runSQL(sqlFile, true, false);
                } catch (Exception e) {
                    logger.debug("stacktrace for the SQLException: ", e);
                    Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
                }

                kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                        "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
                int resultCount = 0;
                try {
                    resultCount = runSQL(sqlFile, true, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Assert.assertEquals(resultCount, 1);
            }
        }

    }

    @Test
    public void testPushDownUDF() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");

        String testSql = getTextFromFile(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_pushdown/query08.sql"));
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = tryPushDownSelectQuery(testSql);
        Assert.assertNotNull(result.getFirst());
        Assert.assertTrue(result.getFirst().size() == 1);
        Assert.assertTrue("0".equals(result.getFirst().get(0).get(0)));
    }

    @Test
    public void testPushDownUpdate() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.update-enabled", "true");

        String createSql = getTextFromFile(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_pushdown/query09.sql"));
        String dropSql = getTextFromFile(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_pushdown/query10.sql"));

        tryPushDownNonSelectQuery(dropSql, false);
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = tryPushDownNonSelectQuery(createSql, true);
        Assert.assertNotNull(result.getFirst());
        Assert.assertNotNull(result.getSecond());
        Assert.assertTrue(result.getFirst().size() == 0);
        Assert.assertTrue(result.getSecond().size() == 0);
        result = tryPushDownNonSelectQuery(createSql, false);
        Assert.assertNotNull(result.getFirst());
        Assert.assertNotNull(result.getSecond());
        Assert.assertTrue(result.getFirst().size() == 0);
        Assert.assertTrue(result.getSecond().size() == 0);
        tryPushDownNonSelectQuery(dropSql, false);
    }

    @Test
    public void testCountColCannotReplacedByCountStar() throws Exception {
        File sqlFile = new File("src/test/resources/query/sql_pushdown/query12.sql");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY, "");
        try {
            runSQL(sqlFile, true, false);
        } catch (SQLException e) {
            logger.debug("stacktrace for the SQLException: ", e);
            Assert.assertEquals(NoRealizationFoundException.class, Throwables.getRootCause(e).getClass());
        }
    }
}
