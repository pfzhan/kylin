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
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.query.routing.RoutingIndicatorException;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ITKapPushDownQueryTest extends KylinTestBase {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private static final Logger logger = LoggerFactory.getLogger(ITKapPushDownQueryTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITKapPushDownQueryTest");
        System.setProperty("needCheckCC", "true");
        KylinTestBase.setupAll();
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_left_join_cube]");
    }

    @AfterClass
    public static void tearDown() {
        logger.info("tearDown in ITKapPushDownQueryTest");
        System.clearProperty("needCheckCC");
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
        RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_left_join_cube]");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY, "");
    }

    @Test
    public void testFilterOnMeasureQuery() throws Exception {

        String queryFileName = "src/test/resources/query/sql_pushdown/query01.sql";
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
                Assert.assertEquals(NoRealizationFoundException.class, findRoot(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
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
                Assert.assertEquals(NoRealizationFoundException.class, findRoot(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
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
                Assert.assertEquals(NoRealizationFoundException.class, findRoot(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
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
                Assert.assertEquals(NoRealizationFoundException.class, findRoot(e).getClass());
            }

            kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
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
            System.setProperty("kap.query.calcite-join-threshold", "0");
            try {
                runSQL(sqlFile, true, false);
                throw new SQLException();
            } catch (SQLException e) {
                logger.debug("stacktrace for the SQLException: ", e);
                Assert.assertEquals(RoutingIndicatorException.class, findRoot(e).getClass());
            } finally {
                System.clearProperty("kap.query.calcite-join-threshold");
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
                    "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");

            List<File> sqlFiles = getFilesFromFolder(
                    new File("src/test/resources/query/sql_computedcolumn"), ".sql");
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
                    Assert.assertEquals(NoRealizationFoundException.class, findRoot(e).getClass());
                }

                kylinConfig.setProperty(PUSHDOWN_RUNNER_KEY,
                        "io.kyligence.kap.storage.parquet.adhoc.PushDownRunnerSparkImpl");
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
}
