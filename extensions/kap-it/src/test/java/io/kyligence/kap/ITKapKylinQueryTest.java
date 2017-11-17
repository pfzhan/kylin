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

import static io.kyligence.kap.KapTestBase.initQueryEngine;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.CompareQueryBySuffix;
import org.apache.kylin.query.H2Database;
import org.apache.kylin.query.ITKylinQueryTest;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.junit.SparkTestRunner;

@RunWith(SparkTestRunner.class)
@Ignore("KAPITKylinQueryTest is contained by KAPITCombinationTest")
public class ITKapKylinQueryTest extends ITKylinQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(ITKapKylinQueryTest.class);

    protected static boolean rawTableFirst = false;

    protected static void setupAll() throws Exception {
        initQueryEngine();
        //setup env
        HBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //setup cube conn
        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        cubeConnection = QueryConnection.getConnection(project);

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++) + ";CACHE_SIZE=32072", "sa",
                "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config, project);
        h2DB.loadAllTables();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITKapKylinQueryTest");
        configure("left", false);
        setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {

        logger.info("tearDown in ITKapKylinQueryTest");
        Candidate.restorePriorities();
        clean();
    }

    @Before
    public void initConn() throws SQLException {
        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        cubeConnection = QueryConnection.getConnection(project);
    }

    protected static void configure(String joinType, Boolean rawTableFirst) {
        if (rawTableFirst) {
            Map<RealizationType, Integer> priorities = Maps.newHashMap();
            priorities.put(RealizationType.HYBRID, 1);
            priorities.put(RealizationType.CUBE, 1);
            priorities.put(RealizationType.INVERTED_INDEX, 0);
            Candidate.setPriorities(priorities);
            ITKapKylinQueryTest.rawTableFirst = true;
        } else {
            Map<RealizationType, Integer> priorities = Maps.newHashMap();
            priorities.put(RealizationType.HYBRID, 0);
            priorities.put(RealizationType.CUBE, 0);
            priorities.put(RealizationType.INVERTED_INDEX, 0);
            Candidate.setPriorities(priorities);
            ITKapKylinQueryTest.rawTableFirst = false;
        }

        ITKapKylinQueryTest.joinType = joinType;

        logger.info("Into combination joinType=" + joinType + ", rawTableFirst=" + rawTableFirst);
    }

    protected static void clean() {
        KylinTestBase.clean();
    }

    //h2 cannot run these queries
    @Test
    public void testH2Uncapable() throws Exception {
        batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_h2_uncapable");
    }

    @Test
    public void testSubQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_subquery", null, true);
    }

    @Test
    public void testLookupQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_lookup", null, true);
    }

    @Test
    public void testDateTimeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_datetime", null, true);
    }

    @Test
    public void testDynamicQuery() throws Exception {
        execAndCompDynamicQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_dynamic", null, true);
    }

    @Test
    public void testDimDistinctCountQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_dim", null, true);
    }

    @Test
    public void testLikeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_like", null, true);
    }

    @Test
    public void testPreciselyDistinctCountQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_precisely", null, true);
        }
    }

    @Test
    public void testDerivedColumnQuery() throws Exception {

        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_derived", null, true);
    }

    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    /////////////////test more

    // due to computed column, the CI cubes in KAP are different in column count
    @Test
    @Override
    public void testVerifyCountQuery() throws Exception {
        verifyResultRowColCount("src/test/resources/query/sql_verifyCount");
    }

    // due to computed column, the CI cubes in KAP are different in column count
    @Test
    @Override
    public void testVerifyCountQueryWithPrepare() throws Exception {
        try {
            Map<String, String> toggles = Maps.newHashMap();
            toggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
            BackdoorToggles.setToggles(toggles);

            verifyResultRowColCount("src/test/resources/query/sql_verifyCount");

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    @Test
    public void testPowerBiQuery() throws Exception {
        this.execAndCompQuery("src/test/resources/query/sql_powerbi", null, true);
    }

    //only raw can do
    @Test
    public void testRawTableQuery() throws Exception {
        if (rawTableFirst)
            this.execAndCompQuery("src/test/resources/query/sql_rawtable", null, true);
    }

    //only raw can support execAndCompQuery
    @Test
    public void testDistinctCountQueryExt() throws Exception {
        if (rawTableFirst)
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct", null, true);
    }

    //in ITKylinQueryTest only left join case
    @Test
    public void testTopNQueryExt() throws Exception {
        if (rawTableFirst)
            this.execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_topn", null, true);
    }

    //in ITKylinQueryTest only left join case
    @Test
    public void testPreciselyDistinctCountQueryExt() throws Exception {
        if (rawTableFirst)
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_precisely", null, true);
    }

    @Override
    public void testIntersectCountQuery() throws Exception {
        // skip, has conflict with raw table, and Kylin CI has covered
    }

    @Test
    public void testRawTablePrecedesCubeOnRawQueries() throws Exception {

        List<File> sqlFiles = getFilesFromFolder(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_raw"),
                ".sql");
        for (File sqlFile : sqlFiles) {
            runSQL(sqlFile, false, false);

            OLAPContext context = OLAPContext.getThreadLocalContexts().iterator().next();
            assertTrue(context.realization instanceof RawTableInstance);
        }
    }

    @Test
    public void testComputedColumnsQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_computedcolumn", null, true, CompareQueryBySuffix.INSTANCE);
    }

    /////////////////test differently

    @Test
    public void testLimitEnabled() throws Exception {
        //test in ITKapLimitEnabledTest
    }

    protected void runTimeoutQueries() throws Exception {
        List<File> sqlFiles = getFilesFromFolder(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_timeout"), ".sql");
        for (File sqlFile : sqlFiles) {
            try {
                runSQL(sqlFile, false, false);
            } catch (SQLException e) {
                String x = Throwables.getStackTraceAsString(e);
                if (x.contains("KylinTimeoutException")) {
                    //expected
                    continue;
                }
            }
            throw new RuntimeException("Not expected ");
        }
    }

    protected void runNoAggQuery() throws Exception {
        List<File> sqlFiles = getFilesFromFolder(new File("src/test/resources/query/sql_noagg"), ".sql");
        for (File sqlFile : sqlFiles) {
            try {
                runSQL(sqlFile, false, false);
            } catch (SQLException e) {
                String x = Throwables.getStackTraceAsString(e);
                if (x.contains("No realization")) {
                    //expected
                    continue;
                }
            }
            throw new RuntimeException("Not expected ");
        }
    }

    @Test
    @Override
    public void testTimeoutQuery() throws Exception {
        if (!"true".equalsIgnoreCase(System.getProperty("sparder.enabled"))) {
            try {
                KylinConfig.getInstanceFromEnv().setProperty("kap.storage.columnar.spark-visit-timeout-ms", "3000");//set timeout to 3s
                super.testTimeoutQuery();
            } finally {
                KylinConfig.getInstanceFromEnv().setProperty("kap.storage.columnar.spark-visit-timeout-ms", "300000");//set timeout to default
            }
        }
    }

    //raw query will be preceded by RawTable all the times
    //as compensation we additionally test timeout for raw query against cube
    @Test
    public void testTimeoutQuery2() throws Exception {
        if (!"true".equalsIgnoreCase(System.getProperty("sparder.enabled"))) {
            try {
                Map<String, String> toggles = Maps.newHashMap();
                toggles.put(BackdoorToggles.DEBUG_TOGGLE_COPROCESSOR_BEHAVIOR,
                        StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM_WITHDELAY.toString());//delay 10ms for every scan
                BackdoorToggles.setToggles(toggles);

                KylinConfig.getInstanceFromEnv().setProperty("kap.storage.columnar.spark-visit-timeout-ms", "3000");//set timeout to 3s

                RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_inner_join_cube]");
                RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_left_join_cube]");

                runTimeoutQueries();

            } finally {
                RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
                RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_left_join_cube]");

                KylinConfig.getInstanceFromEnv().setProperty("kap.storage.columnar.spark-visit-timeout-ms", "300000");//set timeout to default
                BackdoorToggles.cleanToggles();
            }
        }
    }
    /////////////////test less

    //raw table does not support percentile
    @Test
    public void testPercentileQuery() throws Exception {
        if (!rawTableFirst) {
            super.testPercentileQuery();
        }
    }

    @Test
    public void testDisableCubeForNoAggQuery() throws Exception {
        try {
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.disable-cube-noagg-sql", "true");
            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_inner_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=ci_left_join_cube]");
            runNoAggQuery();
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.disable-cube-noagg-sql", "false");
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=ci_left_join_cube]");
        }
    }

    @Ignore("dev only")
    @Test
    public void testKAPSingleInternalQuery() throws Exception {

        try {
            String queryFileName = "src/test/resources/query/temp/temp.sql";

            File sqlFile = new File(queryFileName);
            if (sqlFile.exists()) {
                //runSQL(sqlFile, true, true);
                //runSQL(sqlFile, true, false);
                runSQL(sqlFile, false, false);
            }
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            throw e;
        }
    }

    @Test
    public void testKAPSinglePublicQuery() throws Exception {
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql_derived/query10.sql";

        //String queryFileName = "src/test/resources/query/sparder/tmp/query60.sql";
        //String queryFileName = "src/test/resources/query/sql/query60.sql";

        File sqlFile = new File(queryFileName);
        System.out.println(sqlFile.getAbsolutePath());

        if (sqlFile.exists()) {
            runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    // don't try to ignore this test, try to clean your "temp" folder
    @Test
    public void testTempQuery() throws Exception {
        try {
            PRINT_RESULT = true;
            execAndCompQuery("src/test/resources/query/temp", null, true);
        } finally {
            PRINT_RESULT = false;
        }
    }

    @Override
    @Test
    public void testSnowflakeQuery() throws Exception {
        if (!rawTableFirst) {
            super.testSnowflakeQuery();
        }
    }

    @Test
    public void testUnionQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_union", null, true);
    }

}
