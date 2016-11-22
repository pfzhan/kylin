/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package io.kyligence.kap;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.ITKylinQueryTest;
import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.raw.RawTableInstance;

//@Ignore("KAPITKylinQueryTest is contained by KAPITCombinationTest")
public class ITKapKylinQueryTest extends ITKylinQueryTest {

    protected static boolean rawTableFirst = false;

    @BeforeClass
    public static void setUp() throws Exception {
        printInfo("setUp in ITKapKylinQueryTest");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        //TODO: delete
        priorities.put(RealizationType.HYBRID, 1);
        priorities.put(RealizationType.CUBE, 1);
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        Candidate.setPriorities(priorities);
        ITKapKylinQueryTest.rawTableFirst = true;

        joinType = "left";

        setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        printInfo("tearDown in ITKapKylinQueryTest");
        Candidate.restorePriorities();
        clean();
    }

    protected static void setupAll() throws Exception {
        KylinTestBase.setupAll();

        //uncomment this to use MockedCubeSparkRPC instead of real spark
        //config.setProperty("kap.storage.columnar.spark.cube.gtstorage", "io.kyligence.kap.storage.parquet.cube.MockedCubeSparkRPC");

        //uncomment this to use MockedRawTableTableRPC instead of real spark
        //config.setProperty("kap.storage.columnar.spark.rawtable.gtstorage", "io.kyligence.kap.storage.parquet.rawtable.MockedRawTableTableRPC");
    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        ObserverEnabler.forceCoprocessorUnset();
        HBaseMetadataTestCase.staticCleanupTestMetadata();
    }

    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    /////////////////test more

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

        List<File> sqlFiles = getFilesFromFolder(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_raw"), ".sql");
        for (File sqlFile : sqlFiles) {
            runSQL(sqlFile, false, false);

            OLAPContext context = OLAPContext.getThreadLocalContexts().iterator().next();
            assertTrue(context.realization instanceof RawTableInstance);
        }
    }

    /////////////////test differently

    @Test
    public void testLimitEnabled() throws Exception {
        //test in ITKapLimitEnabledTest
    }

    protected void runTimeoutQueries() throws Exception {
        List<File> sqlFiles = getFilesFromFolder(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_timeout"), ".sql");
        for (File sqlFile : sqlFiles) {
            try {
                runSQL(sqlFile, false, false);
            } catch (SQLException e) {
                if (findRoot(e) instanceof RuntimeException) {
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
        super.testTimeoutQuery();
    }

    //raw query will be preceded by RawTable all the times
    //as compensation we additionally test timeout for raw query against cube
    @Test
    public void testTimeoutQuery2() throws Exception {
        try {

            Map<String, String> toggles = Maps.newHashMap();
            toggles.put(BackdoorToggles.DEBUG_TOGGLE_COPROCESSOR_BEHAVIOR, StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM_WITHDELAY.toString());//delay 10ms for every scan
            BackdoorToggles.setToggles(toggles);

            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.coprocessor.timeout.seconds", "3");//set timeout to 3s

            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=test_kylin_cube_with_slr_empty]");
            RemoveBlackoutRealizationsRule.blackList.add("INVERTED_INDEX[name=test_kylin_cube_without_slr_empty]");

            runTimeoutQueries();

        } finally {
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_empty]");
            RemoveBlackoutRealizationsRule.blackList.remove("INVERTED_INDEX[name=test_kylin_cube_without_slr_empty]");

            KylinConfig.getInstanceFromEnv().setProperty("kylin.query.coprocessor.timeout.seconds", "0");//set timeout to 9s 
            BackdoorToggles.cleanToggles();
        }
    }
    /////////////////test less

    //raw table does not support percentile
    @Test
    public void testPercentileQuery() throws Exception {
        if (!rawTableFirst) {
            batchExecuteQuery("src/test/resources/query/percentile");
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

    @Ignore
    @Test
    public void testKAPSinglePublicQuery() throws Exception {

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql_raw/query03.sql";
        //String queryFileName =  "src/test/resources/query/temp/temp.sql";

        File sqlFile = new File(queryFileName);
        System.out.println(sqlFile.getAbsolutePath());

        if (sqlFile.exists()) {
            runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    //don't try to ignore this test, try to clean your "temp" folder
    @Test
    public void testTempQuery() throws Exception {
        try {
            PRINT_RESULT = true;
            execAndCompQuery("src/test/resources/query/temp", null, true);
        } finally {
            PRINT_RESULT = false;
        }
    }

    @Test
    public void testLikeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_like", null, true);
    }

}
