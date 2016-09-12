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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.H2Database;
import org.apache.kylin.query.ITKylinQueryTest;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
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
        //setup env
        KAPHBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //uncomment this to use MockedCubeSparkRPC instead of real spark
        //config.setProperty("kap.storage.columnar.spark.cube.gtstorage", "io.kyligence.kap.storage.parquet.cube.MockedCubeSparkRPC");

        //uncomment this to use MockedRawTableTableRPC instead of real spark
        //config.setProperty("kap.storage.columnar.spark.rawtable.gtstorage", "io.kyligence.kap.storage.parquet.rawtable.MockedRawTableTableRPC");

        //setup cube conn
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(ProjectInstance.DEFAULT_PROJECT_NAME, config);
        Properties props = new Properties();
        props.setProperty(OLAPQuery.PROP_SCAN_THRESHOLD, "10001");
        cubeConnection = DriverManager.getConnection("jdbc:calcite:model=" + olapTmp.getAbsolutePath(), props);

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++), "sa", "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config);
        h2DB.loadAllTables();

    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        ObserverEnabler.forceCoprocessorUnset();
        KAPHBaseMetadataTestCase.staticCleanupTestMetadata();
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

    @Test
    public void testRawTablePrecedesCubeOnRawQueries() throws Exception {
        this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_raw");
        OLAPContext context = OLAPContext.getThreadLocalContexts().iterator().next();
        assertTrue(context.realization instanceof RawTableInstance);
    }

    /////////////////test less

    // parquet storage(including cube and raw table) does not support timeout/limit pushdown now
    @Test(expected = SQLException.class)
    public void testTimeoutQuery() throws Exception {
        runTimetoutQueries();
    }

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

    @Test
    public void testTempQuery() throws Exception {
        PRINT_RESULT = true;
        execAndCompQuery("src/test/resources/query/temp", null, true);
        PRINT_RESULT = false;
    }

}
