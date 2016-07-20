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

import java.io.File;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.H2Database;
import org.apache.kylin.query.ITKylinQueryTest;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Maps;

//@Ignore("KAPITKylinQueryTest is contained by KAPITCombinationTest")
public class ITKapKylinQueryTest extends ITKylinQueryTest {

    @BeforeClass
    public static void setUp() throws Exception {
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);

        printInfo("setUp in ITKapKylinQueryTest");
        joinType = "inner";

        setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Candidate.restorePriorities();
        printInfo("tearDown in ITKapKylinQueryTest");
        clean();
    }

    protected static void setupAll() throws Exception {
        //setup env
        KAPHBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

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

    //inherit query tests from ITKylinQueryTest

    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    // unique query tests in kap
    @Test
    public void testPercentileQuery() throws Exception {
        batchExecuteQuery("src/test/resources/query/percentile");
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

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query04.sql";

        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    protected static void clean() {
        if (cubeConnection != null)
            closeConnection(cubeConnection);
        if (h2Connection != null)
            closeConnection(h2Connection);

        ObserverEnabler.forceCoprocessorUnset();
        KAPHBaseMetadataTestCase.staticCleanupTestMetadata();
    }

}
