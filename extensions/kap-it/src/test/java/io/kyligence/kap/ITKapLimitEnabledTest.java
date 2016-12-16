/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.kylin.query.KylinTestBase;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ITKapLimitEnabledTest extends KylinTestBase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        printInfo("setUp in ITLimitEnabledTest");
        setupAll();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        printInfo("tearDown in ITLimitEnabledTest");
        clean();
    }

    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    @Test
    public void testLimitEnabled() throws Exception {
        try {
            RemoveBlackoutRealizationsRule.whiteList.add("INVERTED_INDEX[name=test_kylin_cube_with_slr_empty]");

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query01.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query02.sql"), false, false);
            //raw table cannot not enable limit for aggregated queries
            assertFalse(checkFinalPushDownLimit());

            //sql limit in kap query folder
            runSQL(new File("src/test/resources/query/sql_limit/query01.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            RemoveBlackoutRealizationsRule.whiteList.remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_empty]");

            RemoveBlackoutRealizationsRule.whiteList.add("CUBE[name=test_kylin_cube_with_slr_empty]");

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query01.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query02.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            try {
                //sql limit in kap query folder
                runSQL(new File("src/test/resources/query/sql_limit/query01.sql"), false, false);
                assertTrue(false);
            } catch (Exception exception) {
                //expected
            }

            RemoveBlackoutRealizationsRule.whiteList.remove("CUBE[name=test_kylin_cube_with_slr_empty]");

        } finally {
            RemoveBlackoutRealizationsRule.whiteList.remove("CUBE[name=test_kylin_cube_with_slr_empty]");
            RemoveBlackoutRealizationsRule.whiteList.remove("INVERTED_INDEX[name=test_kylin_cube_with_slr_empty]");
        }
    }

}
