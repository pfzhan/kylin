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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.junit.SparkTestRunner;

@RunWith(SparkTestRunner.class)
public class ITKapLimitEnabledTest extends KapTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ITKapLimitEnabledTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITLimitEnabledTest");
        joinType = "inner";
        System.setProperty("sparder.enabled", "false");
        setupAll();
        config.setProperty("kylin.query.pushdown.runner-class-name", "");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in ITLimitEnabledTest");
        clean();
    }

    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }

    @Test
    public void testLimitEnabled() throws Exception {
        try {
            RemoveBlackoutRealizationsRule.whiteList.add("INVERTED_INDEX[name=ci_inner_join_cube]");

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query01.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            runSQL(new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit/query02.sql"), false, false);
            //raw table cannot not enable limit for aggregated queries
            assertFalse(checkFinalPushDownLimit());

            //sql limit in kap query folder
            runSQL(new File("src/test/resources/query/sql_limit/query01.sql"), false, false);
            assertTrue(checkFinalPushDownLimit());

            RemoveBlackoutRealizationsRule.whiteList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");

            RemoveBlackoutRealizationsRule.whiteList.add("CUBE[name=ci_inner_join_cube]");

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

            RemoveBlackoutRealizationsRule.whiteList.remove("CUBE[name=ci_inner_join_cube]");

        } finally {
            RemoveBlackoutRealizationsRule.whiteList.remove("CUBE[name=ci_inner_join_cube]");
            RemoveBlackoutRealizationsRule.whiteList.remove("INVERTED_INDEX[name=ci_inner_join_cube]");
        }
    }

}
