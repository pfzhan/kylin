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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.junit.SparkTestRunner;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.CompareQueryBySuffix;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.ITable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@RunWith(SparkTestRunner.class)
@Ignore("ITKapSparderQueryTest is contained by KAPITCombinationTest")
public class ITKapSparderQueryTest extends KapTestBase {
    private static final Logger logger = LoggerFactory.getLogger(ITKapSparderQueryTest.class);
    //inherit query tests from ITKylinQueryTest
    protected String getQueryFolderPrefix() {
        return "../../kylin/kylin-it/";
    }
    @Test
    public void testIntdersectCountQuery() throws Exception {
        // cannot compare coz H2 does not support intersect count yet..
        if ("left".equalsIgnoreCase(joinType)) {
            this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_intersect_count");
        }
    }

    @Test
    public void testSingleRunQuery() throws Exception {
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql_verifyCount/query03.sql";

        File sqlFile = new File(queryFileName);
        if (sqlFile.exists()) {
            //runSQL(sqlFile, true, true);
            runSQL(sqlFile, true, false);
        }
    }

    @Test
    public void testSparderFilterVisitor() throws Exception {

        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        String queryFileName = "src/test/resources/query/sparder/filtervisitor";

        File sqlFile = new File(queryFileName);
        System.out.println(sqlFile.getAbsolutePath());

        if (sqlFile.exists()) {
            this.execAndCompQuery(queryFileName, null, true);
        }
    }

    @Test
    public void testSparder() throws Exception {
        System.setProperty("kap.query.engine.sparder-enabled", "true");
        System.setProperty("calcite.debug", "false");
        PRINT_RESULT = true;

        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

        //String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query05.sql";
        String queryFileName = "src/test/resources/query/sparder/tmp";
        //        String queryFileName = "/workspace/kyligence_workspace/KAP/kylin/kylin-it/src/test/resources/query/sql/query16.sql";
        //        String queryFileName = "/workspace/kyligence_workspace/KAP/kylin/kylin-it/src/test/resources/query/sql_computedcolumn";

        File sqlFile = new File(queryFileName);
        System.out.println(sqlFile.getAbsolutePath());

        if (sqlFile.exists()) {
            this.execAndCompQuery(queryFileName, null, true);
        }
    }

    @Ignore
    @Test
    public void testTableauProbing() throws Exception {
        batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/tableau_probing");
    }

    //h2 cannot run these queries
    @Test
    public void testH2Uncapable() throws Exception {
        batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_h2_uncapable");
    }

    //pass
    @Test
    public void testCommonQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql", null, true);
    }

    //pass
    @Test
    public void testSnowflakeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_snowflake", null, true);
    }

    //pass
    @Test
    public void testDateTimeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_datetime", null, true);
    }

    //skip
    @Test
    public void testExtendedColumnQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_extended_column", null, true);
    }

    //raw table
    @Test
    public void testLikeQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_like", null, true);
    }

    //raw table
    @Test
    public void testVerifyCountQuery() throws Exception {
        verifyResultRowColCount(getQueryFolderPrefix() + "src/test/resources/query/sql_verifyCount");
    }

    @Test
    public void testVerifyCountQueryWithPrepare() throws Exception {
        try {
            Map<String, String> toggles = Maps.newHashMap();
            toggles.put(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
            BackdoorToggles.setToggles(toggles);

            verifyResultRowColCount(getQueryFolderPrefix() + "src/test/resources/query/sql_verifyCount");

        } finally {
            BackdoorToggles.cleanToggles();

        }
    }

    @Test
    public void testVerifyContentQuery() throws Exception {
        verifyResultContent(getQueryFolderPrefix() + "src/test/resources/query/sql_verifyContent");
    }

    //pass
    @Test
    public void testOrderByQuery() throws Exception {
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_orderby", null, true);
        // FIXME
        // as of optiq 0.8, we lost metadata type with "order by" clause, e.g. sql_orderby/query01.sql
        // thus, temporarily the "order by" clause was cross out, and the needSort is set to true
        // execAndCompQuery("src/test/resources/query/sql_orderby", null, false);
    }

    //pass
    @Test
    public void testLookupQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_lookup", null, true);
    }

    // pass
    @Test
    public void testJoinCastQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_join", null, true);
    }

    //pass
    @Test
    public void testUnionQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_union", null, true);
    }

    //pass
    @Test
    public void testCachedQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_cache", null, true);
    }

    //pass
    @Test
    public void testDerivedColumnQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_derived", null, true);
    }

    @Test
    public void testDistinctCountQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct");
        }
    }

    //pass
    @Test
    public void testComputedColumnsQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_computedcolumn", null, true,
                CompareQueryBySuffix.INSTANCE);
    }

    //skip
    @Test
    public void testTopNQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            this.execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_topn", null, true);
        }
    }

    //pass
    @Test
    public void testPreciselyDistinctCountQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_precisely", null, true);
        }
    }

    //skip
    @Test
    public void testIntersectCountQuery() throws Exception {
        // cannot compare coz H2 does not support intersect count yet..
        if ("left".equalsIgnoreCase(joinType)) {
            this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_intersect_count");
        }
    }

    //pass
    @Test
    public void testMultiModelQuery() throws Exception {
        if ("left".equalsIgnoreCase(joinType)) {
            joinType = "default";
            execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_multi_model", null, true);
            joinType = "left";
        }
    }

    // pass
    @Test
    public void testDimDistinctCountQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_distinct_dim", null, true);
    }

    @Ignore
    @Test
    public void testStreamingTableQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_streaming", null, true);
    }

    @Ignore
    @Test
    public void testTableauQuery() throws Exception {
        execAndCompResultSize(getQueryFolderPrefix() + "src/test/resources/query/sql_tableau", null, true);
    }

    @Test
    public void testSubQuery() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_subquery", null, true);
    }

    // pass
    @Test
    public void testCaseWhen() throws Exception {
        execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_casewhen", null, true);
    }

    // pass
    @Test
    public void testDynamicQuery() throws Exception {
        execAndCompDynamicQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_dynamic", null, true);
    }

    // raw
    @Test
    public void testLimitEnabled() throws Exception {
        List<File> sqlFiles = getFilesFromFolder(
                new File(getQueryFolderPrefix() + "src/test/resources/query/sql_limit"), ".sql");
        for (File sqlFile : sqlFiles) {
            runSQL(sqlFile, false, false);
            assertTrue(checkFinalPushDownLimit());
        }
    }

    // raw
    @Test
    public void testLimitCorrectness() throws Exception {
        this.execLimitAndValidate(getQueryFolderPrefix() + "src/test/resources/query/sql");
    }

    // raw
    @Test
    public void testRawQuery() throws Exception {
        this.execAndCompQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_raw", null, true);
    }

    @Test
    public void testGroupingQuery() throws Exception {
        // cannot compare coz H2 does not support grouping set yet..
        this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_grouping");
    }

    @Test
    public void testWindowQuery() throws Exception {
        // cannot compare coz H2 does not support window function yet..
        this.batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_window");
    }

    @Test
    public void testVersionQuery() throws Exception {
        String expectVersion = KylinVersion.getCurrentVersion().toString();
        logger.info("---------- verify expect version: " + expectVersion);

        String queryName = "QueryKylinVersion";
        String sql = "SELECT VERSION() AS version";

        // execute Kylin
        logger.info("Query Result from Kylin - " + queryName);
        IDatabaseConnection kylinConn = new DatabaseConnection(cubeConnection);
        ITable kylinTable = executeQuery(kylinConn, queryName, sql, false);
        String queriedVersion = String.valueOf(kylinTable.getValue(0, "version"));

        // compare the result
        Assert.assertEquals(expectVersion, queriedVersion);
    }

    @Test
    public void testPercentileQuery() throws Exception {
        batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/query/sql_percentile");
    }
}
