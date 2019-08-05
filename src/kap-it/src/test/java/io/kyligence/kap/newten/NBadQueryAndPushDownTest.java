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
package io.kyligence.kap.newten;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import lombok.val;

public class NBadQueryAndPushDownTest extends NLocalWithSparkSessionTest {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private final static String PROJECT_NAME = "bad_query_test";
    private final static String DEFAULT_PROJECT_NAME = "default";

    @Override
    public String getProject() {
        return PROJECT_NAME;
    }

    @After
    public void teardown() {
        NDefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testTableNotFoundInDatabase() throws Exception {
        //from tpch database
        final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        try {
            NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        } catch (Exception sqlException) {
            Assert.assertTrue(sqlException instanceof SQLException);
            Assert.assertTrue(ExceptionUtils.getRootCause(sqlException) instanceof SqlValidatorException);
        }
    }

    @Test
    public void testPushDownToNonExistentDB() throws Exception {
        //from tpch database
        try {
            final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            pushDownSql(getProject(), sql, 0, 0,
                    new SQLException(new NoRealizationFoundException("testPushDownToNonExistentDB")));
        } catch (Exception e) {
            Assert.assertTrue(ExceptionUtils.getRootCause(e) instanceof NoSuchTableException);
            Assert.assertTrue(ExceptionUtils.getRootCauseMessage(e)
                    .contains("Table or view 'lineitem' not found in database 'default'"));
        }
    }

    @Test
    public void testPushDownForFileNotExist() throws Exception {
        final String sql = "select max(price) from test_kylin_fact";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        try {
            NExecAndComp.queryCubeAndSkipCompute(getProject(), sql);
        } catch (Exception sqlException) {
            if (sqlException instanceof SQLException) {
                Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("Path does not exist"));
                pushDownSql(getProject(), sql, 0, 0, (SQLException) sqlException);
            }
        }
    }

    @Test
    public void testPushDownWithSemicolonQuery() throws Exception {
        final String sql = "select 1 from test_kylin_fact;";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        pushDownSql(getProject(), sql, 10, 0,
                new SQLException(new NoRealizationFoundException("test for semicolon query push down")));
        try {
            pushDownSql(getProject(), sql, 10, 1,
                    new SQLException(new NoRealizationFoundException("test for semicolon query push down")));
        } catch (Exception sqlException) {
            Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("input 'OFFSET'"));
        }
    }

    @Test
    public void testPushDownNonEquiSql() throws Exception {
        File sqlFile = new File("src/test/resources/query/sql_pushdown/query11.sql");
        String sql = new String(Files.readAllBytes(sqlFile.toPath()), StandardCharsets.UTF_8);
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY, "");
        try {
            NExecAndComp.queryCubeAndSkipCompute(DEFAULT_PROJECT_NAME, sql);
        } catch (SQLException e) {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            pushDownSql(DEFAULT_PROJECT_NAME, sql, 0, 0, e);
        }
    }

    @Test
    public void testPushDownUdf() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter,org.apache.kylin.source.adhocquery.HivePushDownConverter");

        String prjName = "tdvt";
        // timstampDiff
        String sql = "SELECT {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")} AS \"TEMP_Test__2048215813__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")}";
        val result = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")));
        Assert.assertNotNull(result);

        //timestampAdd
        sql = "  SELECT {fn TIMESTAMPADD(SQL_TSI_DAY,1,\"CALCS\".\"DATE2\")} AS \"TEMP_Test__3825428522__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n" + "GROUP BY {fn TIMESTAMPADD(SQL_TSI_DAY,1,\"CALCS\".\"DATE2\")}";
        val result1 = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")));
        Assert.assertNotNull(result1);

        // TRUNCATE
        sql = "SELECT {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)} AS \"TEMP_Test__4269159351__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn CONVERT({fn TRUNCATE(\"CALCS\".\"NUM4\",0)}, SQL_BIGINT)}";
        val result2 = pushDownSql(prjName, sql, 0, 0,
                new SQLException(new NoRealizationFoundException("test for  query push down")));
        Assert.assertNotNull(result2);
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
            int offset, SQLException sqlException)
            throws Exception {
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), prjName, SparderEnv.getSparkSession());
        String pushdownSql = NExecAndComp.removeDataBaseInSql(sql);
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = PushDownUtil.tryPushDownSelectQuery(prjName,
                pushdownSql,
                limit, offset, "DEFAULT", sqlException, BackdoorToggles.getPrepareOnly());
        if (result == null) {
            throw sqlException;
        }
        return result;
    }
}
