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

import io.kyligence.kap.util.ExecAndComp;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.exception.QueryErrorCode;
import org.apache.kylin.query.util.PushDownUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Throwables;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import lombok.val;

public class NBadQueryAndPushDownTest extends NLocalWithSparkSessionTest {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private static final String PUSHDOWN_ENABLED = "kylin.query.pushdown-enabled";
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
    }

    @Test
    public void testTableNotFoundInDatabase() throws Exception {
        //from tpch database
        final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        try {
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception sqlException) {
            Assert.assertTrue(sqlException instanceof SQLException);
            Assert.assertTrue(ExceptionUtils.getRootCause(sqlException) instanceof SqlValidatorException);
        }
    }

    @Test
    public void testPushdownCCWithFn() throws Exception {
        final String sql = "select LSTG_FORMAT_NAME,sum(NEST4) from TEST_KYLIN_FACT where ( not { fn convert( \"LSTG_FORMAT_NAME\", SQL_WVARCHAR ) } = 'ABIN' or \"LSTG_FORMAT_NAME\" is null) group by LSTG_FORMAT_NAME limit 1";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");

        // success
        pushDownSql(DEFAULT_PROJECT_NAME, sql, 10, 0, null, true);

        // failed for wrong pushdown converter order
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.SparkSQLFunctionConverter,org.apache.kylin.query.util.PowerBIConverter,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.security.RowFilter,io.kyligence.kap.query.security.HackSelectStarWithColumnACL");
        try {
            pushDownSql(DEFAULT_PROJECT_NAME, sql, 10, 0, null, true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof AnalysisException);
            Assert.assertTrue(e.getMessage().contains("cannot resolve 'NEST4' given input columns"));
        }
    }

    @Ignore
    public void testPushDownToNonExistentDB() throws Exception {
        //from tpch database
        try {
            final String sql = "select * from lineitem where l_orderkey = o.o_orderkey and l_commitdate < l_receiptdate";
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
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
            ExecAndComp.queryModelWithoutCompute(getProject(), sql);
        } catch (Exception sqlException) {
            if (sqlException instanceof SQLException) {
                Assert.assertTrue(ExceptionUtils.getRootCauseMessage(sqlException).contains("Path does not exist"));
                pushDownSql(getProject(), sql, 0, 0, (SQLException) sqlException);
            }
        }
    }

    @Ignore
    public void testPushDownWithSemicolonQuery() throws Exception {
        final String sql = "select 1 from test_kylin_fact;";
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
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
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter");
        File sqlFile = new File("src/test/resources/query/sql_pushdown/query11.sql");
        String sql = new String(Files.readAllBytes(sqlFile.toPath()), StandardCharsets.UTF_8);
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "false");
        try {
            ExecAndComp.queryModelWithoutCompute(DEFAULT_PROJECT_NAME, sql);
        } catch (Exception e) {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                    "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
            pushDownSql(DEFAULT_PROJECT_NAME, sql, 0, 0, (SQLException) e);
        }
    }

    @Ignore
    public void testPushDownUdf() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter");

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

    @Test
    public void testPushDownForced() throws Exception {
        //test for KE-14218

        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_RUNNER_KEY,
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter");

        String prjName = "tdvt";
        // timstampDiff
        String sql = "SELECT {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")} AS \"TEMP_Test__2048215813__0_\"\n"
                + "FROM \"CALCS\" \"CALCS\"\n"
                + "GROUP BY {fn TIMESTAMPDIFF(SQL_TSI_DAY,{d '1900-01-01'},\"CALCS\".\"DATE0\")}";
        val resultForced = pushDownSql(prjName, sql, 0, 0, null, true);
        Assert.assertNotNull(resultForced);

        //test for error when  execption is null and is not forced
        try {
            pushDownSql(prjName, sql, 0, 0, null, false);
            Assert.fail();
        } catch (Exception e) {
            Throwable rootCause = Throwables.getRootCause(e);
            Assert.assertTrue(rootCause instanceof IllegalArgumentException);
        }

        //test for error when  pushdown turn off, and force to push down
        try {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "false");
            Assert.assertFalse(KylinConfig.getInstanceFromEnv().isPushDownEnabled());
            pushDownSql(prjName, sql, 0, 0, null, true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(((KylinException) e).getErrorCode(),
                    QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN.toErrorCode());
            Assert.assertEquals(Throwables.getRootCause(e).getMessage(),
                    "you should turn on pushdown when you want to force to pushdown");
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(PUSHDOWN_ENABLED, "true");
        }
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
            int offset, SQLException sqlException) throws Exception {
        return pushDownSql(prjName, sql, limit, offset, sqlException, false);
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownSql(String prjName, String sql, int limit,
            int offset, SQLException sqlException, boolean isForced) throws Exception {
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), prjName, SparderEnv.getSparkSession());
        String pushdownSql = ExecAndComp.removeDataBaseInSql(sql);
        String massagedSql = QueryUtil.normalMassageSql(KylinConfig.getInstanceFromEnv(), pushdownSql, limit, offset);
        QueryParams queryParams = new QueryParams(prjName, massagedSql, "DEFAULT", BackdoorToggles.getPrepareOnly(),
                sqlException, isForced, true, limit, offset);
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = PushDownUtil.tryPushDownQuery(queryParams);
        if (result == null) {
            throw sqlException;
        }
        return result;
    }
}
