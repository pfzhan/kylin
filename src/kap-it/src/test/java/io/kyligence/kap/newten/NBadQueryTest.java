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

public class NBadQueryTest extends NLocalWithSparkSessionTest {
    private static final String PUSHDOWN_RUNNER_KEY = "kylin.query.pushdown.runner-class-name";
    private final static String PROJECT_NAME = "bad_query_test";

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
            Assert.assertTrue(
                    ExceptionUtils.getRootCause(e) instanceof NoSuchTableException);
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

    private void pushDownSql(String prjName, String sql, int limit, int offset, SQLException sqlException)
            throws Exception {
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), prjName, SparderEnv.getSparkSession());
        Pair<List<List<String>>, List<SelectedColumnMeta>> result = PushDownUtil.tryPushDownSelectQuery(prjName, sql,
                limit, offset,
                "DEFAULT", sqlException, BackdoorToggles.getPrepareOnly());
        if (result == null) {
            throw sqlException;
        }
    }
}
