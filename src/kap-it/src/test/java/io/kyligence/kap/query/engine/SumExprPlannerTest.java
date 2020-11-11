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
package io.kyligence.kap.query.engine;

import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.staticCleanupTestMetadata;
import static io.kyligence.kap.common.util.NLocalFileMetadataTestCase.staticCreateTestMetadata;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.query.rules.CalciteRuleTestBase;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumExprPlannerTest extends CalciteRuleTestBase {

    static final String defaultProject = "default";
    static final  DiffRepository diff = DiffRepository.lookup(SumExprPlannerTest.class);

    private void openSumCaseWhen() {
        // we must make sure kap.query.enable-convert-sum-expression is TRUE to
        // avoid adding SumConstantConvertRule in PlannerFactory
        System.setProperty("kylin.query.convert-sum-expression-enabled", "true");
    }

    private void closeSumCaseWhen() {
        // some sql failed in new SumConstantConvertRule
        System.setProperty("kylin.query.convert-sum-expression-enabled", "false");
    }

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    @Test
    @Ignore("For development")
    public void dumpPlans() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject, "query/sql_sum_expr");
        CalciteRuleTestBase.StringOutput output = new CalciteRuleTestBase.StringOutput(false);
        queries.forEach(e ->checkSQL(defaultProject, e.getSecond(), e.getFirst(), output, diff));
        output.dump(log);
    }

    @Test
    public void testAllCases() throws IOException {
        openSumCaseWhen();
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject, "query/sql_sum_expr");
        Assert.assertEquals("Please adjust expected value, if SQLs are added or removed ", 31, queries.size());
        queries.forEach(e ->checkSQL(defaultProject, e.getSecond(), e.getFirst(), null, diff));
    }

    @Test
    public void testSimpleSQL() {
        openSumCaseWhen();
        String SQL =
                "SELECT " +
                        "SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE 2 END) " +
                        "FROM TEST_KYLIN_FACT";
        checkSQL(defaultProject, SQL, null, null, diff);
    }

    /**
     * see https://olapio.atlassian.net/browse/KE-14512
     */
    @Test
    public void testWithAVG() {
        openSumCaseWhen();
        String SQL =
                "SELECT " +
                        "AVG(PRICE) as price1 " +
                        ",SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE 0 END) as total_price " +
                        "from TEST_KYLIN_FACT";
        checkSQL(defaultProject, SQL, null, null, diff);
    }

    @Test
    public void testKE13524() throws IOException {
        // see https://olapio.atlassian.net/browse/KE-13524 for details
        closeSumCaseWhen();
        String project = "newten";
        Pair<String, String> query = readOneSQL(KylinConfig.getInstanceFromEnv(), project, "sql_sinai_poc", "query15.sql");
        Assert.assertNotNull(query.getSecond());
        Assert.assertNotNull(toCalcitePlan(project, query.getSecond(), KylinConfig.getInstanceFromEnv()));

        String SQL = "select sum(2), sum(0), count(1) from POPHEALTH_ANALYTICS.Z_PROVDASH_UM_ED";
        Assert.assertNotNull(toCalcitePlan(project, SQL, KylinConfig.getInstanceFromEnv()));

    }
}