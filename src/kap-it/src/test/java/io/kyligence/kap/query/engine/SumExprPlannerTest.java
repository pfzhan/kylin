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

import com.google.common.base.Strings;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.query.util.HepUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.test.DiffRepository;
import org.apache.commons.io.FilenameUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.util.QueryUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.notNullValue;

@Slf4j
public class SumExprPlannerTest extends NLocalFileMetadataTestCase {

    /**
     * Use <code>StringOutput</code> to dump all plans in xml format
     *
     * <p>If <code>separate</code> is true, planBefore and planAfter are dumped separately,
     * and we can compare plans before and after optimization.
     */
    static class StringOutput {
        boolean separate;

        StringBuilder builderBefore = new StringBuilder();
        StringBuilder builderAfter = new StringBuilder();

        public StringOutput(boolean separate) {
            this.separate = separate;
        }
        public void dump(Logger log){
            log.debug("planBefore: {}{}", NL, builderBefore.toString());
            if (separate)
                log.debug("planAfter: {}{}", NL, builderAfter.toString());
        }
        private void output(StringBuilder builder, String name , String plan){
            builder.append("        <Resource name=\"").append(name).append("\">").append(NL);
            builder.append("            <![CDATA[");
            builder.append(plan);
            builder.append("]]>").append(NL);
            builder.append("        </Resource>").append(NL);
        }
        public void output(RelNode relBefore, RelNode relAfter, String prefix){
            String before = Strings.isNullOrEmpty(prefix) ? "planBefore"  : prefix + ".planBefore";
            final String planBefore = NL + RelOptUtil.toString(relBefore);
            output(builderBefore,
                    separate ? prefix : before,
                    planBefore);

            String after = Strings.isNullOrEmpty(prefix) ? "planAfter"  : prefix + ".planAfter";
            final String planAfter = NL + RelOptUtil.toString(relAfter);
            output(separate ? builderAfter : builderBefore,
                    separate ? prefix       : after,
                    planAfter);
        }
    }

    static final String NL = System.getProperty("line.separator");
    static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/";
    static final String defaultProject = "default";
    static final  DiffRepository diff = DiffRepository.lookup(SumExprPlannerTest.class);
    static final String emptyLinePattern = "(?m)^[ \t]*\r?\n";

    private void openSumCaseWhen() {
        // we must make sure kap.query.enable-convert-sum-expression is TRUE to
        // avoid adding SumConstantConvertRule in PlannerFactory
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "true");
    }

    private void closeSumCaseWhen() {
        // some sql failed in new SumConstantConvertRule
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "false");
    }

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    private List<Pair<String, String>> reaALLSQLs(String project, String folder, String file) throws IOException {
        final String queryFolder = IT_SQL_KAP_DIR + folder;
        return NExecAndComp
                .fetchQueries(queryFolder)
                .stream().filter(e -> {
                    if (Strings.isNullOrEmpty(file))
                        return true;
                    else
                        return e.getFirst().contains(file);})
                .map(e-> {
                    String sql = QueryUtil
                            .massageSql(e.getSecond(), project, 0, 0, "DEFAULT", false)
                            .replaceAll(emptyLinePattern, ""); // remove empty line
                     return  new Pair<>(FilenameUtils.getBaseName(e.getFirst()), sql);})
                .collect(Collectors.toList());
    }
    private static RelNode toCalcitePlan(String project, String SQL) {
        ProjectSchemaFactory ps = new ProjectSchemaFactory(project, KylinConfig.getInstanceFromEnv());
        QueryExec qe = new QueryExec(KylinConfig.getInstanceFromEnv(), ps);
        try {
            return qe.parseAndOptimize(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }
    static void checkDiff(RelNode relBefore, RelNode relAfter, String prefix){
        String before = Strings.isNullOrEmpty(prefix) ? "planBefore"  : prefix + ".planBefore";
        String beforeExpected = "${" + before + "}";
        final String planBefore = NL + RelOptUtil.toString(relBefore);
        diff.assertEquals(before, beforeExpected, planBefore);

        String after = Strings.isNullOrEmpty(prefix) ? "planAfter"  : prefix + ".planAfter";
        String afterExpected = "${" + after + "}";
        final String planAfter = NL + RelOptUtil.toString(relAfter);
        diff.assertEquals(after, afterExpected, planAfter);
    }
    static void checkSQL(String project, String sql, String prefix, StringOutput StrOut) {
        RelNode relBefore = toCalcitePlan(project, sql);
        Assert.assertThat(relBefore, notNullValue());
        RelNode relAfter = HepUtils.runRuleCollection(relBefore, HepUtils.SumExprRule);
        Assert.assertThat(relAfter, notNullValue());
        log.debug("check plan for {}.sql: {}{}", prefix, NL, sql);

        if(StrOut != null){
            StrOut.output(relBefore, relAfter, prefix);
        } else {
            checkDiff(relBefore, relAfter, prefix);
        }
    }
    @Test
    @Ignore("For development")
    public void dumpPlans() throws IOException {
        List<Pair<String, String>> queries = reaALLSQLs(defaultProject, "query/sql_sum_expr", null);
        StringOutput output = new StringOutput(false);
        queries.forEach(e ->checkSQL(defaultProject, e.getSecond(), e.getFirst(), output));
        output.dump(log);
    }

    @Test
    public void testAllCases() throws IOException {
        openSumCaseWhen();
        List<Pair<String, String>> queries = reaALLSQLs(defaultProject, "query/sql_sum_expr", null);
        Assert.assertEquals("Please adjust expected value, if SQLs are added or removed ", 29, queries.size());
        queries.forEach(e ->checkSQL(defaultProject, e.getSecond(), e.getFirst(), null));
    }

    @Test
    public void testSimpleSQL() {
        openSumCaseWhen();
        String SQL =
                "SELECT " +
                   "SUM(CASE WHEN LSTG_FORMAT_NAME='FP-non GTC' THEN PRICE ELSE 2 END) " +
                "FROM TEST_KYLIN_FACT";
         checkSQL(defaultProject, SQL, null, null);
    }

    @Test
    public void testKE13524() throws IOException {
        // see https://olapio.atlassian.net/browse/KE-13524 for details
        closeSumCaseWhen();
        String project = "newten";
        List<Pair<String, String>> queries = reaALLSQLs(project, "sql_sinai_poc", "query15.sql");
        Assert.assertEquals(1, queries.size());
        Assert.assertNotNull(queries.get(0).getSecond());
        Assert.assertNotNull(toCalcitePlan(project, queries.get(0).getSecond()));

        String SQL = "select sum(2), sum(0), count(1) from POPHEALTH_ANALYTICS.Z_PROVDASH_UM_ED";
        Assert.assertNotNull(toCalcitePlan(project, SQL));

    }
}