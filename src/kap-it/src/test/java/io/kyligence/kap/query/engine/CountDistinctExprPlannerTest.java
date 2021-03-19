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

import io.kyligence.kap.query.rules.CalciteRuleTestBase;
import io.kyligence.kap.query.util.HepUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class CountDistinctExprPlannerTest extends CalciteRuleTestBase {

    static final String defaultProject = "default";
    static final DiffRepository diff = DiffRepository.lookup(CountDistinctExprPlannerTest.class);

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Override
    protected DiffRepository getDiffRepo() {
        return diff;
    }

    protected void checkSQL(String project, String sql, String prefix, StringOutput StrOut, Collection<RelOptRule>... ruleSets) {
        Collection<RelOptRule> rules = new HashSet<>();
        for (Collection<RelOptRule> ruleSet : ruleSets) {
            rules.addAll(ruleSet);
        }
        super.checkSQL(project, sql, prefix, StrOut, rules);
    }

    @Test
    @Ignore("For development")
    public void dumpPlans() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sql_count_distinct_expr");
        CalciteRuleTestBase.StringOutput output = new CalciteRuleTestBase.StringOutput(false);
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), output, HepUtils.SumExprRules, HepUtils.CountDistinctExprRules));
        output.dump(log);
    }

    @Test
    public void testAllCases() throws IOException {
        List<Pair<String, String>> queries = readALLSQLs(KylinConfig.getInstanceFromEnv(), defaultProject,
                "query/sql_count_distinct_expr");
        queries.forEach(e -> checkSQL(defaultProject, e.getSecond(), e.getFirst(), null, HepUtils.SumExprRules, HepUtils.CountDistinctExprRules));
    }

}