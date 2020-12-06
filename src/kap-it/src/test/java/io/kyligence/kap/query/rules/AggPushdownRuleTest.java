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

package io.kyligence.kap.query.rules;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.test.DiffRepository;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.query.optrule.KapAggFilterTransposeRule;
import io.kyligence.kap.query.optrule.KapAggJoinTransposeRule;
import io.kyligence.kap.query.optrule.KapAggProjectMergeRule;
import io.kyligence.kap.query.optrule.KapAggProjectTransposeRule;
import io.kyligence.kap.query.optrule.KapAggregateRule;
import io.kyligence.kap.query.optrule.KapFilterRule;
import io.kyligence.kap.query.optrule.KapJoinRule;
import io.kyligence.kap.query.optrule.KapProjectRule;

public class AggPushdownRuleTest extends CalciteRuleTestBase {

    private final DiffRepository diff = DiffRepository.lookup(AggPushdownRuleTest.class);
    private final String project = "subquery";
    private final List<RelOptRule> rulesDefault = Lists.newArrayList();

    @Before
    public void setUp() {
        overwriteSystemProp("calcite.keep-in-clause", "false");
        createTestMetadata("src/test/resources/ut_meta/agg_push_down");
        rulesDefault.add(KapFilterRule.INSTANCE);
        rulesDefault.add(KapProjectRule.INSTANCE);
        rulesDefault.add(KapAggregateRule.INSTANCE);
        rulesDefault.add(KapJoinRule.INSTANCE);
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
        System.clearProperty("calcite.keep-in-clause");
    }

    @Test
    //Test property kylin.query.calcite.aggregate-pushdown-enabled
    //Test with VolcanoPlanner for all rules
    public void testAggPushdown() throws IOException{
        KylinConfig configBefore = KylinConfig.getInstanceFromEnv();
        List<Pair<String, String>> queries = readALLSQLs(configBefore, project, "query/sql_select_subquery");

        KylinConfig configAfter = KylinConfig.createKylinConfig(configBefore);
        configAfter.setProperty("kylin.query.calcite.aggregate-pushdown-enabled", "true");

        for (Pair<String, String> pair : queries) {
            RelNode relBefore = toCalcitePlan(project, pair.getSecond(), configBefore);
            RelNode relAfter = toCalcitePlan(project, pair.getSecond(), configAfter);
            checkPlanning(relBefore, relAfter, pair.getFirst(),  diff);
        }
    }

    @Test
    //Test with VolcanoPlanner for all rules
    public void testAddRules() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Pair<String, String>> queries = readALLSQLs(config, project, "query/sql_select_subquery");
        List<RelOptRule> rulesToAdd = Lists.newArrayList();
        rulesToAdd.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);
        rulesToAdd.add(KapAggProjectMergeRule.AGG_PROJECT_JOIN);
        rulesToAdd.add(KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN);
        rulesToAdd.add(KapAggProjectTransposeRule.AGG_PROJECT_JOIN);
        rulesToAdd.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);
        rulesToAdd.add(KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG);
        for (Pair<String, String> pair : queries) {
            RelNode relBefore = toCalcitePlan(project, pair.getSecond(), config);
            RelNode relAfter = toCalcitePlan(project, pair.getSecond(), config, null, rulesToAdd);
            checkPlanning(relBefore, relAfter, pair.getFirst(),  diff);
        }
    }

    @Test
    //Test with HepPlanner for single rule
    public void testAggProjectMergeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rules = Lists.newArrayList(rulesDefault);
        rules.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesDefault);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst(), diff);
    }

    @Test
    public void testAggProjectTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query01.sql");

        List<RelOptRule> rules = Lists.newArrayList(rulesDefault);
        rules.add(KapAggProjectTransposeRule.AGG_PROJECT_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesDefault);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst(), diff);
    }

    @Test
    public void testAggFilterTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rulesBefore = Lists.newArrayList(rulesDefault);
        rulesBefore.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);

        List<RelOptRule> rules = Lists.newArrayList(rulesBefore);
        rules.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesBefore);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst(), diff);
    }

    @Test
    public void testAggJoinTransposeRule() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Pair<String, String> query = readOneSQL(config, project, "query/sql_select_subquery", "query05.sql");

        List<RelOptRule> rulesBefore = Lists.newArrayList(rulesDefault);
        rulesBefore.add(KapAggProjectMergeRule.AGG_PROJECT_FILTER_JOIN);
        rulesBefore.add(KapAggFilterTransposeRule.AGG_FILTER_JOIN);

        List<RelOptRule> rules = Lists.newArrayList(rulesBefore);
        rules.add(KapAggJoinTransposeRule.INSTANCE_JOIN_RIGHT_AGG);

        RelRoot relRoot = sqlToRelRoot(project, query.getSecond(), config);
        RelNode relBefore = optimizeSQL(relRoot, rulesBefore);
        RelNode relAfter = optimizeSQL(relRoot, rules);

        checkPlanning(relBefore, relAfter, query.getFirst(), diff);
    }
}
