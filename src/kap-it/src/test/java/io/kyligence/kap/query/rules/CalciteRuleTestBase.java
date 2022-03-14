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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.test.DiffRepository;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Strings;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.engine.QueryOptimizer;
import io.kyligence.kap.query.util.HepUtils;

public class CalciteRuleTestBase extends NLocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CalciteRuleTestBase.class);

    private final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/";
    private final String emptyLinePattern = "(?m)^[ \t]*\r?\n";
    private final String NL = System.getProperty("line.separator");

    public class StringOutput {
        boolean separate;

        StringBuilder builderBefore = new StringBuilder();
        StringBuilder builderAfter = new StringBuilder();

        public StringOutput(boolean separate) {
            this.separate = separate;
        }

        public void dump(Logger log) {
            log.debug("planBefore: {}{}", NL, builderBefore.toString());
            if (separate)
                log.debug("planAfter: {}{}", NL, builderAfter.toString());
        }

        private void output(StringBuilder builder, String name, String plan) {
            builder.append("        <Resource name=\"").append(name).append("\">").append(NL);
            builder.append("            <![CDATA[");
            builder.append(plan);
            builder.append("]]>").append(NL);
            builder.append("        </Resource>").append(NL);
        }

        public void output(RelNode relBefore, RelNode relAfter, String prefix) {
            String before = Strings.isNullOrEmpty(prefix) ? "planBefore" : prefix + ".planBefore";
            final String planBefore = NL + RelOptUtil.toString(relBefore);
            output(builderBefore, separate ? prefix : before, planBefore);

            String after = Strings.isNullOrEmpty(prefix) ? "planAfter" : prefix + ".planAfter";
            final String planAfter = NL + RelOptUtil.toString(relAfter);
            output(separate ? builderAfter : builderBefore, separate ? prefix : after, planAfter);
        }
    }

    protected DiffRepository getDiffRepo() {
        throw new NotImplementedException("getDiffRepo");
    }

    protected Pair<String, String> readOneSQL(KylinConfig config, String project, String folder, String file)
            throws IOException {
        final String queryFolder = IT_SQL_KAP_DIR + folder;
        List<Pair<String, String>> queries = ExecAndComp.fetchQueries(queryFolder).stream().filter(e -> {
            if (Strings.isNullOrEmpty(file))
                return true;
            else
                return e.getFirst().contains(file);
        }).map(e -> {
            QueryParams queryParams = new QueryParams(config, e.getSecond(), project, 0, 0, "DEFAULT", false);
            String sql = QueryUtil.massageSql(queryParams).replaceAll(emptyLinePattern, ""); // remove empty line
            return new Pair<>(FilenameUtils.getBaseName(e.getFirst()), sql);
        }).collect(Collectors.toList());
        Assert.assertEquals(1, queries.size());
        return queries.get(0);
    }

    protected List<Pair<String, String>> readALLSQLs(KylinConfig config, String project, String folder)
            throws IOException {
        final String queryFolder = IT_SQL_KAP_DIR + folder;
        return ExecAndComp.fetchQueries(queryFolder).stream().map(e -> {
            QueryParams queryParams = new QueryParams(config, e.getSecond(), project, 0, 0, "DEFAULT", false);
            String sql = QueryUtil.massageSql(queryParams).replaceAll(emptyLinePattern, ""); // remove empty line
            return new Pair<>(FilenameUtils.getBaseName(e.getFirst()), sql);
        }).collect(Collectors.toList());
    }

    void checkDiff(RelNode relBefore, RelNode relAfter, String prefix) {
        String before = Strings.isNullOrEmpty(prefix) ? "planBefore" : prefix + ".planBefore";
        String beforeExpected = "${" + before + "}";
        final String planBefore = NL + RelOptUtil.toString(relBefore);
        getDiffRepo().assertEquals(before, beforeExpected, planBefore);

        String after = Strings.isNullOrEmpty(prefix) ? "planAfter" : prefix + ".planAfter";
        String afterExpected = "${" + after + "}";
        final String planAfter = NL + RelOptUtil.toString(relAfter);
        getDiffRepo().assertEquals(after, afterExpected, planAfter);
    }

    protected void checkSQLOptimize(String project, String sql, String prefix) {
        RelNode relBefore = sqlToRelRoot(project, sql, KylinConfig.getInstanceFromEnv()).rel;
        RelNode relAfter = toCalcitePlan(project, sql, KylinConfig.getInstanceFromEnv());
        logger.debug("check plan for {}.sql: {}{}", prefix, NL, sql);
        checkDiff(relBefore, relAfter, prefix);
    }

    protected void checkSQLPostOptimize(String project, String sql, String prefix, StringOutput StrOut, Collection<RelOptRule> rules) {
        RelNode relBefore = toCalcitePlan(project, sql, KylinConfig.getInstanceFromEnv());
        Assert.assertThat(relBefore, notNullValue());
        RelNode relAfter = HepUtils.runRuleCollection(relBefore, rules);
        Assert.assertThat(relAfter, notNullValue());
        logger.debug("check plan for {}.sql: {}{}", prefix, NL, sql);

        if (StrOut != null) {
            StrOut.output(relBefore, relAfter, prefix);
        } else {
            checkDiff(relBefore, relAfter, prefix);
        }
    }

    protected void checkPlanning(RelNode relBefore, RelNode relAfter, String prefix) {
        checkPlanning(relBefore, relAfter, prefix, false);
    }

    protected void checkPlanning(RelNode relBefore, RelNode relAfter, String prefix, boolean unchanged) {
        assertThat(relBefore, notNullValue());
        assertThat(relAfter, notNullValue());
        final String planBefore = NL + RelOptUtil.toString(relBefore);
        final String planAfter = NL + RelOptUtil.toString(relAfter);

        if (unchanged) {
            assertThat(planAfter, is(planBefore));
        } else {
            checkDiff(relBefore, relAfter, prefix);
        }
    }

    protected RelNode toCalcitePlan(String project, String SQL, KylinConfig kylinConfig) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        try {
            return qe.parseAndOptimize(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode toCalcitePlanWithoutOptimize(String project, String SQL, KylinConfig kylinConfig) {
        return sqlToRelRoot(project, SQL, kylinConfig).rel;
    }

    protected RelRoot sqlToRelRoot(String project, String SQL, KylinConfig kylinConfig) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        try {
            return qe.sqlToRelRoot(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode toCalcitePlan(String project, String SQL, KylinConfig kylinConfig,
            List<RelOptRule> rulesToRemoved, List<RelOptRule> rulesToAdded) {
        QueryExec qe = new QueryExec(project, kylinConfig);
        if (rulesToRemoved != null && !rulesToRemoved.isEmpty()) {
            qe.plannerRemoveRules(rulesToRemoved);
        }
        if (rulesToAdded != null && !rulesToAdded.isEmpty()) {
            qe.plannerAddRules(rulesToAdded);
        }
        try {
            return qe.parseAndOptimize(SQL);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("sql parse error", e);
        }
    }

    protected RelNode optimizeSQL(RelRoot relRoot, RelOptRule rule) {
        final HepProgram hepProgram = HepProgram.builder() //
                .addRuleInstance(rule) //
                .build();
        final HepPlanner planner = new HepPlanner(hepProgram);
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(DefaultRelMetadataProvider.INSTANCE);
        planner.registerMetadataProviders(list);
        planner.setRoot(relRoot.rel);
        return planner.findBestExp();
    }

    protected RelNode optimizeSQL(RelRoot relRoot, List<RelOptRule> rule) {
        final HepProgram hepProgram = HepProgram.builder() //
                .addRuleCollection(rule) //
                .build();
        return findBestExp(hepProgram, relRoot);
    }

    protected RelNode findBestExp(HepProgram hepProgram, RelRoot relRoot) {
        final HepPlanner planner = new HepPlanner(hepProgram);
        final QueryOptimizer optimizer = new QueryOptimizer(planner);
        RelRoot newRoot = optimizer.optimize(relRoot);
        List<RelMetadataProvider> list = Lists.newArrayList();
        list.add(DefaultRelMetadataProvider.INSTANCE);
        planner.registerMetadataProviders(list);
        planner.setRoot(newRoot.rel);
        return planner.findBestExp();
    }
}
