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

package io.kyligence.kap.newten.auto;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import io.kyligence.kap.guava20.shaded.common.base.Throwables;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.util.ExecAndComp;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * used for test scenarios that mainly caused by all kinds of sql,
 * So it ensures that these sqls can be auto-propose correctly and
 * get right result from the pre-calculate layout.
 *
 */
@Slf4j
public class NAutoBuildAndQueryTest extends AutoTestBase {

    @Test
    public void testSumExpr() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");

        new TestScenario(CompareLevel.SAME, "query/sql_sum_expr").execute();
    }

    @Test
    public void testCountDistinctExpr() throws Exception {
        overwriteSystemProp("kylin.query.convert-sum-expression-enabled", "TRUE");
        overwriteSystemProp("kylin.query.convert-count-distinct-expression-enabled", "TRUE");

        new TestScenario(CompareLevel.SAME, "query/sql_count_distinct_expr").execute();
    }

    @Test
    public void testDimensionAsMeasure() throws Exception {
        updateProjectConfig("kylin.query.implicit-computed-column-convert", "FALSE");
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute();

        updateProjectConfig("kylin.query.implicit-computed-column-convert", "FALSE");
        updateProjectConfig("kylin.query.convert-sum-expression-enabled", "TRUE");
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute();

        updateProjectConfig("kylin.query.implicit-computed-column-convert", "FALSE");
        updateProjectConfig("kylin.query.convert-sum-expression-enabled", "FALSE");
        updateProjectConfig("kylin.query.convert-count-distinct-expression-enabled", "TRUE");
        new TestScenario(CompareLevel.SAME, "query/sql_dimension_as_measure").execute();
    }

    @Test
    public void testAllQueries() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        overwriteSystemProp("kylin.query.join-match-optimization-enabled", "TRUE");

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(getProject(), copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            overrideKylinProps.put("kylin.query.metadata.expose-computed-column", "FALSE");
        });

        executeTestScenario(24,
                /* CompareLevel = SAME */
                new TestScenario(CompareLevel.SAME, "query/h2"), //
                new TestScenario(CompareLevel.SAME, "query/sql_replace_special_symbol"), //
                new TestScenario(CompareLevel.SAME, "query/sql"), //
                new TestScenario(CompareLevel.SAME, "query/sql_boolean"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cache"), //
                new TestScenario(CompareLevel.SAME, "query/sql_casewhen"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_ccv2"), //
                new TestScenario(CompareLevel.SAME, "query/sql_constant"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cross_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_current_date"), //
                new TestScenario(CompareLevel.SAME, "query/sql_datetime"), //
                new TestScenario(CompareLevel.SAME, "query/sql_day_of_week"), //
                new TestScenario(CompareLevel.SAME, "query/sql_derived"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct_dim"), //
                new TestScenario(CompareLevel.SAME, "query/sql_except"),
                new TestScenario(CompareLevel.SAME, "query/sql_extended_column"), //
                new TestScenario(CompareLevel.SAME, "query/sql_filter_simplify"), //
                new TestScenario(CompareLevel.SAME, "query/sql_grouping"), //
                new TestScenario(CompareLevel.SAME, "query/sql_hive"), //
                new TestScenario(CompareLevel.SAME, "query/sql_in"), //
                new TestScenario(CompareLevel.SAME, "query/sql_inner_column"), //
                new TestScenario(CompareLevel.SAME, "query/sql_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_join/sql_right_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_kap"), //
                new TestScenario(CompareLevel.SAME, "query/sql_like"), //
                new TestScenario(CompareLevel.SAME, "query/sql_lookup"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine_inner"), //
                new TestScenario(CompareLevel.SAME, "query/sql_magine_left"), //
                new TestScenario(CompareLevel.SAME, "query/sql_multi_model"), //
                new TestScenario(CompareLevel.SAME, "query/sql_orderby"), //
                new TestScenario(CompareLevel.SAME, "query/sql_probe"), //
                new TestScenario(CompareLevel.SAME, "query/sql_raw"), //
                new TestScenario(CompareLevel.SAME, "query/sql_rawtable"), //
                new TestScenario(CompareLevel.SAME, "query/sql_similar"), //
                new TestScenario(CompareLevel.SAME, "query/sql_snowflake"), //
                new TestScenario(CompareLevel.SAME, "query/sql_subquery"), //
                new TestScenario(CompareLevel.SAME, "query/sql_tableau"), //
                new TestScenario(CompareLevel.SAME, "query/sql_timestamp"), //
                new TestScenario(CompareLevel.SAME, "query/sql_udf"), //
                new TestScenario(CompareLevel.SAME, "query/sql_union"), //
                new TestScenario(CompareLevel.SAME, "query/sql_value"), //
                new TestScenario(CompareLevel.SAME, "query/sql_verifyContent"), //
                new TestScenario(CompareLevel.SAME, "query/sql_window/new_sql_window"), //
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/time"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/string"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/misc"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/math"),
                new TestScenario(CompareLevel.SAME, "query/sql_spark_func/constant_query"),
                new TestScenario(CompareLevel.SAME, "query/sql_truncate"), //
                new TestScenario(CompareLevel.SAME, "query/sql-replace"), //

                /* CompareLevel = SAME, JoinType = LEFT */
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_distinct_precisely"), //
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_topn"), //

                /* CompareLevel = SAME_ROWCOUNT */
                new TestScenario(CompareLevel.SAME_ROWCOUNT,
                        "query/sql_computedcolumn/sql_computedcolumn_ifnull_timestamp"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_distinct/sql_distinct_hllc"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_function/sql_function_ifnull_timestamp"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_function/sql_function_constant_func"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_h2_uncapable"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_limit"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile_only_with_spark_cube"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_verifyCount"),

                /* CompareLevel = SAME_ORDER */
                new TestScenario(CompareLevel.SAME_ORDER, "query/sql_window"),

                /* CompareLevel = NONE */
                new TestScenario(CompareLevel.NONE, "query/sql_intersect_count"),
                new TestScenario(CompareLevel.NONE, "query/sql_limit_offset"),
                new TestScenario(CompareLevel.NONE, "query/sql_function/sql_function_round"));
    }

    @Test
    public void testBuildAndQueryWithExcludeTable() throws Exception {
        excludedSqlPatterns.addAll(loadWhiteListPatterns());
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        executeTestScenario(BuildAndCompareContext.builder().expectModelNum(2)
                .testScenarios(Lists.newArrayList(new TestScenario(CompareLevel.SAME, "query/h2"))).extension(df -> {
                    FavoriteRuleManager ruleManager = FavoriteRuleManager.getInstance(kylinConfig, getProject());
                    List<FavoriteRule.AbstractCondition> conds = Lists.newArrayList();
                    conds.add(new FavoriteRule.Condition(null, df.getModel().getRootFactTableName()));
                    ruleManager.updateRule(conds, true, FavoriteRule.EXCLUDED_TABLES_RULE);
                }).build());
    }

    @Test
    public void testSpecialJoin() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(getProject(), copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            overrideKylinProps.put("kylin.query.metadata.expose-computed-column", "TRUE");
        });

        executeTestScenario(new TestScenario(CompareLevel.SAME, "query/sql_powerbi"),
                new TestScenario(CompareLevel.SAME, "query/sql_special_join"));

        projectManager.updateProject(getProject(), copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            overrideKylinProps.put("kylin.query.metadata.expose-computed-column", "FALSE");
        });

        executeTestScenario(new TestScenario(CompareLevel.SAME, "query/sql_special_join_condition"));
    }

    @Test
    public void testNonEqualJoin() throws Exception {
        if ("true".equals(System.getProperty("runDailyUT"))) {
            overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
            overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
            overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
            executeTestScenario(new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join"));
        }
    }

    @Test
    public void testNonEqualInnerJoin() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "FALSE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");

        Assert.assertFalse(KylinConfig.getInstanceFromEnv().isQueryNonEquiJoinModelEnabled());
        executeTestScenario(2, new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join", 32, 33));

        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        Assert.assertTrue(KylinConfig.getInstanceFromEnv().isQueryNonEquiJoinModelEnabled());
        executeTestScenario(1, new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join", 32, 33));
    }

    @Ignore("blocked by KE-30323")
    @Test
    public void testUDFs() throws Exception {
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.filter-key.enabled", "TRUE");
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");
        overwriteSystemProp("kylin.smart.conf.computed-column.suggestion.enabled-if-no-sampling", "TRUE");
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.updateProject(getProject(), copyForWrite -> {
            LinkedHashMap<String, String> overrideKylinProps = copyForWrite.getOverrideKylinProps();
            overrideKylinProps.put("kylin.query.metadata.expose-computed-column", "FALSE");
        });

        executeTestScenario(new TestScenario(CompareLevel.SAME, "query/sql_function"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_nullHandling"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_formatUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_DateUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_OtherUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_StringUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_nullHandling"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_formatUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_DateUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_OtherUDF"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_expression"), //
                new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_select_group_same_column") //
        );
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");
        Set<String> exclusionList = Sets.newHashSet();
        overwriteSystemProp("calcite.debug", "true");
        new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/temp").execute();
    }

    @Test
    public void testCorr() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");

        new TestScenario(CompareLevel.SAME, "query/sql_corr").execute();
    }

    @Test
    public void testGroupingSetsWithoutSplitGroupingSets() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        overwriteSystemProp("kylin.query.engine.split-group-sets-into-union", "FALSE");
        new TestScenario(CompareLevel.SAME, "query/sql_grouping").execute();
    }

    @Ignore
    @Test
    public void testQueryForPreparedMetadata() throws Exception {
        TestScenario scenario = new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/temp");
        collectQueries(Lists.newArrayList(scenario));
        List<Pair<String, String>> queries = scenario.getQueries();
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        ExecAndComp.execAndCompare(queries, getProject(), scenario.getCompareLevel(),
                scenario.getJoinType().toString());
    }

    @Test
    public void testCCWithSelectStar() throws Exception {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        boolean exposeComputedColumnConfBefore = projectManager.getProject(getProject()).getConfig()
                .exposeComputedColumn();
        projectManager.updateProject(getProject(), copyForWrite -> copyForWrite.getOverrideKylinProps()
                .put("kylin.query.metadata.expose-computed-column", "TRUE"));
        try {
            new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn/sql_computedcolumn_with_select_star", 0, 1)
                    .execute();
            new TestScenario(CompareLevel.NONE, "query/sql_computedcolumn/sql_computedcolumn_with_select_star", 1, 4)
                    .execute();
        } finally {
            projectManager.updateProject(getProject(),
                    copyForWrite -> copyForWrite.getOverrideKylinProps().put(
                            "kylin.query.metadata.expose-computed-column",
                            String.valueOf(exposeComputedColumnConfBefore)));

        }
    }

    @Test
    public void testEscapeParentheses() throws Exception {
        overwriteSystemProp("kylin.query.transformers",
                "io.kyligence.kap.query.util.CognosParenthesesEscapeTransformer, io.kyligence.kap.query.util.ConvertToComputedColumn, org.apache.kylin.query.util.DefaultQueryTransformer, io.kyligence.kap.query.util.EscapeTransformer, org.apache.kylin.query.util.KeywordDefaultDirtyHack");
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.CognosParenthesesEscapeTransformer,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter");
        overwriteSystemProp("kylin.query.table-detect-transformers",
                "io.kyligence.kap.query.util.CognosParenthesesEscapeTransformer,  org.apache.kylin.query.util.DefaultQueryTransformer, io.kyligence.kap.query.util.EscapeTransformer");
        new TestScenario(CompareLevel.SAME, "query/sql_parentheses_escape").execute();
    }

    @Test
    public void testOrdinalQuery() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "query/sql_ordinal").execute();
    }

    @Ignore("not storage query, skip")
    @Test
    public void testTableauProbing() throws Exception {
        new TestScenario(CompareLevel.NONE, "query/tableau_probing").execute();
    }

    @Test
    public void testDynamicQuery() throws Exception {
        TestScenario testScenario = new TestScenario(CompareLevel.SAME, "query/sql_dynamic");
        testScenario.setDynamicSql(true);
        testScenario.execute();
    }

    @Test
    public void testCalciteOperatorTablesConfig() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.FUN", "standard,oracle");
        executeTestScenario(new TestScenario(CompareLevel.SAME, "query/sql_function/oracle_function"), // NVL
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_DateUDF") // make sure udfs are executed correctly
        );
    }

    /**
     * Following cased are not supported in auto-model test
     */
    @Ignore("not supported")
    @Test
    public void testNotSupported() throws Exception {

        // FIXME  https://github.com/Kyligence/KAP/issues/8090  
        // percentile and sql_intersect_count do not support
        // new TestScenario(CompareLevel.SAME, "sql_intersect_count")
        // new TestScenario(CompareLevel.SAME, "sql_percentile")//,

        /* CompareLevel = SAME */

        // Covered by manual test with fixed
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn").execute();
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn_common").execute();
        new TestScenario(CompareLevel.SAME, "query/sql_computedcolumn_leftjoin").execute();

        /* CompareLevel = NONE */

        // test bad query detector
        // see ITKapKylinQueryTest.runTimeoutQueries
        new TestScenario(CompareLevel.NONE, "query/sql_timeout").execute();

        // stream not testable
        new TestScenario(CompareLevel.NONE, "query/sql_streaming").execute();

        // see ITMassInQueryTest
        new TestScenario(CompareLevel.NONE, "query/sql_massin_distinct").execute();
        new TestScenario(CompareLevel.NONE, "query/sql_massin").execute();
        new TestScenario(CompareLevel.NONE, "query/sql_intersect_count").execute();

        // see ITKylinQueryTest.testInvalidQuery
        new TestScenario(CompareLevel.NONE, "query/sql_invalid").execute();

        /* CompareLevel = SAME_ROWCOUNT */
    }

    @Test
    public void testConformance() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "query/sql_conformance").execute();
    }

    @Test
    //reference KE-11887
    public void testQuerySingleValue() throws Exception {
        final String TEST_FOLDER = "query/sql_single_value";
        try {
            //query00 subquery return multi null row
            //query01 should throw RuntimeException
            new TestScenario(CompareLevel.NONE, TEST_FOLDER, 0, 2).execute();
            assert false;
        } catch (Throwable e) {
            String rootCauseMsg = Throwables.getRootCause(e).getMessage();
            Assert.assertEquals(rootCauseMsg, "more than 1 row returned in a single value aggregation");
        }

        //query02 support SINGLE_VALUE
        //query03 support :Issue 4337 , select (select '2012-01-02') as data, xxx from table group by xxx
        //query04 support :Subquery is null
        new TestScenario(CompareLevel.SAME, TEST_FOLDER, 2, 5).execute();

    }
}
