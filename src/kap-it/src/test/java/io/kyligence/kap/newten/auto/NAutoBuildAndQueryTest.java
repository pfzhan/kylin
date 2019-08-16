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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.SparderEnv;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.newten.NExecAndComp;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("serial")
/***
 *
 * used for test scenarios that mainly caused by all kinds of sql,
 * So it ensures that these sqls can be auto-propose correctly and
 * get right result from the pre-calculate layout.
 *
 */
public class NAutoBuildAndQueryTest extends NAutoTestBase {

    @Test
    public void testAllQueries() throws Exception {
        executeTestScenario(
                /* CompareLevel = SAME */
                new TestScenario(CompareLevel.SAME, "query/h2"), //
                new TestScenario(CompareLevel.SAME, "query/sql"), //
                new TestScenario(CompareLevel.SAME, "query/sql_boolean"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cache"), //
                new TestScenario(CompareLevel.SAME, "query/sql_casewhen"), //
                new TestScenario(CompareLevel.SAME, "query/sql_cross_join"), //
                new TestScenario(CompareLevel.SAME, "query/sql_current_date"), //
                new TestScenario(CompareLevel.SAME, "query/sql_datetime"), //
                new TestScenario(CompareLevel.SAME, "query/sql_day_of_week"), //
                new TestScenario(CompareLevel.SAME, "query/sql_derived"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct"), //
                new TestScenario(CompareLevel.SAME, "query/sql_distinct_dim"), //
                new TestScenario(CompareLevel.SAME, "query/sql_extended_column"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function"), //
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_nullHandling"),
                new TestScenario(CompareLevel.SAME, "query/sql_function/sql_function_formatUDF"),
                new TestScenario(CompareLevel.SAME, "query/sql_grouping"), //
                new TestScenario(CompareLevel.SAME, "query/sql_hive"), //
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
                new TestScenario(CompareLevel.SAME, "query/sql_non_equi_join"),
                new TestScenario(CompareLevel.SAME, "query/sql_orderby"), //
                new TestScenario(CompareLevel.SAME, "query/sql_probe"), //
                new TestScenario(CompareLevel.SAME, "query/sql_powerbi"),
                new TestScenario(CompareLevel.SAME, "query/sql_raw"), //
                new TestScenario(CompareLevel.SAME, "query/sql_rawtable"), //
                new TestScenario(CompareLevel.SAME, "query/sql_snowflake"), //
                new TestScenario(CompareLevel.SAME, "query/sql_subquery"), //
                new TestScenario(CompareLevel.SAME, "query/sql_tableau"), //
                new TestScenario(CompareLevel.SAME, "query/sql_timestamp"), //
                new TestScenario(CompareLevel.SAME, "query/sql_udf"), //
                new TestScenario(CompareLevel.SAME, "query/sql_union"), //
                new TestScenario(CompareLevel.SAME, "query/sql_value"), //
                new TestScenario(CompareLevel.SAME, "query/sql_verifyContent"), //
                new TestScenario(CompareLevel.SAME, "query/sql_window/new_sql_window"), //


                /* CompareLevel = SAME, JoinType = LEFT */
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_distinct_precisely"), //
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "query/sql_topn"), //

                /* CompareLevel = SAME_ROWCOUNT */
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_distinct/sql_distinct_hllc"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_function/sql_function_ifnull_timestamp"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_h2_uncapable"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_limit"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_percentile_only_with_spark_cube"),
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/sql_verifyCount"),

                /* CompareLevel = SAME_ORDER */
                new TestScenario(CompareLevel.SAME_ORDER, "query/sql_window"));
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        Set<String> exclusionList = Sets.newHashSet();
        overwriteSystemProp("calcite.debug", "true");
        new TestScenario(CompareLevel.SAME, "query/temp").execute();
    }

    @Ignore
    @Test
    public void testQueryForPreparedMetadata() throws Exception {
        TestScenario scenario = new TestScenario(CompareLevel.SAME_ROWCOUNT, "query/temp");
        collectQueries(scenario);
        List<Pair<String, String>> queries = scenario.getQueries();
        populateSSWithCSVData(kylinConfig, getProject(), SparderEnv.getSparkSession());
        NExecAndComp.execAndCompareNew(queries, getProject(), scenario.getCompareLevel(), scenario.joinType.toString(),
                null);
    }

    @Test
    public void testEscapeParentheses() throws Exception {
        overwriteSystemProp("kylin.query.transformers",
                "io.kyligence.kap.query.util.CognosParenthesesEscapeTransformer, io.kyligence.kap.query.util.ConvertToComputedColumn, org.apache.kylin.query.util.DefaultQueryTransformer, io.kyligence.kap.query.util.EscapeTransformer, org.apache.kylin.query.util.KeywordDefaultDirtyHack");
        overwriteSystemProp("kylin.query.pushdown.converter-class-names",
                "io.kyligence.kap.query.util.CognosParenthesesEscapeTransformer,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.util.SparkSQLFunctionConverter,org.apache.kylin.source.adhocquery.HivePushDownConverter");
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

    @Override
    protected void executeTestScenario(TestScenario... tests) throws Exception {
        if ("true".equals(System.getProperty("skipAutoModelingCI"))) { // -DskipAutoModelingCI=true
            return;
        }
        super.executeTestScenario(tests);
    }

    @Override
    protected Set<String> loadWhiteListSqlPatterns() throws IOException {
        log.info("override loadWhiteListSqlPatterns in NAutoBuildAndQueryTest");

        Set<String> result = Sets.newHashSet();
        final String folder = getFolder("query/unchecked_layout_list");
        File[] files = new File(folder).listFiles();
        if (files == null || files.length == 0) {
            return result;
        }

        String[] fileContentArr = new String(getFileBytes(files[0])).split(System.getProperty("line.separator"));
        final List<String> fileNames = Arrays.stream(fileContentArr)
                .filter(name -> !name.startsWith("-") && name.length() > 0) //
                .collect(Collectors.toList());
        final List<Pair<String, String>> queries = Lists.newArrayList();
        for (String name : fileNames) {
            File tmp = new File(NAutoTestBase.IT_SQL_KAP_DIR + "/" + name);
            final String sql = new String(getFileBytes(tmp));
            queries.add(new Pair<>(tmp.getCanonicalPath(), sql));
        }

        queries.forEach(pair -> {
            String sql = pair.getSecond(); // origin sql
            result.addAll(changeJoinType(sql));

            // add limit
            if (!sql.toLowerCase().contains("limit ")) {
                result.addAll(changeJoinType(sql + " limit 5"));
            }
        });

        return result;
    }
}
