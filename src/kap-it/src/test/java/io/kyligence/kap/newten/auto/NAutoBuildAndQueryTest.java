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

import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.newten.NExecAndComp.CompareLevel;

@SuppressWarnings("serial")
public class NAutoBuildAndQueryTest extends NAutoTestBase {

    @Test
    public void testAllQueries() throws Exception {
        executeTestScenario(
                /* CompareLevel = SAME */
                new TestScenario(CompareLevel.SAME, "sql"), //
                new TestScenario(CompareLevel.SAME, "sql_lookup"), //
                new TestScenario(CompareLevel.SAME, "sql_like"), //
                new TestScenario(CompareLevel.SAME, "sql_cache"), //
                new TestScenario(CompareLevel.SAME, "sql_derived"), //
                new TestScenario(CompareLevel.SAME, "sql_datetime"), //
                new TestScenario(CompareLevel.SAME, "sql_distinct"), //
                new TestScenario(CompareLevel.SAME, "sql_distinct_dim"), //
                new TestScenario(CompareLevel.SAME, "sql_multi_model"), //
                new TestScenario(CompareLevel.SAME, "sql_orderby"), //
                new TestScenario(CompareLevel.SAME, "sql_snowflake"), //
                new TestScenario(CompareLevel.SAME, "sql_join"), //
                new TestScenario(CompareLevel.SAME, "sql_union"), //
                new TestScenario(CompareLevel.SAME, "sql_grouping"), //
                new TestScenario(CompareLevel.SAME, "sql_hive"), //
                new TestScenario(CompareLevel.SAME, "sql_raw"), //
                new TestScenario(CompareLevel.SAME, "sql_rawtable"), //
                new TestScenario(CompareLevel.SAME, "sql_subquery"), //
                new TestScenario(CompareLevel.SAME, "sql_magine"), //
                new TestScenario(CompareLevel.SAME, "sql_magine_inner"), //
                new TestScenario(CompareLevel.SAME, "sql_magine_window"), //

                /* CompareLevel = SAME, JoinType = LEFT */
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "sql_distinct_precisely"), //
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "sql_topn"), //

                /* CompareLevel = SAME_ROWCOUNT */
                new TestScenario(CompareLevel.SAME_ROWCOUNT, "sql_tableau"), //

                /* CompareLevel = NONE */
                new TestScenario(CompareLevel.NONE, "sql_timestamp"), //
                new TestScenario(CompareLevel.NONE, "sql_window"), //
                new TestScenario(CompareLevel.NONE, "sql_h2_uncapable"));

        // FIXME  https://github.com/Kyligence/KAP/issues/8090   percentile and sql_intersect_count do not support
        // new TestScenario("sql_intersect_count", CompareLevel.NONE, "left")
        // new TestScenario("sql_percentile", CompareLevel.NONE)//,
        // new TestScenario("sql_powerbi", CompareLevel.SAME),
    }

    @Test
    public void testCountStar() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_count_star").execute();
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        Set<String> exclusionList = Sets.newHashSet();
        new TestScenario(CompareLevel.SAME, "temp", exclusionList).execute();
    }

    /***************
     * Test Kylin test queries with auto modeling
     */
    @Test
    public void testPowerBI() throws Exception {
        overwriteSystemProp("kylin.query.pushdown.runner-class-name",
                "org.apache.kylin.query.adhoc.PushDownRunnerSparkImpl");
        new TestScenario(CompareLevel.SAME, "sql_powerbi").execute();
    }

    @Ignore("not storage query, skip")
    @Test
    public void testTableauProbing() throws Exception {
        new TestScenario(CompareLevel.NONE, "tableau_probing").execute();
    }

    @Test
    public void testLimitCorrectness() throws Exception {
        new TestScenario(CompareLevel.SAME, true, "sql").execute();
    }

    /**
     * Following cased are not in Newten M1 scope
     */
    @Ignore("not in Newten M1 scope")
    @Test
    public void testNotSupported() throws Exception {

        /* CompareLevel = SAME */
        new TestScenario(CompareLevel.SAME, "sql_verifyContent").execute();
        new TestScenario(CompareLevel.SAME, "sql_current_date").execute();
        new TestScenario(CompareLevel.SAME, "sql_percentile").execute();
        new TestScenario(CompareLevel.SAME, "sql_extended_column").execute();

        /* CompareLevel = NONE */
        new TestScenario(CompareLevel.NONE, "sql_timeout").execute();
        new TestScenario(CompareLevel.NONE, "sql_streaming").execute();
        new TestScenario(CompareLevel.NONE, "sql_massin_distinct").execute();
        new TestScenario(CompareLevel.NONE, "sql_massin").execute();
        new TestScenario(CompareLevel.NONE, "sql_limit").execute();
        new TestScenario(CompareLevel.NONE, "sql_invalid").execute();
        new TestScenario(CompareLevel.NONE, "sql_dynamic").execute();
        new TestScenario(CompareLevel.NONE, "sql_timeout").execute();

        /* CompareLevel = SAME_ROWCOUNT */
        new TestScenario(CompareLevel.SAME_ROWCOUNT, "sql_verifyCount").execute();
    }

    @Override
    protected void executeTestScenario(TestScenario... tests) throws Exception {
        if ("true".equals(System.getProperty("skipAutoModelingCI"))) { // -DskipAutoModelingCI=true
            return;
        }
        super.executeTestScenario(tests);
    }
}
