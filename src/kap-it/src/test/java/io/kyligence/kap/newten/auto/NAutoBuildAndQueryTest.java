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
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.newten.NExecAndComp.CompareLevel;
import io.kyligence.kap.rest.service.FavoriteQueryService;

@SuppressWarnings("serial")
public class NAutoBuildAndQueryTest extends NAutoTestBase {

    @Test
    public void testAllQueries() throws Exception {
        executeTestScenario(
                /* CompareLevel = SAME */
                new TestScenario(CompareLevel.SAME, "sql"), //
                new TestScenario(CompareLevel.SAME, "sql_kap"), //
                new TestScenario(CompareLevel.SAME, "sql_boolean"), //
                new TestScenario(CompareLevel.SAME, "sql_cache"), //
                new TestScenario(CompareLevel.SAME, "sql_casewhen"), //
                new TestScenario(CompareLevel.SAME, "sql_cross_join"), //
                new TestScenario(CompareLevel.SAME, "sql_current_date"), //
                new TestScenario(CompareLevel.SAME, "sql_datetime"), //
                new TestScenario(CompareLevel.SAME, "sql_derived"), //
                new TestScenario(CompareLevel.SAME, "sql_distinct"), //
                new TestScenario(CompareLevel.SAME, "sql_distinct_dim"), //
                new TestScenario(CompareLevel.SAME, "sql_extended_column"), //
                new TestScenario(CompareLevel.SAME, "sql_grouping"), //
                new TestScenario(CompareLevel.SAME, "sql_h2_uncapable"), //
                new TestScenario(CompareLevel.SAME, "sql_hive"), //
                new TestScenario(CompareLevel.SAME, "sql_join"), //
                new TestScenario(CompareLevel.SAME, "sql_like"), //
                new TestScenario(CompareLevel.SAME, "sql_lookup"), //
                new TestScenario(CompareLevel.SAME, "sql_magine"), //
                new TestScenario(CompareLevel.SAME, "sql_magine_inner"), //
                new TestScenario(CompareLevel.SAME, "sql_magine_left"), //
                new TestScenario(CompareLevel.SAME, "sql_magine_window"), //
                new TestScenario(CompareLevel.SAME, "sql_multi_model"), //
                new TestScenario(CompareLevel.SAME_ORDER, "sql_orderby"), //
                new TestScenario(CompareLevel.SAME, "sql_probe"), //
                new TestScenario(CompareLevel.SAME, "sql_raw"), //
                new TestScenario(CompareLevel.SAME, "sql_rawtable"), //
                new TestScenario(CompareLevel.SAME, "sql_should_work"), //
                new TestScenario(CompareLevel.SAME, "sql_snowflake"), //
                new TestScenario(CompareLevel.SAME, "sql_sparder_function"), //
                new TestScenario(CompareLevel.SAME, "sql_subquery"), //
                new TestScenario(CompareLevel.SAME, "sql_tableau"), //
                new TestScenario(CompareLevel.SAME, "sql_timestamp"), //
                new TestScenario(CompareLevel.SAME, "sql_udf"), //
                new TestScenario(CompareLevel.SAME, "sql_value"), //
                new TestScenario(CompareLevel.SAME, "sql_verifyContent"), //
                new TestScenario(CompareLevel.SAME, "sql_union"), //
                new TestScenario(CompareLevel.SAME, "sql_noagg"), //
                new TestScenario(CompareLevel.SAME, "sql_special_functions"), //
                new TestScenario(CompareLevel.SAME, "sql_day_of_week"), //

                /* CompareLevel = SAME, JoinType = LEFT */
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "sql_distinct_precisely"), //
                new TestScenario(CompareLevel.SAME, JoinType.LEFT, "sql_topn"), //

                /* CompareLevel = SAME_ROWCOUNT */

                /* CompareLevel = NONE */
                new TestScenario(CompareLevel.SAME_ORDER,  "sql_window",
                        Sets.newHashSet("query08.sql", "query09.sql", "query12.sql")),
                new TestScenario(CompareLevel.NONE, "sql_window")//
        );
    }

    @Test
    @Ignore("For development")
    public void testTemp() throws Exception {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        Set<String> exclusionList = Sets.newHashSet();
        overwriteSystemProp("calcite.debug", "true");
        new TestScenario(CompareLevel.NONE, "temp", exclusionList).execute();
    }

    /**
     * Test a query only only with count(*), can build and query from IndexPlan,
     * don't move it.
     */
    @Test
    public void testCountStar() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_count_star").execute();
    }

    /***************
     * Test Kylin test queries with auto modeling
     */
    @Test
    public void testPowerBI() throws Exception {
        overwriteSystemProp("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        new TestScenario(CompareLevel.SAME, "sql_powerbi").execute();
    }

    @Test
    public void testProposeSQLWontChangeOriginMetadata() throws Exception {
        try {
            // 1. create metadata
            proposeWithSmartMaster(
                    new TestScenario[] { new TestScenario(CompareLevel.SAME, "auto/sql_repropose", 0, 1) },
                    getProject());
            buildAllCubes(KylinConfig.getInstanceFromEnv(), getProject());

            //2. get accelerate tip
            List<String> waitingAccelerateSqls = collectQueries(
                    new TestScenario(CompareLevel.SAME, "auto/sql_repropose", 0, 1));
            new FavoriteQueryService().getOptimizedModelNumForTest(getProject(),
                    waitingAccelerateSqls.toArray(new String[] {}));

            //3. ensure metadata
            List<MeasureDesc> measureDescs = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listEffectiveRewriteMeasures(getProject(), "EDW.TEST_SITES");
            measureDescs.stream().filter(measureDesc -> measureDesc.getFunction().isSum()).forEach(measureDesc -> {
                FunctionDesc func = measureDesc.getFunction();
                Assert.assertTrue(!func.getColRefs().isEmpty());
            });
        } finally {
            FileUtils.deleteDirectory(new File("../kap-it/metastore_db"));
        }
    }

    @Test
    public void testLimit() throws Exception {
        new TestScenario(CompareLevel.SAME_ROWCOUNT, "sql_limit").execute();
    }

    @Test
    public void testOrdinalQuery() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "sql_ordinal").execute();
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

        // different test method, already covered by SAME
        new TestScenario(CompareLevel.SAME, "sql_verifyContent").execute();

        // Covered by manual test with fixed 
        new TestScenario(CompareLevel.SAME, "sql_computedcolumn").execute();
        new TestScenario(CompareLevel.SAME, "sql_computedcolumn_common").execute();
        new TestScenario(CompareLevel.SAME, "sql_computedcolumn_leftjoin").execute();

        // Use NONE, SparkSQL does not support udf TimestampAdd
        new TestScenario(CompareLevel.SAME, "sql_current_date").execute();

        /* CompareLevel = NONE */

        // test bad query detector
        // see ITKapKylinQueryTest.runTimeoutQueries
        new TestScenario(CompareLevel.NONE, "sql_timeout").execute();

        // stream not testable
        new TestScenario(CompareLevel.NONE, "sql_streaming").execute();

        // see ITMassInQueryTest
        new TestScenario(CompareLevel.NONE, "sql_massin_distinct").execute();
        new TestScenario(CompareLevel.NONE, "sql_massin").execute();

        // see ITKylinQueryTest.testInvalidQuery
        new TestScenario(CompareLevel.NONE, "sql_invalid").execute();

        // see KylinTestBase.execAndCompDynamicQuery
        new TestScenario(CompareLevel.NONE, "sql_dynamic").execute();
        new TestScenario(CompareLevel.NONE, "sql_dynamic_sparder").execute();

        /* CompareLevel = SAME_ROWCOUNT */

        // different test method
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
