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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.query.engine.exec.sparder.SparderQueryPlanExec;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class QueryExecTest extends NLocalFileMetadataTestCase {

    final String project = "default";

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        staticCleanupTestMetadata();
    }

    private Dataset<Row> check(String SQL) throws SqlParseException {
        RelNode node = toCalcitePlan(SQL);
        log.debug(RelOptUtil.toString(node));
        return toSparkPlan(node);
    }

    private Dataset<Row> toSparkPlan(RelNode node) {
        SparderEnv.skipCompute();
        SparderQueryPlanExec planExec = new SparderQueryPlanExec();
        planExec.execute(node, null);
        Dataset<Row> dataset = SparderEnv.getDF();
        Assert.assertNotNull(dataset);
        return dataset;
    }
    private RelNode toCalcitePlan(String SQL) throws SqlParseException {
        ProjectSchemaFactory ps = new ProjectSchemaFactory(project, KylinConfig.getInstanceFromEnv());
        QueryExec qe = new QueryExec(KylinConfig.getInstanceFromEnv(), ps);
        return qe.parseAndOptimize(SQL);
    }

    @Test
    public void test_15261() throws SqlParseException {

        // Can not reproduce https://github.com/Kyligence/KAP/issues/15261 at 4.x
        // we needn't introduce KapAggregateReduceFunctionsRule as we did in 3.x
        overwriteSystemProp("kap.query.enable-convert-sum-expression", "TRUE");
        String SQL =
                "select sum(t.a1 * 2)  from (" +
                        "select sum(price/2) as a1, sum(ITEM_COUNT) as a2 from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME" +
                        ") t";
        Assert.assertNotNull(check(SQL));
    }

    /**
     * See {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}, it will rewrite <code>Sum(x)</code> to
     * <code>case COUNT(x) when 0 then null else SUM0(x) end</code>.
     *
     * <p>This rule doesn't consider situation where x is null, and still convert it to
     * <code>case COUNT(null) when 0 then null else SUM0(null) end</code>, which is incompatible with model section
     *
     * <p>See also {@link io.kyligence.kap.query.engine.QueryExecTest#test_15261()}
     * @throws SqlParseException
     */
    @Test
    public void test_sum_case_when_has_null() throws SqlParseException {

        overwriteSystemProp("kap.query.enable-convert-sum-expression", "TRUE");
        String SQLWithZero =
                "select CAL_DT,\n" +
                        "       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else 0 end)\n" +
                        "from TEST_KYLIN_FACT\n" +
                        "group by CAL_DT";
        check(SQLWithZero);
        String SQLWithNull =
                "select CAL_DT,\n" +
                        "       sum(case when LSTG_FORMAT_NAME in ('ABIN', 'XYZ') then 2 else null end)\n" +
                        "from TEST_KYLIN_FACT\n" +
                        "group by CAL_DT";
        check(SQLWithNull);
    }
}
