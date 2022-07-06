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

package io.kyligence.kap.newten;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.util.ExecAndComp;
import scala.Option;
import scala.runtime.AbstractFunction1;

public class NExactlyMatchTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/agg_exact_match");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "agg_match";
    }

    @Test
    public void testInClause() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String base = "select count(*) from TEST_KYLIN_FACT ";

        String in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) in ('A','F')"
                + " and substring(LSTG_FORMAT_NAME, 1, 1) in ('O','B')";

        String not_in1 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        String not_in2 = base + "where substring(LSTG_FORMAT_NAME, 1, 1) not in ('A','F')"
                + " or substring(LSTG_FORMAT_NAME, 1, 1) not in ('O','B')";

        overwriteSystemProp("calcite.keep-in-clause", "true");
        Dataset<Row> df1 = ExecAndComp.queryModelWithoutCompute(getProject(), in1);
        Dataset<Row> df2 = ExecAndComp.queryModelWithoutCompute(getProject(), in2);
        Dataset<Row> df3 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);
        Dataset<Row> df4 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);

        Assert.assertTrue(existsIn(df1));
        Assert.assertTrue(existsIn(df2));
        Assert.assertTrue(existsIn(df3));
        Assert.assertTrue(existsIn(df4));
        ArrayList<String> querys = Lists.newArrayList(in1, in2, not_in1, not_in2);
        ExecAndComp.execAndCompareQueryList(querys, getProject(), ExecAndComp.CompareLevel.SAME, "left");

        overwriteSystemProp("calcite.keep-in-clause", "false");
        Dataset<Row> df5 = ExecAndComp.queryModelWithoutCompute(getProject(), in1);
        Dataset<Row> df6 = ExecAndComp.queryModelWithoutCompute(getProject(), in2);
        Dataset<Row> df7 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);
        Dataset<Row> df8 = ExecAndComp.queryModelWithoutCompute(getProject(), not_in2);

        Assert.assertFalse(existsIn(df5));
        Assert.assertFalse(existsIn(df6));
        Assert.assertFalse(existsIn(df7));
        Assert.assertFalse(existsIn(df8));
    }

    @Test
    public void testSkipAgg() throws Exception {
        fullBuild("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e");

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String exactly_match1 = "select count (distinct price) as a from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME having count (distinct price)  > 0 ";
        Dataset<Row> m1 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m1));

        String exactly_match2 = "select count(*) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m2 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match2);
        Assert.assertFalse(existsAgg(m2));

        String exactly_match3 = "select LSTG_FORMAT_NAME,sum(price),CAL_DT from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m3 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match3);
        String[] fieldNames = m3.schema().fieldNames();
        Assert.assertFalse(existsAgg(m3));
        Assert.assertTrue(fieldNames[0].contains("LSTG_FORMAT_NAME"));
        Assert.assertTrue(fieldNames[1].contains("GMV_SUM"));
        Assert.assertTrue(fieldNames[2].contains("CAL_DT"));

        String exactly_match4 = "select count (distinct price) as a from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME having count (distinct price)  > 0 ";
        Dataset<Row> m4 = ExecAndComp.queryModelWithoutCompute(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m4));
        // assert results
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", exactly_match3));
        query.add(Pair.newPair("", exactly_match4));
        ExecAndComp.execAndCompare(query, getProject(), ExecAndComp.CompareLevel.SAME, "left");

        String not_match1 = "select count(*) from TEST_KYLIN_FACT";
        Dataset<Row> n1 = ExecAndComp.queryModelWithoutCompute(getProject(), not_match1);
        Assert.assertTrue(existsAgg(n1));
    }

    private boolean existsAgg(Dataset<Row> m1) {
        return !m1.logicalPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                return v1 instanceof Aggregate;
            }
        }).isEmpty();
    }

    private boolean existsIn(Dataset<Row> m1) {
        Option<LogicalPlan> option = m1.logicalPlan().find(new AbstractFunction1<LogicalPlan, Object>() {
            @Override
            public Object apply(LogicalPlan v1) {
                return v1 instanceof Filter;
            }
        });

        if (option.isDefined()) {
            Filter filter = (Filter) option.get();
            return filter.condition().find(new AbstractFunction1<Expression, Object>() {
                @Override
                public Object apply(Expression v1) {
                    return v1 instanceof In;
                }
            }).isDefined();
        } else {
            return false;
        }
    }
}
