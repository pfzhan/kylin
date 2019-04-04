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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import scala.runtime.AbstractFunction1;

import java.util.ArrayList;
import java.util.List;

public class NExactlyMatchTest extends NLocalWithSparkSessionTest {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata("src/test/resources/ut_meta/agg_exact_match");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Override
    public String getProject() {
        return "agg_match";
    }

    @Test
    public void testSkipAgg() throws Exception {
        fullBuildCube("c9ddd37e-c870-4ccf-a131-5eef8fe6cb7e", getProject());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());

        String exactly_match1 = "select sum(price) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m1 = NExecAndComp.queryCube(getProject(), exactly_match1);
        Assert.assertFalse(existsAgg(m1));

        String exactly_match2 = "select count(*) from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m2 = NExecAndComp.queryCube(getProject(), exactly_match2);
        Assert.assertFalse(existsAgg(m2));

        String exactly_match3 = "select LSTG_FORMAT_NAME,sum(price),CAL_DT from TEST_KYLIN_FACT group by TRANS_ID, CAL_DT, LSTG_FORMAT_NAME";
        Dataset<Row> m3 = NExecAndComp.queryCube(getProject(), exactly_match3);
        String[] fieldNames = m3.schema().fieldNames();
        Assert.assertFalse(existsAgg(m3));
        Assert.assertTrue(fieldNames[0].contains("LSTG_FORMAT_NAME"));
        Assert.assertTrue(fieldNames[1].contains("GMV_SUM"));
        Assert.assertTrue(fieldNames[2].contains("CAL_DT"));

        // assert results
        List<Pair<String, String>> query = new ArrayList<>();
        query.add(Pair.newPair("", exactly_match3));
        NExecAndComp.execAndCompare(query, getProject(), NExecAndComp.CompareLevel.SAME, "left");


        String not_match1 = "select count(*) from TEST_KYLIN_FACT";
        Dataset<Row> n1 = NExecAndComp.queryCube(getProject(), not_match1);
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
}
