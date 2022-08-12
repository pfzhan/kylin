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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;

public class NMultipleColumnsInTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_meta/multiple_columns_in");

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
        return "multiple_columns_in";
    }

    @Test
    public void test() throws Exception {
        val dfName = "7c670664-8d05-466a-802f-83c023b56c77";
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, SegmentRange.TimePartitionedSegmentRange.createInfinite(), Sets.newLinkedHashSet(layouts),
                true);

        overwriteSystemProp("calcite.keep-in-clause", "true");
        overwriteSystemProp("calcite.convert-multiple-columns-in-to-or", "true");
        runCase();

        overwriteSystemProp("calcite.keep-in-clause", "false");
        runCase();
    }

    private void runCase() throws Exception {
        // test use multiple_columns_in with other filter
        String actual_sql1 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where (IS_EFFECTUAL, LSTG_FORMAT_NAME) in ((false, 'FP-GTC'), (true, 'Auction')) and IS_EFFECTUAL in (false)"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        String expect_sql1 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where ((IS_EFFECTUAL = false and LSTG_FORMAT_NAME = 'FP-GTC') or (IS_EFFECTUAL = true and LSTG_FORMAT_NAME = 'Auction')) and IS_EFFECTUAL in (false)"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        assertSameResults(actual_sql1, expect_sql1);

        // test use multiple_columns_in alone
        String actual_sql2 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where (IS_EFFECTUAL, LSTG_FORMAT_NAME) in ((false, 'FP-GTC'), (true, 'Auction'))"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";

        String expect_sql2 = "select count(*) as val, IS_EFFECTUAL, LSTG_FORMAT_NAME from TEST_KYLIN_FACT "
                + "where ((IS_EFFECTUAL = false and LSTG_FORMAT_NAME = 'FP-GTC') or (IS_EFFECTUAL = true and LSTG_FORMAT_NAME = 'Auction'))"
                + "group by IS_EFFECTUAL, LSTG_FORMAT_NAME order by val";
        assertSameResults(actual_sql2, expect_sql2);
    }

    private void assertSameResults(String actualSql, String expectSql) throws Exception {
        List<String> actualResults = ExecAndComp.queryModelWithoutCompute(getProject(), actualSql).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        List<String> expectResults = ExecAndComp.queryModelWithoutCompute(getProject(), expectSql).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertTrue(actualResults.containsAll(expectResults) && expectResults.containsAll(actualResults));
    }
}
