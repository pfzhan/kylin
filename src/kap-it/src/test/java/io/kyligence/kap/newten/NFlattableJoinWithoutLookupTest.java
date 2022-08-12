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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.common.SparderQueryTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.util.ExecAndComp;

public class NFlattableJoinWithoutLookupTest extends NLocalWithSparkSessionTest {

    private NDataflowManager dfMgr = null;

    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.job.flat-table-join-without-lookup", "true");
        overwriteSystemProp("kylin.engine.persist-flattable-enabled", "false");
        this.createTestMetadata("src/test/resources/ut_meta/flattable_without_join_lookup");
        dfMgr = NDataflowManager.getInstance(getTestConfig(), getProject());

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "flattable_without_join_lookup";
    }

    private String sql = "select  CAL_DT as dt1, cast (TEST_ORDER_STRING.TEST_TIME_ENC as timestamp) as ts2, cast(TEST_ORDER_STRING.TEST_DATE_ENC  as date) as dt2,TEST_ORDER.ORDER_ID, count(*) FROM TEST_ORDER LEFT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID LEFT JOIN TEST_ORDER_STRING on TEST_ORDER.ORDER_ID = TEST_ORDER_STRING.ORDER_ID group by TEST_ORDER.ORDER_ID ,TEST_ORDER_STRING.TEST_TIME_ENC , TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT order by TEST_ORDER.ORDER_ID,TEST_ORDER_STRING.TEST_TIME_ENC , TEST_ORDER_STRING.TEST_DATE_ENC ,CAL_DT ";

    @Test
    public void testFlattableWithoutLookup() throws Exception {
        buildSegs("8c670664-8d05-466a-802f-83c023b56c77", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        Dataset<Row> cube = ExecAndComp.queryModel(getProject(), sql);
        Dataset<Row> pushDown = ExecAndComp.querySparkSql(sql);
        String msg = SparderQueryTest.checkAnswer(cube, pushDown, true);
        Assert.assertNull(msg);
    }

    @Test
    public void testFlattableJoinLookup() throws Exception {
        buildSegs("9cde9d25-9334-4b92-b229-a00f49453757", 10001L);
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        Dataset<Row> cube = ExecAndComp.queryModel(getProject(), sql);
        Dataset<Row> pushDown = ExecAndComp.querySparkSql(sql);
        String msg = SparderQueryTest.checkAnswer(cube, pushDown, true);
        Assert.assertNull(msg);
    }

    private void buildSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts),
                true);
    }
}
