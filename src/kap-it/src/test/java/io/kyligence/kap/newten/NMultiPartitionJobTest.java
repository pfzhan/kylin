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
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.util.ExecAndComp;
import lombok.val;

public class NMultiPartitionJobTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        overwriteSystemProp("kylin.model.multi-partition-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Override
    public String getProject() {
        return "multi_partition";
    }

    @Test
    public void testConstantComputeColumn() throws Exception {
        String dfID = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dfManager.getDataflow(dfID);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val layouts = df.getIndexPlan().getAllLayouts();
        long startTime = SegmentRange.dateToLong("2020-11-05");
        long endTime = SegmentRange.dateToLong("2020-11-06");
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);

        val buildPartitions = Lists.<String[]> newArrayList();
        buildPartitions.add(new String[] { "usa" });
        buildPartitions.add(new String[] { "cn" });
        buildPartitions.add(new String[] { "africa" });

        indexDataConstructor.buildIndex(dfID, segmentRange, Sets.newLinkedHashSet(layouts), true, buildPartitions);
        String sqlHitCube = " select count(1) from TEST_BANK_INCOME t1 inner join TEST_BANK_LOCATION t2 on t1. COUNTRY = t2. COUNTRY "
                + " where  t1.dt = '2020-11-05' ";
        List<String> hitCubeResult = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube).collectAsList()
                .stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(1, hitCubeResult.size());

        // will auto offline
        overwriteSystemProp("kylin.model.multi-partition-enabled", "false");
        long startTime2 = SegmentRange.dateToLong("2020-11-06");
        long endTime2 = SegmentRange.dateToLong("2020-11-07");
        val segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(startTime2, endTime2);
        indexDataConstructor.buildIndex(dfID, segmentRange2, Sets.newLinkedHashSet(layouts), true, buildPartitions);
        NDataflow df2 = dfManager.getDataflow(dfID);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, df2.getStatus());
    }

    @Test
    public void testGlobalDict() throws Exception {
        String dfID = "0080e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dfManager.getDataflow(dfID);
        val update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        val layouts = df.getIndexPlan().getAllLayouts();
        long startTime = SegmentRange.dateToLong("2020-11-01");
        long endTime = SegmentRange.dateToLong("2020-11-06");
        val segmentRange = new SegmentRange.TimePartitionedSegmentRange(startTime, endTime);

        val segmentId = indexDataConstructor.buildIndex(dfID, segmentRange, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "un" }));
        String sqlHitCube = " select count(distinct INCOME) from TEST_BANK_INCOME t1 "
                + "where t1.dt < '2020-11-06' and t1.dt >= '2020-11-01'";
        String whereSql = " and COUNTRY in ('un')";
        List<Row> result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(2, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un', 'usa')";
        indexDataConstructor.buildMultiPartition(dfID, segmentId, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "usa" }));
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(5, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un', 'usa', 'cn')";
        indexDataConstructor.buildMultiPartition(dfID, segmentId, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "cn" }));
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(9, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('un')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(2, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('usa')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(5, result.get(0).getLong(0));

        whereSql = " and COUNTRY in ('cn')";
        result = ExecAndComp.queryModelWithoutCompute(getProject(), sqlHitCube + whereSql).collectAsList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals(6, result.get(0).getLong(0));

        // will auto offline
        overwriteSystemProp("kylin.model.multi-partition-enabled", "false");
        long startTime2 = SegmentRange.dateToLong("2020-11-06");
        long endTime2 = SegmentRange.dateToLong("2020-11-07");
        val segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(startTime2, endTime2);
        indexDataConstructor.buildIndex(dfID, segmentRange2, Sets.newLinkedHashSet(layouts), true,
                Lists.<String[]> newArrayList(new String[] { "africa" }));
        NDataflow df2 = dfManager.getDataflow(dfID);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, df2.getStatus());
    }
}
