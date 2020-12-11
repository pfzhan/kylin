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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

public class NMultiPartitionJobTest extends NLocalWithSparkSessionTest {
    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.model.multi-partition-enabled", "true");
        this.createTestMetadata("src/test/resources/ut_meta/multi_partition");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
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

        val buildPartitions = Lists.<String[]>newArrayList();
        buildPartitions.add(new String[]{"usa"});
        buildPartitions.add(new String[]{"cn"});
        buildPartitions.add(new String[]{"africa"});

        buildCuboid(dfID, segmentRange, Sets.newLinkedHashSet(layouts), getProject(), true, buildPartitions);
        String sqlHitCube = " select count(1) from TEST_BANK_INCOME t1 inner join TEST_BANK_LOCATION t2 on t1. COUNTRY = t2. COUNTRY " +
                " where  t1.dt = '2020-11-05' ";
        List<String> hitCubeResult = NExecAndComp.queryFromCube(getProject(), sqlHitCube)
                .collectAsList().stream().map(Row::toString).collect(Collectors.toList());
        Assert.assertEquals(1, hitCubeResult.size());

        // will auto offline
        System.setProperty("kylin.model.multi-partition-enabled", "false");
        long startTime2 = SegmentRange.dateToLong("2020-11-06");
        long endTime2 = SegmentRange.dateToLong("2020-11-07");
        val segmentRange2 = new SegmentRange.TimePartitionedSegmentRange(startTime2, endTime2);
        buildCuboid(dfID, segmentRange2, Sets.newLinkedHashSet(layouts), getProject(), true, buildPartitions);
        NDataflow df2 = dfManager.getDataflow(dfID);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, df2.getStatus());
    }
}