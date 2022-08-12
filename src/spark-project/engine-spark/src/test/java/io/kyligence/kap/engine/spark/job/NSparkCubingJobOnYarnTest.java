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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.execution.NSparkCubingJob;
import io.kyligence.kap.job.execution.NSparkMergingJob;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.JobContextUtil;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

@Ignore("for test spark job on yarn")
public class NSparkCubingJobOnYarnTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContextForTest(getTestConfig());
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        JobContextUtil.cleanUp();
    }

    @Test
    public void testSparkJobOnYarn() throws IOException, InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.hdfs-working-dir", "hdfs://sandbox/kylin");
        config.setProperty("kylin.env", "DEV");
        config.setProperty("kylin.engine.spark.job-jar", "../assembly/target/ke-assembly-4.0.0-SNAPSHOT-job.jar");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        ExecutableManager execMgr = ExecutableManager.getInstance(config, "default");

        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        NDataSegment oneSeg = dsMgr.appendSegment(df,
                new SegmentRange.TimePartitionedSegmentRange(0L, SegmentRange.dateToLong("2012-06-01")));
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN",
                null);

        // launch the job
        execMgr.addJob(job);

        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        oneSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2012-06-01"), SegmentRange.dateToLong("2013-06-01")));

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN", null);

        // launch the job
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        oneSeg = dsMgr.mergeSegments(df,
                new SegmentRange.TimePartitionedSegmentRange(0L, SegmentRange.dateToLong("2013-06-01")), false);

        NSparkMergingJob mergeJob = NSparkMergingJob.merge(oneSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                RandomUtil.randomUUIDStr());

        // launch the job
        execMgr.addJob(mergeJob);

        // wait job done
        status = wait(mergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

    }

    private ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);

            ExecutableState status = job.getStatus();
            if (!status.isProgressing()) {
                return status;
            }
        }
    }
}
