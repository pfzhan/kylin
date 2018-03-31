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
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.job.impl.threadpool.NDefaultScheduler;

@Ignore("This is a convenient way to submit newten spark cubing to yarn.")
public class NSparkCubingJobOnYarnTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
        System.setProperty("kylin.hadoop.conf.dir",
                "/Users/wangcheng/Developments/KAP/extensions/examples/test_case_data/sandbox");
        System.setProperty("SPARK_HOME", "/Users/wangcheng/Developments/KAP/build/spark");
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

    @Test
    public void test1() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kylin.env.hdfs-working-dir", "hdfs://sandbox/kylin");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        config.setProperty("kylin.source.provider.11", "io.kyligence.kap.engine.spark.source.NSparkDataSource");
        config.setProperty("kylin.env", "DEV");
        config.setProperty("kylin.engine.mr.job-jar",
                "/Users/wangcheng/Developments/KAP/extensions/assembly/target/kap-assembly-3.0.0-SNAPSHOT-job.jar");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        ExecutableManager execMgr = ExecutableManager.getInstance(config);

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = new ArrayList<>();
        round1.add(layouts.get(4));
        round1.add(layouts.get(5));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round1, df.getName());
        for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
            NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertEquals(null, layout);
        }

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN");
        NSparkCubingStep sparkStep = (NSparkCubingStep) job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        /**
         * Round2. Build new layouts, should reuse the data from already existing cuboid.
         * Notice: After round1 the segment has been updated, need to refresh the cache before use the old one.
         */
        List<NCuboidLayout> round2 = new ArrayList<>();
        round2.add(layouts.get(0));
        round2.add(layouts.get(1));
        round2.add(layouts.get(2));
        round2.add(layouts.get(3));

        //update seg
        oneSeg = dsMgr.getDataflow("ncube_basic").getSegment(oneSeg.getId());
        nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round2, df.getName());
        for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
            NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertTrue(layout != null);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kylin.env.hdfs-working-dir", "hdfs://sandbox/kylin");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        config.setProperty("kylin.source.provider.11", "io.kyligence.kap.engine.spark.source.NSparkDataSource");
        config.setProperty("kylin.env", "DEV");
        config.setProperty("kylin.engine.mr.job-jar",
                "/Users/wangcheng/Developments/KAP/extensions/assembly/target/kap-assembly-3.0.0-SNAPSHOT-job.jar");
        config.setProperty("kap.storage.columnar.shard-size-mb", "16");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        ExecutableManager execMgr = ExecutableManager.getInstance(config);

        NDataflow df = dsMgr.getDataflow("ssb");
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();

        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN");

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
    }

    private ExecutableState wait(AbstractExecutable job) throws InterruptedException {
        while (true) {
            Thread.sleep(500);

            ExecutableState status = job.getStatus();
            if (!status.isReadyOrRunning()) {
                return status;
            }
        }
    }
}
