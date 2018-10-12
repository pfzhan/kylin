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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegDetails;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.NDictionaryBuilder;
import io.kyligence.kap.engine.spark.builder.NSnapshotBuilder;

@SuppressWarnings("serial")
public class NSparkCubingJobTest extends NLocalWithSparkSessionTest {
    public static final String DEFAULT_PROJECT = "default";

    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(DEFAULT_PROJECT);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        config = getTestConfig();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
    }

    @After
    public void after() throws Exception {
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testMergeBasics() throws IOException {
        final String dataJson1 = "0,1,3,1000\n0,2,2,1000";
        final String dataJson2 = "0,1,2,2000";

        File dataFile1 = File.createTempFile("tmp1", ".csv");
        FileUtils.writeStringToFile(dataFile1, dataJson1, Charset.defaultCharset());
        Dataset<Row> dataset1 = ss.read().csv(dataFile1.getAbsolutePath());
        Assert.assertEquals(2, dataset1.count());
        dataset1.show();

        File dataFile2 = File.createTempFile("tmp2", ".csv");
        FileUtils.writeStringToFile(dataFile2, dataJson2, Charset.defaultCharset());
        Dataset<Row> dataset2 = ss.read().csv(dataFile2.getAbsolutePath());
        Assert.assertEquals(1, dataset2.count());
        dataset2.show();

        Dataset<Row> dataset3 = dataset2.union(dataset1);
        Assert.assertEquals(3, dataset3.count());
        dataset3.show();
        dataFile1.delete();
        dataFile2.delete();
    }

    @Test
    public void testBuildDictionary() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow("ncube_basic");

        NDataSegment seg = df.copy().getLastSegment();
        seg.setDictionaries(null);
        Assert.assertEquals(0, seg.getDictionaries().size());
        NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(df.getCubePlan(), seg.getSegRange());
        Dataset<Row> ds = NJoinedFlatTable.generateDataset(flatTable, ss);

        NDictionaryBuilder dictionaryBuilder = new NDictionaryBuilder(seg, ds);
        seg = dictionaryBuilder.buildDictionary();
        Assert.assertEquals(21, seg.getDictionaries().size());
    }

    @Test
    public void testBuildSnapshot() throws Exception {
        KylinConfig config = getTestConfig();
        System.out.println(getTestConfig().getMetadataUrl());
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow("ncube_basic");

        NDataSegment seg = df.copy().getLastSegment();
        seg.setSnapshots(null);
        Assert.assertEquals(0, seg.getSnapshots().size());
        NSnapshotBuilder builder = new NSnapshotBuilder(seg, ss);
        seg = builder.buildSnapshot();

        Assert.assertEquals(7, seg.getSnapshots().size());
    }

    @Test
    @Ignore("should be covered by nencodingtest")
    public void testBuildWithEncoding() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        config.setProperty("kylin.job.scheduler.provider.110",
                "io.kyligence.kap.job.impl.threadpool.NDefaultScheduler");
        config.setProperty("kylin.job.scheduler.default", "110");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        NExecutableManager execMgr = NExecutableManager.getInstance(config, "default");

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("test_encoding");
        NDataSegment toBeBuild = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();

        List<NCuboidLayout> round1 = new ArrayList<>();
        round1.add(layouts.get(0));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round1, df.getName());
        for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
            NCuboidLayout layout = NCuboidLayoutChooser.selectLayoutForBuild(toBeBuild,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertEquals(null, layout);
        }

        // Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(toBeBuild), Sets.newLinkedHashSet(round1),
                "ADMIN");
        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        NDataflow ndf = dsMgr.getDataflow("test_encoding");
        NDataSegment seg = ndf.getLastSegment();
        List<String[]> resultFromLayout = Lists.newArrayList();
        for (Object[] data : getCuboidDataAfterDecoding(seg, 1)) {
            String[] res = new String[data.length];
            for (int i = 0; i < data.length; i++)
                res[i] = data[i] == null ? null : data[i].toString();

            resultFromLayout.add(res);
        }
        String[] e1 = new String[] { "1", "1.1", "string_dict", "-62135769600000", "0", "T", "true", "1", "fix_length",
                "FF00FF", "-32767", "-32767", "00010101", "-62135769600000", "-62135769600000", "-62135769600000", "0",
                "1" };
        String[] e2 = new String[] { "2", "2.2", "string_dict", "253402214400000", "2147483646000", "F", "false", "0",
                "fix_length", "1A2BFF", "32767", "32767", "99991231", "253402214400000", "253402214400000",
                "253402214400000", "2147483647000", "1" };
        String[] e3 = new String[18];
        e3[17] = "1";
        Assert.assertArrayEquals(e3, resultFromLayout.get(0));
        Assert.assertArrayEquals(e1, resultFromLayout.get(1));
        Assert.assertArrayEquals(e2, resultFromLayout.get(2));
    }

    @Test
    public void testBuildJob() throws Exception {
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("ncube_basic");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = new ArrayList<>();
        round1.add(layouts.get(0));
        round1.add(layouts.get(1));
        round1.add(layouts.get(2));
        round1.add(layouts.get(3));
        round1.add(layouts.get(7));

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
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

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
        round2.add(layouts.get(4));
        round2.add(layouts.get(5));
        round2.add(layouts.get(6));
        round2.add(layouts.get(8));

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

        validateCube(0);
        validateTableIndex(0);
    }

    @Test
    public void testCancelCubingJob() throws Exception {
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = new ArrayList<>();
        round1.add(layouts.get(0));
        round1.add(layouts.get(1));
        round1.add(layouts.get(2));
        round1.add(layouts.get(3));
        round1.add(layouts.get(7));
        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN");
        execMgr.addJob(job);
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());
        while (true) {
            if (execMgr.getJob(job.getId()).getStatus().equals(ExecutableState.RUNNING)) {
                Thread.sleep(1000);
                break;
            }
        }
        Class clazz = NDefaultScheduler.class;
        Field field = clazz.getDeclaredField("threadToInterrupt");
        field.setAccessible(true);
        ConcurrentHashMap<String, Thread> threadToInterrupt = (ConcurrentHashMap<String, Thread>) field.get(clazz);
        Assert.assertEquals(true, threadToInterrupt.containsKey(job.getId()));
        Thread thread = threadToInterrupt.get(job.getId());
        Assert.assertEquals(false, thread.isInterrupted());
        job.cancelJob();
        Assert.assertEquals(false, threadToInterrupt.containsKey(job.getId()));
        waitThreadInterupt(thread, 60000);
        Assert.assertEquals(true, thread.isInterrupted());
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());
        execMgr.discardJob(job.getId());
    }

    @Test
    public void testCancelMergingJob() throws Exception {
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        long start = SegmentRange.dateToLong("2011-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout>newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout>newLinkedHashSet(layouts));
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(firstMergeJob);
        while (true) {
            if (execMgr.getJob(firstMergeJob.getId()).getStatus().equals(ExecutableState.RUNNING)) {
                Thread.sleep(1000);
                break;
            }
        }
        Class clazz = NDefaultScheduler.class;
        Field field = clazz.getDeclaredField("threadToInterrupt");
        field.setAccessible(true);
        ConcurrentHashMap<String, Thread> threadToInterrupt = (ConcurrentHashMap<String, Thread>) field.get(clazz);
        Assert.assertEquals(true, threadToInterrupt.containsKey(firstMergeJob.getId()));
        Thread thread = threadToInterrupt.get(firstMergeJob.getId());
        Assert.assertEquals(false, thread.isInterrupted());
        firstMergeJob.cancelJob();
        Assert.assertEquals(false, threadToInterrupt.containsKey(firstMergeJob.getId()));
        waitThreadInterupt(thread, 100000);
        Assert.assertEquals(true, thread.isInterrupted());
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertEquals(df.getSegment(firstMergeJob.getSparkCubingStep().getSegmentIds()), null);
        execMgr.discardJob(firstMergeJob.getId());
    }

    private void waitThreadInterupt(Thread thread, int maxWaitTime) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < maxWaitTime) {
            if (thread.isInterrupted()) {
                break;
            }
        }
        if (System.currentTimeMillis() - start >= maxWaitTime) {
            throw new RuntimeException("too long wait time");
        }
    }

    @Ignore
    @Test
    //should it test merge case?
    public void testRuleBasedCube() throws Exception {
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow("rule_based_cube");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("rule_based_cube");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = Lists.newArrayList(layouts);

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
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

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
        round2.add(layouts.get(4));
        round2.add(layouts.get(5));
        round2.add(layouts.get(6));
        round2.add(layouts.get(8));

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

        validateCube(0);
        validateTableIndex(0);
    }

    @Test
    @Ignore("the build process is tested in NMeasuresTest, no need to build again")
    public void testMeasuresFullBuild() throws Exception {
        String cubeName = "ncube_full_measure_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow(cubeName);
        builCuboid(cubeName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.<NCuboidLayout> newLinkedHashSet(df.getCubePlan().getAllCuboidLayouts()));
        List<Object[]> resultFromLayout = getCuboidDataAfterDecoding(
                NDataflowManager.getInstance(config, DEFAULT_PROJECT).getDataflow(cubeName).getSegment(1), 1);
        for (Object[] row : resultFromLayout) {
            if (row[0].equals("10000000158")) {
                Assert.assertEquals("4", row[1].toString());// COUNT(*)
                Assert.assertEquals("40000000632", row[2].toString());// SUM(ID1)
                Assert.assertEquals(Double.valueOf("2637.703"), Double.valueOf(row[3].toString()), 0.000001);// SUM(PRICE2)
                Assert.assertEquals("10000000158", row[10].toString());// MIN(ID1)
                Assert.assertEquals(10000000158.0, ((TopNCounter) row[11]).getCounters()[0], 0.000001);// TOPN(ID1)
                Assert.assertEquals("3", row[15].toString());// HLL(NAME1)
                Assert.assertEquals("4", row[16].toString());
                Assert.assertEquals(4, ((PercentileCounter) row[21]).getRegisters().size());// percentile(PRICE1)
                Assert.assertEquals("478.63", row[25].toString());// HLL(NAME1, PRICCE1)
            }
            // verify the all null value aggregate
            if (row[0].equals("10000000162")) {
                Assert.assertEquals("3", row[1].toString());// COUNT(*)
                Assert.assertEquals(Double.valueOf("0"), Double.valueOf(row[3].toString()), 0.000001);// SUM(PRICE2)
                Assert.assertEquals(Double.valueOf("0"), Double.valueOf(row[4].toString()), 0.000001);// SUM(PRICE3)
                Assert.assertEquals("0", row[5].toString());// MAX(PRICE3)
                Assert.assertEquals("10000000162", row[6].toString());// MIN(ID1)
                Assert.assertEquals("0", row[15].toString());// HLL(NAME1)
                Assert.assertEquals("0", row[16].toString());// HLL(NAME2)
                Assert.assertEquals(0, ((PercentileCounter) row[21]).getRegisters().size());// percentile(PRICE1)
                Assert.assertEquals("0.0", row[25].toString());// HLL(NAME1, PRICE1)
            }
        }
    }

    @Test
    @Ignore("the build process is tested in manual & auto test, no need to build again")
    public void testMergeJob() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NExecutableManager execMgr = NExecutableManager.getInstance(config, DEFAULT_PROJECT);

        NDataflow df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        /**
         * Round1. Build 4 segment
         */
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        long start = SegmentRange.dateToLong("2011-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));
        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        builCuboid("ncube_basic", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<NCuboidLayout> newLinkedHashSet(layouts));

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(firstMergeJob);
        // wait job done

        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment secondMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2013-01-01"), SegmentRange.dateToLong("2015-06-01")), false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, Sets.newLinkedHashSet(layouts),
                "ADMIN");
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob));

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow("ncube_basic").getSegment(4);
        NDataSegment secondSegment = dsMgr.getDataflow("ncube_basic").getSegment(5);
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01"),
                SegmentRange.dateToLong("2013-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2015-01-01")), secondSegment.getSegRange());
        Assert.assertEquals(19, firstSegment.getDictionaries().size());
        Assert.assertEquals(19, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());
    }

    private void validateCube(int segmentId) {
        NDataflow df = NDataflowManager.getInstance(config, DEFAULT_PROJECT).getDataflow("ncube_basic");
        NDataSegment seg = df.getSegment(segmentId);

        // check row count in NDataSegDetails
        Assert.assertEquals(10000, seg.getCuboid(1).getRows());
        Assert.assertEquals(10000, seg.getCuboid(2).getRows());
        Assert.assertEquals(10000, seg.getCuboid(1001).getRows());
        Assert.assertEquals(10000, seg.getCuboid(1002).getRows());

        /*List<String[]> resultFromLayout = getCuboidDataAfterDecoding(seg, 1);
        long actualCount = 0L;
        long actualMax = 0L;
        for (String[] row : resultFromLayout) {
            if (row[0].equals("402")) {
                actualCount = Long.parseLong(row[2]);
                actualMax = Long.parseLong(row[3]);
            }
        }
        Assert.assertEquals(402, resultFromLayout.size());
        Assert.assertEquals(402, resultFromLayout.size());
        Assert.assertEquals(2, actualCount);
        Assert.assertEquals(1, actualMax);*/

        /* List<Object[]> resultFromLayout = getCuboidDataAfterDecoding(seg, 3001);
        for (Object[] row : resultFromLayout) {
            if (row[0].equals("402")) {
                Assert.assertEquals("1", row[4].toString());
                Assert.assertEquals("1", row[5].toString());
                Assert.assertEquals(1, ((RoaringBitmapCounter) row[6]).getCount()); //COUNT DISTINCT bitmap
            }
        }*/
    }

    private void validateTableIndex(int segmentId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment seg = df.getSegment(segmentId);
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, 1000000001);
        NCuboidLayout layout = dataCuboid.getCuboidLayout();
        Assert.assertEquals(10000, seg.getCuboid(1000000001).getRows());

        List<Object[]> resultFromLayout = getCuboidDataAfterDecoding(seg, 1000000001);
        // The table index cuboid should sort by column 0, assert it's order.
        Assert.assertEquals("Australia", resultFromLayout.get(0)[1].toString());
        Assert.assertEquals("Australia", resultFromLayout.get(1)[1].toString());
        Assert.assertEquals("英国", resultFromLayout.get(9998)[1].toString());
        Assert.assertEquals("英国", resultFromLayout.get(9999)[1].toString());
    }
}
