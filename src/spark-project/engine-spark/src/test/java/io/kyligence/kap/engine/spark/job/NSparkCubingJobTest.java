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

import static io.kyligence.kap.metadata.cube.model.NBatchConstants.P_LAYOUT_IDS;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.DefaultContext;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.percentile.PercentileCounter;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.DFSnapshotBuilder;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.engine.spark.storage.ParquetStorage;
import io.kyligence.kap.metadata.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

@SuppressWarnings("serial")
public class NSparkCubingJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Before
    public void setup() throws Exception {
        ss.sparkContext().setLogLevel("ERROR");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");

        NDefaultScheduler.destroyInstance();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }

        config = getTestConfig();
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
    public void testBuildSnapshot() throws Exception {
        KylinConfig config = getTestConfig();
        System.out.println(getTestConfig().getMetadataUrl());
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        NDataSegment seg = df.copy().getLastSegment();
        seg.setSnapshots(null);
        Assert.assertEquals(0, seg.getSnapshots().size());
        DFSnapshotBuilder builder = new DFSnapshotBuilder(seg, ss);
        seg = builder.buildSnapshot();

        Assert.assertEquals(7, seg.getSnapshots().size());
    }

    @Test
    @Ignore("should be covered by nencodingtest")
    public void testBuildWithEncoding() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.job.scheduler.provider.110",
                "io.kyligence.kap.job.impl.threadpool.NDefaultScheduler");
        config.setProperty("kylin.job.scheduler.default", "110");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        NExecutableManager execMgr = NExecutableManager.getInstance(config, "default");

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("test_encoding");
        NDataSegment toBeBuild = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();

        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(layouts.get(0));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(toBeBuild,
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
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(df.getIndexPlan().getCuboidLayout(20_000_020_001L));
        round1.add(df.getIndexPlan().getCuboidLayout(1_000_001L));
        round1.add(df.getIndexPlan().getCuboidLayout(30001L));
        round1.add(df.getIndexPlan().getCuboidLayout(10002L));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertEquals(null, layout);
        }

        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN");
        NSparkCubingStep sparkStep = job.getSparkCubingStep();
        StorageURL distMetaUrl = StorageURL.valueOf(sparkStep.getDistMetaUrl());
        Assert.assertEquals("hdfs", distMetaUrl.getScheme());
        Assert.assertTrue(distMetaUrl.getParameter("path").startsWith(config.getHdfsWorkingDirectory()));

        final NSparkAnalysisStep analysisStep = job.getSparkAnalysisStep();
        Assert.assertNotNull(analysisStep);

        // launch the job
        execMgr.addJob(job);

        // wait job done
        ExecutableState status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val merger = new AfterBuildResourceMerger(config, getProject(), JobTypeEnum.INC_BUILD);
        merger.mergeAfterIncrement(df.getUuid(), oneSeg.getId(), ExecutableUtils.getLayoutIds(sparkStep),
                ExecutableUtils.getRemoteStore(config, sparkStep));
        merger.mergeAnalysis(job.getSparkAnalysisStep());

        /**
         * Round2. Build new layouts, should reuse the data from already existing cuboid.
         * Notice: After round1 the segment has been updated, need to refresh the cache before use the old one.
         */
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(df.getIndexPlan().getCuboidLayout(1L));
        round2.add(df.getIndexPlan().getCuboidLayout(20_000_000_001L));
        round2.add(df.getIndexPlan().getCuboidLayout(20001L));
        round2.add(df.getIndexPlan().getCuboidLayout(10001L));

        //update seg
        val df2 = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        oneSeg = df2.getSegment(oneSeg.getId());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(round2, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertTrue(layout != null);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        merger.mergeAfterCatchup(df2.getUuid(), Sets.newHashSet(oneSeg.getId()),
                ExecutableUtils.getLayoutIds(job.getSparkCubingStep()),
                ExecutableUtils.getRemoteStore(config, job.getSparkCubingStep()));
        merger.mergeAnalysis(job.getSparkAnalysisStep());

        validateCube(df2.getSegments().getFirstSegment().getId());
        validateTableIndex(df2.getSegments().getFirstSegment().getId());
        validateTableExt(df.getModel());
    }

    @Test
    public void testCancelCubingJob() throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = new ArrayList<>();
        round1.add(layouts.get(0));
        round1.add(layouts.get(1));
        round1.add(layouts.get(2));
        round1.add(layouts.get(3));
        round1.add(layouts.get(7));
        // Round1. Build new segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round1), "ADMIN");
        execMgr.addJob(job);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
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
        //FIXME Unstable, will fix in #7302
        //        waitThreadInterrupt(thread, 60000);
        //        Assert.assertEquals(true, thread.isInterrupted());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        execMgr.discardJob(job.getId());
    }

    @Test
    public void testCancelMergingJob() throws Exception {

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(0, df.getSegments().size());
        // ready dataflow, segment, cuboid layout
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2011-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                UUID.randomUUID().toString());
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
        //FIXME Unstable, will fix in #7302
        //waitThreadInterrupt(thread, 1000000);
        //Assert.assertEquals(true, thread.isInterrupted());
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertEquals(df.getSegment(firstMergeJob.getSparkMergingStep().getSegmentIds().iterator().next()), null);
        execMgr.discardJob(firstMergeJob.getId());
    }

    private void waitThreadInterrupt(Thread thread, int maxWaitTime) {
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
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow("rule_based_cube");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("rule_based_cube");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        List<LayoutEntity> round1 = Lists.newArrayList(layouts);

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromLayouts(round1, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
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
        List<LayoutEntity> round2 = new ArrayList<>();
        round2.add(layouts.get(4));
        round2.add(layouts.get(5));
        round2.add(layouts.get(6));
        round2.add(layouts.get(8));

        //update seg
        oneSeg = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegment(oneSeg.getId());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(round2, df.getUuid());
        for (IndexEntity rootCuboid : nSpanningTree.getRootIndexEntities()) {
            LayoutEntity layout = NCuboidLayoutChooser.selectLayoutForBuild(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertTrue(layout != null);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        val df2 = dsMgr.getDataflow(df.getUuid());
        validateCube(df2.getSegments().getFirstSegment().getId());
        validateTableIndex(df2.getSegments().getFirstSegment().getId());
    }

    @Test
    @Ignore("the build process is tested in NMeasuresTest, no need to build again")
    public void testMeasuresFullBuild() throws Exception {
        String cubeName = "ncube_full_measure_test";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(cubeName);
        buildCuboid(cubeName, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.<LayoutEntity> newLinkedHashSet(df.getIndexPlan().getAllLayouts()), true);
        List<Object[]> resultFromLayout = getCuboidDataAfterDecoding(
                NDataflowManager.getInstance(config, getProject()).getDataflow(cubeName).getSegments().get(1), 1);
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
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NExecutableManager execMgr = NExecutableManager.getInstance(config, getProject());

        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        /**
         * Round1. Build 4 segment
         */
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        long start = SegmentRange.dateToLong("2011-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2013-01-01");
        end = SegmentRange.dateToLong("2013-06-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);
        start = SegmentRange.dateToLong("2013-06-01");
        end = SegmentRange.dateToLong("2015-01-01");
        buildCuboid("89af4ee2-2cdb-4b07-b39e-4c29856309aa", new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.<LayoutEntity> newLinkedHashSet(layouts), true);

        /**
         * Round2. Merge two segments
         */
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2010-01-02"), SegmentRange.dateToLong("2013-01-01")), false);
        NSparkMergingJob firstMergeJob = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN",
                UUID.randomUUID().toString());
        execMgr.addJob(firstMergeJob);
        // wait job done

        Assert.assertEquals(ExecutableState.SUCCEED, wait(firstMergeJob));

        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment secondMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2013-01-01"), SegmentRange.dateToLong("2015-06-01")), false);
        NSparkMergingJob secondMergeJob = NSparkMergingJob.merge(secondMergeSeg, Sets.newLinkedHashSet(layouts),
                "ADMIN", UUID.randomUUID().toString());
        execMgr.addJob(secondMergeJob);
        // wait job done
        Assert.assertEquals(ExecutableState.SUCCEED, wait(secondMergeJob));

        /**
         * validate cube segment info
         */
        NDataSegment firstSegment = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().get(4);
        NDataSegment secondSegment = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getSegments().get(5);
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01"),
                SegmentRange.dateToLong("2013-01-01")), firstSegment.getSegRange());
        Assert.assertEquals(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2013-01-01"),
                SegmentRange.dateToLong("2015-01-01")), secondSegment.getSegRange());
        Assert.assertEquals(19, firstSegment.getDictionaries().size());
        Assert.assertEquals(19, secondSegment.getDictionaries().size());
        Assert.assertEquals(7, firstSegment.getSnapshots().size());
        Assert.assertEquals(7, secondSegment.getSnapshots().size());
    }

    private void validateCube(String segmentId) {
        NDataflow df = NDataflowManager.getInstance(config, getProject())
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment seg = df.getSegment(segmentId);

        // check row count in NDataSegDetails
        Assert.assertEquals(10000, seg.getLayout(1).getRows());
        Assert.assertEquals(10000, seg.getLayout(10001).getRows());
        Assert.assertEquals(10000, seg.getLayout(10002).getRows());

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

    private void validateTableIndex(String segmentId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NDataSegment seg = df.getSegment(segmentId);
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataLayout dataCuboid = NDataLayout.newDataLayout(segCuboids, 20000000001L);
        LayoutEntity layout = dataCuboid.getLayout();
        Assert.assertEquals(10000, seg.getLayout(20000000001L).getRows());

        ParquetStorage storage = new ParquetStorage();
        Dataset<Row> ret = storage.getFrom(NSparkCubingUtil.getStoragePath(dataCuboid), ss);
        Assert.assertEquals("Australia", ret.collectAsList().get(0).apply(1).toString());
        Assert.assertEquals("Australia", ret.collectAsList().get(1).apply(1).toString());
        Assert.assertEquals("英国", ret.collectAsList().get(9998).apply(1).toString());
        Assert.assertEquals("英国", ret.collectAsList().get(9999).apply(1).toString());
    }

    private void validateTableExt(NDataModel dataModel) throws IOException {
        val rootFactTbl = dataModel.getRootFactTableName();
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        final TableDesc rootTableDesc = tableMetadataManager.getTableDesc(rootFactTbl);
        final TableExtDesc rootTableExt = tableMetadataManager.getTableExtIfExists(rootTableDesc);
        Assert.assertNotNull(rootTableExt);
        Assert.assertEquals(10000, rootTableExt.getTotalRows());
        Assert.assertEquals(11, rootTableExt.getColumnStats().size());
        Assert.assertEquals(1, rootTableExt.getLoadingRange().size());
        assertColumnStats(rootTableExt);

        for (final JoinTableDesc lookupDesc : dataModel.getJoinTables()) {
            final TableRef lookupTableRef = lookupDesc.getTableRef();
            final TableExtDesc lookupTableExt = tableMetadataManager.getTableExtIfExists(lookupTableRef.getTableDesc());
            Assert.assertNotNull(lookupTableExt);
            assertColumnStats(lookupTableExt);
        }

    }

    private void assertColumnStats(TableExtDesc tableExt) throws IOException {
        val fs = HadoopUtil.getWorkingFileSystem();
        val colStatsHdfsPath = NTableMetadataManager.ColumnStatsStore.getInstance(tableExt, getTestConfig())
                .getColumnStatsPath();
        val actualResult = fs.listStatus(new Path(colStatsHdfsPath));
        Assert.assertEquals(1, actualResult.length);
        Assert.assertEquals(tableExt.getColStatsPath(), actualResult[0].getPath().toString());

        for (val colStats : tableExt.getColumnStats()) {
            Assert.assertTrue(colStats.getRangeHLLC().size() > 0);
        }
    }

    @Test
    public void testNSparkCubingJobUsingModelUuid() {
        String modelAlias = "nmodel_basic_alias";
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // set model alias
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, getProject());
        NDataModel dataModel = dataModelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        dataModel.setAlias(modelAlias);
        dataModelManager.updateDataModelDesc(dataModel);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
        df = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // ready dataflow, segment, cuboid layout
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(layouts), "ADMIN");

        String targetSubject = job.getTargetModel();
        Assert.assertEquals(dataModel.getUuid(), targetSubject);
    }

    @Test
    public void testSparkExecutable_WrapConfig() {
        val project = "default";
        ExecutableContext context = new DefaultContext(Maps.newConcurrentMap(), getTestConfig());
        NSparkExecutable executable = new NSparkExecutable();
        executable.setProject(project);

        NProjectManager.getInstance(getTestConfig()).updateProject(project, copyForWrite -> {
            copyForWrite.getOverrideKylinProps().put("kylin.engine.spark-conf.spark.locality.wait", "10");
        });
        // get SparkConfigOverride from project overrideProps
        KylinConfig config = executable.wrapConfig(context);
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertNull(getTestConfig().getSparkConfigOverride().get("spark.locality.wait"));
        Assert.assertEquals("10", config.getSparkConfigOverride().get("spark.locality.wait"));

        // get SparkConfigOverride from indexPlan overrideProps
        executable.setParam(NBatchConstants.P_DATAFLOW_ID, "89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        NIndexPlanManager.getInstance(getTestConfig(), project).updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa",
                copyForWrite -> {
                    copyForWrite.getOverrideProps().put("kylin.engine.spark-conf.spark.locality.wait", "20");
                });
        config = executable.wrapConfig(context);
        Assert.assertEquals(getTestConfig(), config.base());
        Assert.assertNull(getTestConfig().getSparkConfigOverride().get("spark.locality.wait"));
        Assert.assertEquals("20", config.getSparkConfigOverride().get("spark.locality.wait"));
    }

    @Test
    public void testLayoutIdMoreThan10000() throws ExecuteException, IOException {
        String path = null;
        try {
            NSparkExecutable executable = new NSparkExecutable();
            Set<Long> randomLayouts = Sets.newHashSet();
            for (int i = 0; i < 100000; i++) {
                randomLayouts.add(RandomUtils.nextLong(1, 100000));
            }
            executable.setParam(P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(randomLayouts));
            executable.dumpArgs();
            Set<Long> layouts = NSparkCubingUtil.str2Longs(executable.getParam(P_LAYOUT_IDS));
            randomLayouts.removeAll(layouts);
            Assert.assertEquals(0, randomLayouts.size());
        } finally {
            if (path != null) {
                File file = new File(path);
                if (file.exists()) {
                    file.delete();
                }
            }
        }
    }
}
