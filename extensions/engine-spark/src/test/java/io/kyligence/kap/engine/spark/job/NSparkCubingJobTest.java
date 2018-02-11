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
import io.kyligence.kap.job.impl.threadpool.NDefaultScheduler;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import io.kyligence.kap.cube.cuboid.NCuboidLayoutChooser;
import io.kyligence.kap.cube.cuboid.NSpanningTree;
import io.kyligence.kap.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.cube.kv.NCubeDimEncMap;
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
import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.engine.spark.builder.NDictionaryBuilder;
import io.kyligence.kap.engine.spark.storage.NParquetStorage;
import io.kyligence.kap.job.execution.NExecutableManager;

@SuppressWarnings("serial")
public class NSparkCubingJobTest extends NLocalSparkWithCSVDataTest {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    @After
    public void after() throws Exception {
        DefaultScheduler.destroyInstance();
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
        KylinConfig config = getTestConfig();
        System.out.println(getTestConfig().getMetadataUrl());
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        NDataflow df = dsMgr.getDataflow("ncube_basic");

        NDataSegment seg = df.copy().getLastSegment();
        seg.setDictionaries(null);
        Assert.assertEquals(0, seg.getDictionaries().size());
        NCubeJoinedFlatTableDesc flatTable = new NCubeJoinedFlatTableDesc(df.getCubePlan(), seg.getSegRange());
        Dataset<Row> ds = NJoinedFlatTable.generateDataset(flatTable, ss);

        NDictionaryBuilder dictionaryBuilder = new NDictionaryBuilder(seg, ds);
        seg = dictionaryBuilder.buildDictionary();
        Assert.assertEquals(4, seg.getDictionaries().size());
    }

    @Test
    public void testBuildJob() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        config.setProperty("kylin.job.scheduler.provider.110",
                "io.kyligence.kap.job.impl.threadpool.NDefaultScheduler");
        config.setProperty("kylin.job.scheduler.default", "110");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        NExecutableManager execMgr = NExecutableManager.getInstance(config, "default");

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment oneSeg = dsMgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();
        List<NCuboidLayout> round1 = new ArrayList<>();
        round1.add(layouts.get(4));
        round1.add(layouts.get(5));

        NSpanningTree nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round1, df.getName());
        for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
            NCuboidLayout layout = NCuboidLayoutChooser.selectCuboidLayout(oneSeg,
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
        round2.add(layouts.get(0));
        round2.add(layouts.get(1));
        round2.add(layouts.get(2));
        round2.add(layouts.get(3));

        //update seg
        oneSeg = dsMgr.getDataflow("ncube_basic").getSegment(oneSeg.getId());
        nSpanningTree = NSpanningTreeFactory.fromCuboidLayouts(round2, df.getName());
        for (NCuboidDesc rootCuboid : nSpanningTree.getRootCuboidDescs()) {
            NCuboidLayout layout = NCuboidLayoutChooser.selectCuboidLayout(oneSeg,
                    rootCuboid.getEffectiveDimCols().keySet(), nSpanningTree.retrieveAllMeasures(rootCuboid));
            Assert.assertTrue(layout != null);
        }

        job = NSparkCubingJob.create(Sets.newHashSet(oneSeg), Sets.newLinkedHashSet(round2), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        validate(1);
    }

    @Ignore
    @Test
    public void testMergeJob() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        ExecutableManager execMgr = ExecutableManager.getInstance(config);

        NDataflow df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        // ready dataflow, segment, cuboid layout

        long start = dateToLong("2011/01/01");
        long end = dateToLong("2013/01/01");
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment firstSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(start, end));
        df = dsMgr.getDataflow("ncube_basic");
        List<NCuboidLayout> layouts = df.getCubePlan().getAllCuboidLayouts();

        // Round1. Build first segment
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(firstSeg), Sets.newLinkedHashSet(layouts),
                "ADMIN");
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
         * Round2. Build second segment
         */

        //update seg
        start = dateToLong("2013/01/01");
        end = dateToLong("2014/01/02");
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment secondSeg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(start, end));

        job = NSparkCubingJob.create(Sets.newHashSet(secondSeg), Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(job);

        // wait job done
        status = wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        /**
         * Round3. Merge two segments
         */
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment mergeSeg = dsMgr.mergeSegments(df,
                new SegmentRange.TimePartitionedSegmentRange(dateToLong("2011/01/01"), dateToLong("2015/01/02")), false);

        NSparkMergingJob mergeJob = NSparkMergingJob.merge(mergeSeg, Sets.newLinkedHashSet(layouts), "ADMIN");
        execMgr.addJob(mergeJob);

        // wait job done
        status = wait(mergeJob);
        Assert.assertEquals(ExecutableState.SUCCEED, status);

        validate(2);
    }

    private void validate(int segmentId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, "default");
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment seg = df.getSegment(segmentId);
        NDataSegDetails segCuboids = seg.getSegDetails();
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(segCuboids, 1);
        NCuboidLayout layout = dataCuboid.getCuboidLayout();

        // check row count in NDataSegDetails
        Assert.assertEquals(402, seg.getCuboid(1).getRows());
        Assert.assertEquals(402, seg.getCuboid(2).getRows());
        Assert.assertEquals(402, seg.getCuboid(1001).getRows());
        Assert.assertEquals(402, seg.getCuboid(1002).getRows());

        NParquetStorage storage = new NParquetStorage();
        Dataset<Row> ret = storage.getCuboidData(dataCuboid, ss);
        IDimensionEncodingMap dimEncoding = new NCubeDimEncMap(seg);

        for (TblColRef colRef : seg.getCubePlan().getEffectiveDimCols().values()) {
            dimEncoding.get(colRef);
        }

        RowKeyColumnIO colIO = new RowKeyColumnIO(dimEncoding);
        MeasureCodec measureCodec = new MeasureCodec(layout.getOrderedMeasures().values().toArray(new MeasureDesc[0]));
        int ms = layout.getOrderedMeasures().size();

        List<String[]> resultFromLayout = new ArrayList<>();
        for (Row row : ret.collectAsList()) {
            List<String> l = new ArrayList<>();
            int i = 0;
            for (TblColRef rowkey : layout.getOrderedDimensions().values()) {
                byte[] bytes = (byte[]) row.get(i);
                String value = colIO.readColumnString(rowkey, bytes, 0, bytes.length);
                l.add(value);
                i++;
            }
            for (int j = 0; j < ms; j++, i++) {
                ByteBuffer buffer = ByteBuffer.wrap((byte[]) row.get(i));
                String m = measureCodec.decode(j, buffer).toString();
                l.add(m);
            }
            resultFromLayout.add(l.toArray(new String[0]));
        }
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
        Assert.assertEquals(1, actualCount);
        Assert.assertEquals(1, actualMax);
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

    private long dateToLong(String dateString) {
        Date date = null;
        String format = "yyyy/MM/dd";
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            System.out.println("Failed to parse string to date.");
        }
        if (date == null)
            return 0L;
        return date.getTime();
    }
}
