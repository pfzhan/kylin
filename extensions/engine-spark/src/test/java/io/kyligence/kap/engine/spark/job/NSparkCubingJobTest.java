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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import io.kyligence.kap.engine.spark.NJoinedFlatTable;
import io.kyligence.kap.engine.spark.NLocalSparkWithCSVDataTest;
import io.kyligence.kap.engine.spark.builder.NDictionaryBuilder;
import io.kyligence.kap.engine.spark.storage.NParquetStorage;

@SuppressWarnings("serial")
public class NSparkCubingJobTest extends NLocalSparkWithCSVDataTest {

    @Before
    public void setup() throws Exception {
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        createTestMetadata();
        DefaultScheduler scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()), new MockJobLock());
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
    public void testBuildDictionary() throws Exception {
        KylinConfig config = getTestConfig();
        System.out.println(getTestConfig().getMetadataUrl());
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");

        NDataflowManager dsMgr = NDataflowManager.getInstance(config);
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
    public void test() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.storage.hbase.util.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        NDataflowManager dsMgr = NDataflowManager.getInstance(config);
        ExecutableManager execMgr = ExecutableManager.getInstance(config);

        Assert.assertTrue(config.getHdfsWorkingDirectory().startsWith("file:"));

        // ready dataflow, segment, cuboid layout
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment oneSeg = dsMgr.appendSegment(df);
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

        validate();
    }

    private void validate() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config);
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment seg = df.getSegment(1);
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
}
