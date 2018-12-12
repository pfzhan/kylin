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

package io.kyligence.kap.cube.model;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.var;

public class NDataflowManagerTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCached() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");

        Assert.assertTrue(df.isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().getSegDetails().isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().getCuboid(1).isCachedAndShared());

        df = df.copy();
        Assert.assertFalse(df.isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().getSegDetails().isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().getCuboid(1).isCachedAndShared());
    }

    @Test
    public void testImmutableCachedObj() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");

        try {
            df.setStatus(RealizationStatusEnum.OFFLINE);
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected
        }

        try {
            df.getSegments().get(0).setCreateTimeUTC(0);
            Assert.fail();
        } catch (IllegalStateException ex) {
            // expected
        }

        df.copy().setStatus(RealizationStatusEnum.OFFLINE);
    }

    @Test
    public void testCRUD() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NCubePlanManager cubeMgr = NCubePlanManager.getInstance(testConfig, projectDefault);
        NProjectManager projMgr = NProjectManager.getInstance(testConfig);

        final String name = UUID.randomUUID().toString();
        final String owner = "test_owner";
        final ProjectInstance proj = projMgr.getProject(projectDefault);
        final NCubePlan cube = cubeMgr.getCubePlan("ncube_basic");

        // create
        int cntBeforeCreate = mgr.listAllDataflows().size();
        NDataflow df = mgr.createDataflow(name, cube, owner);
        Assert.assertNotNull(df);

        // list
        List<NDataflow> cubes = mgr.listAllDataflows();
        Assert.assertEquals(cntBeforeCreate + 1, cubes.size());

        // get
        df = mgr.getDataflow(name);
        Assert.assertNotNull(df);

        // update
        NDataflowUpdate update = new NDataflowUpdate(name);
        update.setDescription("new_description");
        df = mgr.updateDataflow(update);
        Assert.assertEquals("new_description", df.getDescription());

        // update cached objects causes exception

        // remove
        mgr.dropDataflow(name);
        Assert.assertEquals(cntBeforeCreate, mgr.listAllDataflows().size());
        Assert.assertNull(mgr.getDataflow(name));
    }

    @Test
    public void testUpdateSegment() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");

        NDataSegment newSeg = mgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(newSeg);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());
    }

    @Test
    public void testMergeSegmentsSuccess() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());

        NDataSegment seg1 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg1);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());

        NDataSegment seg2 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 2L));
        seg2.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg2);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataSegment mergedSeg = mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 2L), true);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(3, df.getSegments().size());

        Assert.assertTrue(mergedSeg.getSegRange().contains(seg1.getSegRange()));
        Assert.assertTrue(mergedSeg.getSegRange().contains(seg2.getSegRange()));
    }

    @Test
    public void testGetDataflow() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        Assert.assertNotNull(mgr.getDataflowByUuid("0aeb985d-aec5-488a-a9b7-a5a564004433"));
        Assert.assertNotNull(mgr.getDataflowsByCubePlan("ncube_basic"));
    }

    @Test
    public void testMergeSegmentsFail() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");
        //ncube_basic
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());

        NDataSegment seg1 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg1);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());

        NDataSegment seg2 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 2L));
        seg2.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg2);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataSegment seg3 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(5L, 6L));
        seg3.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg3);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(3, df.getSegments().size());

        try {
            mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 2L), false);
            fail("No exception thrown.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(e.getMessage().contains("Empty cube segment found"));
        }

        try {
            mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 6L), false);
            fail("No exception thrown.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
            Assert.assertTrue(e.getMessage().contains("Merging segments must not have gaps between"));
        }

        // Set seg1's cuboid-0's status to NEW
        NDataCuboid dataCuboid = NDataCuboid.newDataCuboid(seg1.getDataflow(), seg1.getId(), 0);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg1);
        update.setToAddOrUpdateCuboids(dataCuboid);
        mgr.updateDataflow(update);

        try {
            mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L), true);
            fail("No exception thrown.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(e.getMessage().contains("must contain at least 2 segments, but there is 1"));
        }

        try {
            mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 2L), true);
            fail("No exception thrown.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertTrue(e.getMessage().contains("has different layout status"));
        }
    }

    @Test
    public void testUpdateCuboid() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");

        // test cuboid remove
        Assert.assertEquals(8, df.getSegments().getFirstSegment().getCuboidsMap().size());
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveCuboids(df.getSegments().getFirstSegment().getCuboid(1001L));
        mgr.updateDataflow(update);

        // verify after remove
        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(7, df.getSegments().getFirstSegment().getCuboidsMap().size());

        // test cuboid add
        NDataSegment seg = df.getSegments().getFirstSegment();
        update = new NDataflowUpdate(df.getName());
        update.setToAddOrUpdateCuboids(//
                NDataCuboid.newDataCuboid(df, seg.getId(), 1001L), // to add
                NDataCuboid.newDataCuboid(df, seg.getId(), 1002L) // existing, will update with warning
        );
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(8, df.getSegments().getFirstSegment().getCuboidsMap().size());
    }

    @Test
    public void testConcurrentMergeAndMerge() throws Exception {
        System.setProperty("kylin.cube.max-building-segments", "10");
        NDataflowManager mgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        String dfName = "ncube_basic";
        //get a empty dataflow
        NDataflow dataflow = mgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getName());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        NDataflow newDataflow = mgr.getDataflow(dfName);
        Assert.assertEquals(0, newDataflow.getSegments().size());

        //append tow segements to empty dataflow
        long start1 = SegmentRange.dateToLong("2010-01-01");
        long end1 = SegmentRange.dateToLong("2013-01-01");

        NDataSegment seg1 = mgr.appendSegment(newDataflow, new SegmentRange.TimePartitionedSegmentRange(start1, end1));
        seg1.setStatus(SegmentStatusEnum.READY);
        NDataflowUpdate update1 = new NDataflowUpdate(newDataflow.getName());
        update1.setToUpdateSegs(seg1);
        mgr.updateDataflow(update1);

        NDataflow updatedDataflow = mgr.getDataflow(dfName);

        long start2 = SegmentRange.dateToLong("2013-01-01");
        long end2 = SegmentRange.dateToLong("2015-01-01");
        NDataSegment seg2 = mgr.appendSegment(updatedDataflow,
                new SegmentRange.TimePartitionedSegmentRange(start2, end2));
        seg2.setStatus(SegmentStatusEnum.READY);
        NDataflowUpdate update2 = new NDataflowUpdate(newDataflow.getName());
        update2.setToUpdateSegs(seg2);
        mgr.updateDataflow(update2);

        NDataflow encodingDataflow = mgr.getDataflow(dfName);
        Assert.assertEquals(encodingDataflow.getSegments().size(), 2);

        //merge two segements
        mgr.mergeSegments(encodingDataflow, new SegmentRange.TimePartitionedSegmentRange(start1, end2), true);

        Assert.assertEquals(mgr.getDataflow(dfName).getSegments().size(), 3);
        Assert.assertNotNull(mgr.getDataflow(dfName).getSegments().get(2));
    }

    @Test
    @Ignore
    public void testConcurrency() throws IOException, InterruptedException {
        // this test case merge from PR <https://github.com/Kyligence/KAP/pull/4744>
        final KylinConfig testConfig = getTestConfig();
        final NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NCubePlanManager cubePlanMgr = NCubePlanManager.getInstance(testConfig, projectDefault);
        NProjectManager projMgr = NProjectManager.getInstance(testConfig);

        final String[] dataFlowNames = { "df1", "df2", "df3", "df4" };
        final String owner = "test_owner";
        final int n = dataFlowNames.length;
        final int updatesPerCube = 100;
        testConfig.setProperty("kylin.cube.max-building-segments", String.valueOf(updatesPerCube));

        final ProjectInstance proj = projMgr.getProject(projectDefault);
        final NCubePlan cube = cubePlanMgr.getCubePlan("ncube_basic");
        final List<NDataflow> dataflows = new ArrayList<>();

        // create
        for (String dataFlowName : dataFlowNames) {
            dataflows.add(mgr.createDataflow(dataFlowName, cube, owner));
        }

        final AtomicInteger runningFlag = new AtomicInteger();
        final Vector<Exception> exceptions = new Vector<>();

        // 1 thread, keeps reloading dataflow
        Thread reloadThread = new Thread() {
            @Override
            public void run() {
                try {
                    Random rand = new Random();
                    while (runningFlag.get() == 0) {
                        String name = dataFlowNames[rand.nextInt(n)];
//                        NDataflowManager.getInstance(testConfig, projectDefault).reloadDataFlow(name);
                        Thread.sleep(1);
                    }
                } catch (Exception ex) {
                    exceptions.add(ex);
                }
            }
        };
        reloadThread.start();

        // 4 threads, keeps updating cubes
        Thread[] updateThreads = new Thread[n];
        for (int i = 0; i < n; i++) {
            // each thread takes care of one dataFlow
            // for now, the design refuses concurrent updates to one dataFlow
            final String dataFlowName = dataFlowNames[i];
            updateThreads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random rand = new Random();
                        for (int i = 0; i < updatesPerCube; i++) {
                            NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
                            NDataflow dataflow = mgr.getDataflow(dataFlowName);
                            mgr.appendSegment(dataflow,
                                    new SegmentRange.TimePartitionedSegmentRange((long) i, (long) i + 1));
                            Thread.sleep(rand.nextInt(1));
                        }
                    } catch (Exception ex) {
                        exceptions.add(ex);
                    }
                }
            };
            updateThreads[i].start();
        }

        // wait things done
        for (int i = 0; i < n; i++) {
            updateThreads[i].join();
        }
        runningFlag.incrementAndGet();
        reloadThread.join();

        // check result and error
        if (exceptions.isEmpty() == false) {
            fail();
        }
        for (int i = 0; i < n; i++) {
            NDataflow dataflow = mgr.getDataflow(dataFlowNames[i]);
            Assert.assertEquals(updatesPerCube, dataflow.getSegments().size());
        }

    }

    @Test
    public void testRefreshSegment() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("ncube_basic");
        NDataSegment segment = df.getSegments(SegmentStatusEnum.READY).get(0);

        NDataSegment newSegment = mgr.refreshSegment(df, segment.getSegRange());

        Assert.assertEquals(newSegment.getSegRange().equals(segment.getSegRange()), true);
        Assert.assertEquals(newSegment.getStatus().equals(SegmentStatusEnum.NEW), true);
    }

    @Test
    public void testGetDataflow2() throws IOException {
        KylinConfig testConfig = getTestConfig();
        String cubePlanName = "ncube_basic";
        String uuid = "0aeb985d-aec5-488a-a9b7-a5a564008891";
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);

        NDataflow df = mgr.getDataflowByUuid(uuid);
        Assert.assertTrue(uuid.equals(df.getUuid()));

        List<NDataflow> dataflows = mgr.getDataflowsByCubePlan(cubePlanName);
        for (NDataflow dataflow : dataflows) {
            Assert.assertTrue(dataflow.getCubePlanName().equals(cubePlanName));
        }

    }

    @Test
    public void testCalculateHoles() throws IOException {
        String dataFlowName = "dataFlowWithHole";
        KylinConfig testConfig = getTestConfig();
        val modelMgr = NDataModelManager.getInstance(testConfig, projectDefault);
        modelMgr.updateDataModel("nmodel_basic", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NCubePlanManager cubePlanMgr = NCubePlanManager.getInstance(testConfig, projectDefault);
        final NCubePlan cubePlan = cubePlanMgr.getCubePlan("ncube_basic");

        NDataflow df = mgr.createDataflow(dataFlowName, cubePlan, "test_owner");

        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(10L, 100L));
        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1000L, 10000L));

        List<NDataSegment> dataSegments = mgr.calculateHoles(dataFlowName);

        Assert.assertEquals(2, dataSegments.size());

    }

    @Test
    public void testRemoveLayouts() throws IOException {
        val testConfig = getTestConfig();
        val mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        var dataflow = mgr.getDataflow("ncube_basic");
        val originSize = dataflow.getLastSegment().getSegDetails().getCuboids().size();
        dataflow = mgr.removeLayouts(dataflow, Lists.newArrayList(1000001L, 1L));
        Assert.assertEquals(originSize - 2, dataflow.getLastSegment().getSegDetails().getCuboids().size());
        dataflow = mgr.removeLayouts(dataflow, Lists.newArrayList(100000000L));
        Assert.assertEquals(originSize - 2, dataflow.getLastSegment().getSegDetails().getCuboids().size());
    }
}
