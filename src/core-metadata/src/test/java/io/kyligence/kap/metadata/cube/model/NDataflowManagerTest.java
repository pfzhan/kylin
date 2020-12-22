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

package io.kyligence.kap.metadata.cube.model;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.CubeTestUtils;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.var;

@RunWith(TimeZoneTestRunner.class)
public class NDataflowManagerTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testInvalidMerge() {
        thrown.expectMessage("Range TimePartitionedSegmentRange[" + SegmentRange.dateToLong("2010-01-01") + ","
                + SegmentRange.dateToLong("2013-01-01") + ") must contain at least 2 segments, but there is 0");

        NDataflowManager dfMgr = NDataflowManager.getInstance(getTestConfig(), "default");
        NDataflow df = dfMgr.getDataflowByModelAlias("nmodel_basic");
        dfMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")), false);
    }

    @Test
    public void testCached() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        Assert.assertTrue(df.isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().getSegDetails().isCachedAndShared());
        Assert.assertTrue(df.getSegments().getFirstSegment().getLayout(1).isCachedAndShared());

        df = df.copy();
        Assert.assertFalse(df.isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().getSegDetails().isCachedAndShared());
        Assert.assertFalse(df.getSegments().getFirstSegment().getLayout(1).isCachedAndShared());
    }

    @Test
    public void testImmutableCachedObj() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

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
        NIndexPlanManager indePlanManager = NIndexPlanManager.getInstance(testConfig, projectDefault);
        NProjectManager projMgr = NProjectManager.getInstance(testConfig);

        final String name = UUID.randomUUID().toString();
        final String owner = "test_owner";
        final ProjectInstance proj = projMgr.getProject(projectDefault);
        final IndexPlan cube = indePlanManager.getIndexPlanByModelAlias("nmodel_basic");

        val copy = cube.copy();
        copy.setUuid(name);
        CubeTestUtils.createTmpModelAndCube(getTestConfig(), copy);
        // create
        int cntBeforeCreate = mgr.listAllDataflows().size();
        NDataflow df = mgr.createDataflow(copy, owner);
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
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        NDataSegment newSeg = mgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(newSeg);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());
    }

    @Test
    public void testUpdateSegmentByDataflowSetSegments() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        NDataSegDetailsManager dsdMgr = NDataSegDetailsManager.getInstance(testConfig, projectDefault);

        mgr.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        Segments<NDataSegment> segsSet = df.getSegments();

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());

        mgr.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setSegments(new Segments<>());
        });

        for (NDataSegment segment : segsSet) {
            Assert.assertNull(dsdMgr.getForSegment(segment));
        }
    }

    @Test
    public void testMergeSegmentsSuccess() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(0, df.getSegments().size());

        NDataSegment seg1 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg1);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());

        NDataSegment seg2 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 2L));
        seg2.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg2);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataSegment mergedSeg = mgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(0L, 2L), true);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(3, df.getSegments().size());

        Assert.assertTrue(mergedSeg.getSegRange().contains(seg1.getSegRange()));
        Assert.assertTrue(mergedSeg.getSegRange().contains(seg2.getSegRange()));
    }

    @Test
    public void testGetDataflow() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        Assert.assertNotNull(mgr.getDataflowByModelAlias("nmodel_basic"));
    }

    @Test
    public void testMergeSegmentsFail() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");
        //nmodel_basic
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(0, df.getSegments().size());

        NDataSegment seg1 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg1);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(1, df.getSegments().size());

        NDataSegment seg2 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 2L));
        seg2.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg2);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataSegment seg3 = mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(5L, 6L));
        seg3.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg3);
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
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
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("are discontinuous."));
        }

        // Set seg1's cuboid-0's status to NEW
        NDataLayout dataCuboid = NDataLayout.newDataLayout(seg1.getDataflow(), seg1.getId(),
                df.getIndexPlan().getAllLayouts().get(0).getId());
        update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(seg1);
        update.setToAddOrUpdateLayouts(dataCuboid);
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
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(
                    e.getMessage().contains("The indexes included in the selected segments are not fully identical"));
        }
    }

    @Test
    public void testUpdateCuboid() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");

        // test cuboid remove
        Assert.assertEquals(8, df.getSegments().getFirstSegment().getLayoutsMap().size());
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveLayouts(df.getSegments().getFirstSegment().getLayout(10001L));
        mgr.updateDataflow(update);

        // verify after remove
        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(7, df.getSegments().getFirstSegment().getLayoutsMap().size());

        // test cuboid add
        NDataSegment seg = df.getSegments().getFirstSegment();
        update = new NDataflowUpdate(df.getUuid());
        update.setToAddOrUpdateLayouts(//
                NDataLayout.newDataLayout(df, seg.getId(), 10001L), // to add
                NDataLayout.newDataLayout(df, seg.getId(), 10002L) // existing, will update with warning
        );
        mgr.updateDataflow(update);

        df = mgr.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(8, df.getSegments().getFirstSegment().getLayoutsMap().size());
    }

    @Test
    public void testConcurrentMergeAndMerge() throws Exception {
        NDataflowManager mgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        String dfName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        //get a empty dataflow
        NDataflow dataflow = mgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(dataflow.getUuid());
        update.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);
        NDataflow newDataflow = mgr.getDataflow(dfName);
        Assert.assertEquals(0, newDataflow.getSegments().size());

        //append tow segements to empty dataflow
        long start1 = SegmentRange.dateToLong("2010-01-01");
        long end1 = SegmentRange.dateToLong("2013-01-01");

        NDataSegment seg1 = mgr.appendSegment(newDataflow, new SegmentRange.TimePartitionedSegmentRange(start1, end1));
        seg1.setStatus(SegmentStatusEnum.READY);
        NDataflowUpdate update1 = new NDataflowUpdate(newDataflow.getUuid());
        update1.setToUpdateSegs(seg1);
        mgr.updateDataflow(update1);

        NDataflow updatedDataflow = mgr.getDataflow(dfName);

        long start2 = SegmentRange.dateToLong("2013-01-01");
        long end2 = SegmentRange.dateToLong("2015-01-01");
        NDataSegment seg2 = mgr.appendSegment(updatedDataflow,
                new SegmentRange.TimePartitionedSegmentRange(start2, end2));
        seg2.setStatus(SegmentStatusEnum.READY);
        NDataflowUpdate update2 = new NDataflowUpdate(newDataflow.getUuid());
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
        NIndexPlanManager indePlanMgr = NIndexPlanManager.getInstance(testConfig, projectDefault);
        NProjectManager projMgr = NProjectManager.getInstance(testConfig);

        final String[] dataflowIds = { "df1", "df2", "df3", "df4" };
        final String owner = "test_owner";
        final int n = dataflowIds.length;
        final int updatesPerCube = 100;

        final ProjectInstance proj = projMgr.getProject(projectDefault);
        final IndexPlan cube = indePlanMgr.getIndexPlanByModelAlias("nmodel_basic");
        final List<NDataflow> dataflows = new ArrayList<>();

        // create
        for (String dataFlowId : dataflowIds) {
            dataflows.add(mgr.createDataflow(cube, owner));
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
                        String name = dataflowIds[rand.nextInt(n)];
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
            final String dataflowId = dataflowIds[i];
            updateThreads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random rand = new Random();
                        for (int i = 0; i < updatesPerCube; i++) {
                            NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
                            NDataflow dataflow = mgr.getDataflow(dataflowId);
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
        if (!exceptions.isEmpty()) {
            fail();
        }
        for (String dataflowId : dataflowIds) {
            NDataflow dataflow = mgr.getDataflow(dataflowId);
            Assert.assertEquals(updatesPerCube, dataflow.getSegments().size());
        }

    }

    @Test
    public void testRefreshSegment() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflowByModelAlias("nmodel_basic");
        NDataSegment segment = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING).get(0);

        NDataSegment newSegment = mgr.refreshSegment(df, segment.getSegRange());

        Assert.assertTrue(newSegment.getSegRange().equals(segment.getSegRange()));
        Assert.assertEquals(SegmentStatusEnum.NEW, newSegment.getStatus());
    }

    @Test
    public void testRefreshSegmentMultiPartition() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NDataflow df = mgr.getDataflow("b780e4e4-69af-449e-b09f-05c90dfa04b6");
        NDataSegment segment = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING).get(0);
        NDataSegment newSegment = mgr.refreshSegment(df, segment.getSegRange());
        Assert.assertEquals(segment.getSegRange(), newSegment.getSegRange());
        Assert.assertEquals(SegmentStatusEnum.NEW, newSegment.getStatus());
        Assert.assertEquals(0, newSegment.getMultiPartitions().get(0).getSourceCount());
        Assert.assertEquals(0, newSegment.getMultiPartitions().get(0).getStorageSize());
    }

    @Test
    public void testGetDataflow2() throws IOException {
        KylinConfig testConfig = getTestConfig();
        String uuid = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);

        NDataflow df = mgr.getDataflow(uuid);
        Assert.assertTrue(uuid.equals(df.getUuid()));
    }

    @Test
    public void testCalculateHoles() throws IOException {
        KylinConfig testConfig = getTestConfig();
        val modelMgr = NDataModelManager.getInstance(testConfig, projectDefault);
        modelMgr.updateDataModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        NIndexPlanManager indePlanMgr = NIndexPlanManager.getInstance(testConfig, projectDefault);
        final IndexPlan indexPlan = indePlanMgr.getIndexPlanByModelAlias("nmodel_basic");

        val copy = indexPlan.copy();
        copy.setUuid(UUID.randomUUID().toString());
        CubeTestUtils.createTmpModelAndCube(testConfig, copy);

        NDataflow df = mgr.createDataflow(copy, "test_owner");

        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1L));
        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(10L, 100L));
        mgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(1000L, 10000L));

        List<NDataSegment> dataSegments = mgr.calculateHoles(copy.getUuid());
        Assert.assertEquals(2, dataSegments.size());

        df = mgr.getDataflow(copy.getId());
        Assert.assertEquals(2, mgr.calculateHoles(copy.getUuid(), df.getSegments()).size());

        val segments = Lists.newArrayList(df.getSegments());
        segments.add(mgr.newSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 9L)));
        Assert.assertEquals(2, mgr.calculateHoles(copy.getUuid(), segments).size());
        segments.clear();
        segments.addAll(df.getSegments());
        segments.add(mgr.newSegment(df, new SegmentRange.TimePartitionedSegmentRange(1L, 10L)));
        Assert.assertEquals(1, mgr.calculateHoles(copy.getUuid(), segments).size());
    }

    @Test
    public void testRemoveLayouts() throws IOException {
        val testConfig = getTestConfig();
        val mgr = NDataflowManager.getInstance(testConfig, projectDefault);
        var dataflow = mgr.getDataflowByModelAlias("nmodel_basic");
        val originSize = dataflow.getLastSegment().getSegDetails().getLayouts().size();
        dataflow = mgr.removeLayouts(dataflow, Lists.newArrayList(1000001L, 1L));
        Assert.assertEquals(originSize - 2, dataflow.getLastSegment().getSegDetails().getLayouts().size());
        dataflow = mgr.removeLayouts(dataflow, Lists.newArrayList(100000000L));
        Assert.assertEquals(originSize - 2, dataflow.getLastSegment().getSegDetails().getLayouts().size());
    }

    @Test
    public void testCuboidsNotInCube() {
        val indePlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val cube = indePlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(1L), true, true);
        });

        val dfMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val df = dfMgr.getDataflow(cube.getUuid());
        for (NDataSegment segment : df.getSegments()) {
            Assert.assertFalse(segment.getLayoutsMap().containsKey(1L));
        }
    }

    @Test
    public void testGetModels() {
        val mgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        List<NDataModel> models = mgr.listUnderliningDataModels();
        Assert.assertEquals(7, models.size());

        val mgrSsb = NDataflowManager.getInstance(getTestConfig(), "ssb");
        List<NDataModel> models2 = mgrSsb.listUnderliningDataModels();
        Assert.assertEquals(0, models2.size());
    }

    @Test
    public void testBrokenDataFlow_WithBrokenModel() {
        val project = "broken_test";
        val modelId = "3f8941de-d01c-42b8-91b5-44646390864b";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val dataflow = dfManager.getDataflow(modelId);
        Assert.assertEquals(false, dataflow.isBroken());
        Assert.assertEquals(true, dataflow.checkBrokenWithRelatedInfo());

    }

    @Test
    public void testBrokenDataFlow_WithBrokenIndexPlan() {
        val project = "broken_test";
        val dataflowName = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val dataflow = dfManager.getDataflow(dataflowName);
        Assert.assertEquals(false, dataflow.isBroken());
        Assert.assertEquals(true, dataflow.checkBrokenWithRelatedInfo());

    }

    @Test
    public void testBrokenDataFlow() {
        val project = "broken_test";
        val dataflowId = "f1bb4bbd-a638-442b-a276-e301fde0d7f6";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val dataflow = dfManager.getDataflow(dataflowId);
        Assert.assertEquals(true, dataflow.isBroken());

        Assert.assertEquals(dataflowId, dataflow.getId());
        thrown.expect(RuntimeException.class);
        thrown.expectMessage("call on Broken Entity's getAllColumns method");
        dataflow.getAllColumns();
    }

    @Test
    public void testGetDataflowByModelAlias_WithBrokenCubePlan() {
        val project = "broken_test";
        val dataflowId = "039eef32-9691-4c88-93ba-d65c58a1ab7a";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(dataflowId);

        Assert.assertEquals(false, df.isBroken());
        Assert.assertEquals(true, df.checkBrokenWithRelatedInfo());

        val df2 = dfManager.getDataflowByModelAlias("AUTO_MODEL_TEST_ACCOUNT_1");
        Assert.assertEquals(df2, df);
    }

    @Test
    public void testGetDataflowByModelAlias_WithBrokenModel() {
        val project = "broken_test";
        val dataflowId = "3f8941de-d01c-42b8-91b5-44646390864b";
        val dfManager = NDataflowManager.getInstance(getTestConfig(), project);
        val df = dfManager.getDataflow(dataflowId);

        Assert.assertEquals(false, df.isBroken());
        Assert.assertEquals(true, df.checkBrokenWithRelatedInfo());

        val df2 = dfManager.getDataflowByModelAlias("AUTO_MODEL_TEST_COUNTRY_1");
        Assert.assertNull(df2);
    }

    @Test
    public void testCacheReload_TableChanged() {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val df1 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), projectDefault);

        val tb = tableManager.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        val copy = tableManager.copyForWrite(tb);
        copy.setTop(true);
        tableManager.updateTableDesc(copy);

        val df2 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");

        Assert.assertNotSame(df1, df2);
    }

    @Test
    public void testCacheReload_TableRemoved() {
        val dfManager = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val df1 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), projectDefault);

        tableManager.removeSourceTable("DEFAULT.TEST_KYLIN_FACT");

        val df2 = dfManager.getDataflowByModelAlias("nmodel_basic_inner");

        Assert.assertNotSame(df1, df2);
    }

    @Test
    public void testJoinedFlatTableDescDiff() {
        KylinConfig kylinConfig = getTestConfig();
        NDataflowManager dfManager = NDataflowManager.getInstance(kylinConfig, projectDefault);
        List<NDataflow> dataflowList = dfManager.listAllDataflows();
        NDataSegment dataSegment = dataflowList.get(4).getLastSegment();
        Assert.assertEquals("11124840-b3e3-43db-bcab-2b78da666d00", dataSegment.getId());
        NCubeJoinedFlatTableDesc flatTableDesc1 = new NCubeJoinedFlatTableDesc(dataSegment);
        Assert.assertEquals(4, flatTableDesc1.getUsedColumns().size());
        Assert.assertEquals(4, flatTableDesc1.getAllColumns().size());

        kylinConfig.setProperty("kylin.engine.persist-flattable-enabled", "true");
        NCubeJoinedFlatTableDesc flatTableDesc2 = new NCubeJoinedFlatTableDesc(dataSegment);
        // when persist flat table is enabled, flat table will include all columns in model
        Assert.assertEquals(26, flatTableDesc2.getAllColumns().size());
        // used columns only include all columns used in current index plan
        Assert.assertEquals(4, flatTableDesc2.getUsedColumns().size());
    }

}
