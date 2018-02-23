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

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NDataflowManagerTest extends NLocalFileMetadataTestCase {

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
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NDataflow df = mgr.getDataflow("ncube_basic");

        Assert.assertTrue(df.isCachedAndShared());
        Assert.assertTrue(df.getSegment(0).isCachedAndShared());
        Assert.assertTrue(df.getSegment(0).getSegDetails().isCachedAndShared());
        Assert.assertTrue(df.getSegment(0).getCuboid(1).isCachedAndShared());

        df = df.copy();
        Assert.assertFalse(df.isCachedAndShared());
        Assert.assertFalse(df.getSegment(0).isCachedAndShared());
        Assert.assertFalse(df.getSegment(0).getSegDetails().isCachedAndShared());
        Assert.assertFalse(df.getSegment(0).getCuboid(1).isCachedAndShared());
    }

    @Test
    public void testImmutableCachedObj() {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NDataflow df = mgr.getDataflow("ncube_basic");

        try {
            df.setStatus(RealizationStatusEnum.DISABLED);
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

        df.copy().setStatus(RealizationStatusEnum.DISABLED);
    }

    @Test
    public void testCRUD() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NCubePlanManager cubeMgr = NCubePlanManager.getInstance(testConfig);
        ProjectManager projMgr = ProjectManager.getInstance(testConfig);

        final String name = UUID.randomUUID().toString();
        final String owner = "test_owner";
        final ProjectInstance proj = projMgr.getProject("default");
        final NCubePlan cube = cubeMgr.getCubePlan("ncube_basic");

        // create
        int cntBeforeCreate = mgr.listAllDataflows().size();
        NDataflow df = mgr.createDataflow(name, proj.getName(), cube, owner);
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
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NDataflow df = mgr.getDataflow("ncube_basic");

        NDataSegment newSeg = mgr.appendSegment(df);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(newSeg);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());
    }

    @Test
    public void testMergeSegments() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NDataflow df = mgr.getDataflow("ncube_basic");

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(0, df.getSegments().size());

        NDataSegment seg1 = mgr.appendSegment(df, new SegmentRange(0, 1));
        seg1.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg1);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(1, df.getSegments().size());

        NDataSegment seg2 =mgr.appendSegment(df, new SegmentRange(1, 2));
        seg2.setStatus(SegmentStatusEnum.READY);
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(seg2);
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(2, df.getSegments().size());

        NDataSegment mergedSeg = mgr.mergeSegments(df, new SegmentRange(0, 2), true);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(3, df.getSegments().size());

        Assert.assertTrue(mergedSeg.getSegRange().contains(seg1.getSegRange()));
        Assert.assertTrue(mergedSeg.getSegRange().contains(seg2.getSegRange()));
    }

    @Test
    public void testUpdateCuboid() throws IOException {
        KylinConfig testConfig = getTestConfig();
        NDataflowManager mgr = NDataflowManager.getInstance(testConfig);
        NDataflow df = mgr.getDataflow("ncube_basic");

        // test cuboid remove
        Assert.assertEquals(4, df.getSegment(0).getCuboidsMap().size());
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveCuboids(df.getSegment(0).getCuboid(1001L));
        mgr.updateDataflow(update);

        // verify after remove
        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(3, df.getSegment(0).getCuboidsMap().size());

        // test cuboid add
        NDataSegment seg = df.getSegment(0);
        update = new NDataflowUpdate(df.getName());
        update.setToAddOrUpdateCuboids(//
                NDataCuboid.newDataCuboid(df, seg.getId(), 1001L), // to add
                NDataCuboid.newDataCuboid(df, seg.getId(), 1002L) // existing, will update with warning
        );
        mgr.updateDataflow(update);

        df = mgr.getDataflow("ncube_basic");
        Assert.assertEquals(4, df.getSegment(0).getCuboidsMap().size());
    }
}
