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

package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class RawTableManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        final String cubeName = "ci_left_join_cube";

        // init
        RawTableManager mgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance existing = mgr.getRawTableInstance(cubeName);

        Assert.assertNotNull(existing);

        RawTableDesc desc = existing.getRawTableDesc();
        Assert.assertNotNull(desc);

        // update
        RawTableInstance toUpdate = mgr.getRawTableInstance(cubeName);
        toUpdate.setCreateTimeUTC(1000);
        RawTableUpdate builder = new RawTableUpdate(toUpdate);
        mgr.updateRawTable(builder);
        mgr.reloadAll();
        RawTableInstance updated = mgr.getRawTableInstance(cubeName);

        assertEquals(1000, updated.getCreateTimeUTC());
    }

    @Test
    public void testCreateAndDrop() throws Exception {
        final String cubeName = "a_whole_new_rawtable";
        final String cubeDesc = "ci_left_join_cube";

        // cube for raw dependency
        createCubeInstance(cubeName, cubeDesc);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableDescManager descMgr = RawTableDescManager.getInstance(getTestConfig());
        ResourceStore store = getStore();
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());

        store.deleteResource("/raw_table_instance/" + cubeName + ".json");

        RawTableDesc desc = descMgr.getRawTableDesc(cubeDesc);
        RawTableInstance createdRaw = rawMgr.createRawTableInstance(cubeName, ProjectInstance.DEFAULT_PROJECT_NAME,
                desc, null);

        assertTrue(createdRaw.equals(rawMgr.getRawTableInstance(cubeName)));
        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(createdRaw));

        RawTableInstance droppedRaw = RawTableManager.getInstance(getTestConfig()).dropRawTableInstance(cubeName,
                false);

        assertTrue(createdRaw.equals(droppedRaw));
        assertTrue(!prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(droppedRaw));
        assertNull(RawTableManager.getInstance(getTestConfig()).getRawTableInstance(cubeName));

        // cube for raw dependency
        dropCubeInstance(cubeName);
    }

    private void createCubeInstance(String cubeName, String descName) throws IOException {
        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        ProjectManager prjMgr = ProjectManager.getInstance(config);
        ResourceStore store = getStore();
        store.deleteResource("/cube/" + cubeName + ".json");
        CubeDescManager cubeDescMgr = CubeDescManager.getInstance(config);
        CubeDesc desc = cubeDescMgr.getCubeDesc(descName);
        CubeInstance createdCube = cubeMgr.createCube(cubeName, ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);

        assertTrue(createdCube.equals(cubeMgr.getCube(cubeName)));
        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(createdCube));

    }

    private void dropCubeInstance(String cubeName) throws IOException {
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());
        CubeInstance droppedCube = CubeManager.getInstance(getTestConfig()).dropCube(cubeName, false);

        assertTrue(!prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(droppedCube));
        assertNull(CubeManager.getInstance(getTestConfig()).getCube(cubeName));
    }

    @Test
    public void testAutoMergeNormal() throws Exception {
        String cubeName = "ci_left_join_cube";

        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube(cubeName).latestCopyForWrite();

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        raw.getRawTableDesc().setAutoMergeTimeRanges(new long[] { 2000, 6000 });

        mgr.updateCube(new CubeUpdate(cube));
        rawMgr.updateRawTable(new RawTableUpdate(raw));

        assertTrue(cube.needAutoMerge());
        assertTrue(raw.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        assertEquals(0, raw.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        RawTableUpdate rawUpdate = new RawTableUpdate(raw);
        rawMgr.updateRawTable(rawUpdate);

        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size());
        assertEquals(2, raw.getSegments().size());

        SegmentRange mergedSeg = cube.autoMergeCubeSegments();
        SegmentRange mergedRawSeg = raw.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);
        assertTrue(mergedRawSeg != null);
    }

    @Test
    public void testGetAllRawTables() throws Exception {
        final ResourceStore store = ResourceStore.getKylinMetaStore(getTestConfig());
        final NavigableSet<String> rawPath = store.listResources(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT);
        assertTrue(rawPath.size() > 1);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        final List<RawTableInstance> raws = rawMgr.listAllRawTables();
        assertEquals(rawPath.size(), raws.size());
    }

    @Test
    public void testAutoMergeWithGap() throws Exception {
        String cubeName = "ci_left_join_cube";

        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube(cubeName).latestCopyForWrite();

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        mgr.updateCube(new CubeUpdate(cube));

        raw.getRawTableDesc().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        rawMgr.updateRawTable(new RawTableUpdate(raw));

        assertTrue(cube.needAutoMerge());
        assertTrue(raw.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        assertEquals(0, raw.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(2000L, 4000L));
        mgr.updateCubeSegStatus(seg3, SegmentStatusEnum.READY);

        RawTableSegment rawSeg3 = rawMgr.appendSegment(raw, seg3);
        rawSeg3.setStatus(SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size());
        assertEquals(2, raw.getSegments().size());

        SegmentRange mergedSeg = cube.autoMergeCubeSegments();
        SegmentRange mergedRawSeg = raw.autoMergeCubeSegments();

        assertTrue(mergedSeg == null);
        assertTrue(mergedRawSeg == null);

        // append a new seg which will be merged

        CubeSegment seg4 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(4000L, 8000L));
        mgr.updateCubeSegStatus(seg4, SegmentStatusEnum.READY);

        RawTableSegment rawSeg4 = rawMgr.appendSegment(raw, seg4);
        rawSeg4.setStatus(SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(3, cube.getSegments().size());
        assertEquals(3, raw.getSegments().size());

        mergedSeg = cube.autoMergeCubeSegments();
        mergedRawSeg = raw.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);
        assertTrue((Long) mergedSeg.getStart() == 2000 && (Long) mergedSeg.getEnd() == 8000);

        assertTrue(mergedRawSeg != null);
        assertTrue((Long) mergedRawSeg.getStart() == 2000 && (Long) mergedRawSeg.getEnd() == 8000);

        // fill the gap

        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(4, cube.getSegments().size());
        assertEquals(4, raw.getSegments().size());

        mergedSeg = cube.autoMergeCubeSegments();
        mergedRawSeg = raw.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);
        assertTrue((Long) mergedSeg.getStart() == 0 && (Long) mergedSeg.getEnd() == 8000);

        assertTrue(mergedRawSeg != null);
        assertTrue((Long) mergedRawSeg.getStart() == 0 && (Long) mergedRawSeg.getEnd() == 8000);
    }
}
