/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.cube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 */
public class CubeManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {

        CubeInstance cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_without_slr_ready");
        CubeDesc desc = cube.getDescriptor();
        //System.out.println(JsonUtil.writeValueAsIndentString(desc));

        String signature = desc.calculateSignature();
        desc.getModel().getPartitionDesc().setPartitionDateColumn("test_column");
        assertTrue(signature.equals(desc.calculateSignature()));
    }

    @Test
    public void testCreateAndDrop() throws Exception {

        KylinConfig config = getTestConfig();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        ProjectManager prjMgr = ProjectManager.getInstance(config);
        ResourceStore store = getStore();

        // clean legacy in case last run failed
        store.deleteResource("/cube/a_whole_new_cube.json");

        CubeDescManager cubeDescMgr = getCubeDescManager();
        CubeDesc desc = cubeDescMgr.getCubeDesc("test_kylin_cube_with_slr_desc");
        CubeInstance createdCube = cubeMgr.createCube("a_whole_new_cube", ProjectInstance.DEFAULT_PROJECT_NAME, desc,
                null);
        assertTrue(createdCube.equals(cubeMgr.getCube("a_whole_new_cube")));

        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(createdCube));

        CubeInstance droppedCube = CubeManager.getInstance(getTestConfig()).dropCube("a_whole_new_cube", false);
        assertTrue(createdCube.equals(droppedCube));

        assertTrue(!prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(droppedCube));

        assertNull(CubeManager.getInstance(getTestConfig()).getCube("a_whole_new_cube"));
    }

    @Test
    public void testAutoMergeNormal() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty").latestCopyForWrite();

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        mgr.updateCube(new CubeUpdate(cube));

        assertTrue(cube.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size());

        SegmentRange mergedSeg = cube.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);

    }

    @Test
    public void testConcurrentBuildAndMerge() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty").latestCopyForWrite();

        System.setProperty("kylin.cube.max-building-segments", "10");
        // no segment at first
        assertEquals(0, cube.getSegments().size());

        Map<Integer, Long> m1 = Maps.newHashMap();
        m1.put(1, 1000L);
        Map<Integer, Long> m2 = Maps.newHashMap();
        m2.put(1, 2000L);
        Map<Integer, Long> m3 = Maps.newHashMap();
        m3.put(1, 3000L);
        Map<Integer, Long> m4 = Maps.newHashMap();
        m4.put(1, 4000L);

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 1000L, null, m1));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(1000L, 2000L, m1, m2));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.mergeSegments(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 2000L, null, null), true);
        //seg3.setStatus(SegmentStatusEnum.NEW);

        CubeSegment seg4 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(2000L, 3000L, m2, m3));

        seg4.setStatus(SegmentStatusEnum.NEW);
        seg4.setLastBuildJobID("test");
        seg4.setStorageLocationIdentifier("test");
        CubeUpdate update = new CubeUpdate(cube.latestCopyForWrite());
        update.setToUpdateSegs(seg4);
        mgr.updateCube(update);

        CubeSegment seg5 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(3000L, 4000L, m3, m4));
        mgr.updateCubeSegStatus(seg5, SegmentStatusEnum.READY);

        mgr.promoteNewlyBuiltSegments(cube, seg4);

        cube = mgr.getCube(cube.getName());
        assertTrue(cube.getSegments().size() == 5);

        assertTrue(cube.getSegmentById(seg1.getUuid()) != null
                && cube.getSegmentById(seg1.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg2.getUuid()) != null
                && cube.getSegmentById(seg2.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg3.getUuid()) != null
                && cube.getSegmentById(seg3.getUuid()).getStatus() == SegmentStatusEnum.NEW);
        assertTrue(cube.getSegmentById(seg4.getUuid()) != null
                && cube.getSegmentById(seg4.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg5.getUuid()) != null
                && cube.getSegmentById(seg5.getUuid()).getStatus() == SegmentStatusEnum.READY);

    }

    @Test
    public void testConcurrentMergeAndMerge() throws Exception {
        System.setProperty("kylin.cube.max-building-segments", "10");
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty").latestCopyForWrite();

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        Map<Integer, Long> m1 = Maps.newHashMap();
        m1.put(1, 1000L);
        Map<Integer, Long> m2 = Maps.newHashMap();
        m2.put(1, 2000L);
        Map<Integer, Long> m3 = Maps.newHashMap();
        m3.put(1, 3000L);
        Map<Integer, Long> m4 = Maps.newHashMap();
        m4.put(1, 4000L);

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 1000L, null, m1));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(1000L, 2000L, m1, m2));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(2000L, 3000L, m2, m3));
        mgr.updateCubeSegStatus(seg3, SegmentStatusEnum.READY);

        CubeSegment seg4 = mgr.appendSegment(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(3000L, 4000L, m3, m4));
        mgr.updateCubeSegStatus(seg4, SegmentStatusEnum.READY);

        CubeSegment merge1 = mgr.mergeSegments(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(0L, 2000L, null, null), true);
        merge1.setStatus(SegmentStatusEnum.NEW);
        merge1.setLastBuildJobID("test");
        merge1.setStorageLocationIdentifier("test");

        CubeSegment merge2 = mgr.mergeSegments(cube,
                new SegmentRange.KafkaOffsetPartitionedSegmentRange(2000L, 4000L, null, null), true);
        merge2.setStatus(SegmentStatusEnum.NEW);
        merge2.setLastBuildJobID("test");
        merge2.setStorageLocationIdentifier("test");

        CubeUpdate update = new CubeUpdate(cube.latestCopyForWrite());
        update.setToUpdateSegs(merge1, merge2);
        mgr.updateCube(update);

        mgr.promoteNewlyBuiltSegments(cube, merge1);

        cube = mgr.getCube(cube.getName());
        assertTrue(cube.getSegments().size() == 4);

        assertTrue(cube.getSegmentById(seg1.getUuid()) == null);
        assertTrue(cube.getSegmentById(seg2.getUuid()) == null);
        assertTrue(cube.getSegmentById(merge1.getUuid()) != null
                && cube.getSegmentById(merge1.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg3.getUuid()) != null
                && cube.getSegmentById(seg3.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(seg4.getUuid()) != null
                && cube.getSegmentById(seg4.getUuid()).getStatus() == SegmentStatusEnum.READY);
        assertTrue(cube.getSegmentById(merge2.getUuid()) != null
                && cube.getSegmentById(merge2.getUuid()).getStatus() == SegmentStatusEnum.NEW);

    }

    @Test
    public void testGetAllCubes() throws Exception {
        final ResourceStore store = ResourceStore.getKylinMetaStore(getTestConfig());
        final NavigableSet<String> cubePath = store.listResources(ResourceStore.CUBE_RESOURCE_ROOT);
        assertTrue(cubePath.size() > 1);

        final List<CubeInstance> cubes = store.getAllResources(ResourceStore.CUBE_RESOURCE_ROOT, CubeInstance.class,
                CubeManager.CUBE_SERIALIZER);
        assertEquals(cubePath.size(), cubes.size());
    }

    @Test
    public void testAutoMergeWithGap() throws Exception {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_empty").latestCopyForWrite();

        cube.getDescriptor().setAutoMergeTimeRanges(new long[] { 2000, 6000 });
        mgr.updateCube(new CubeUpdate(cube));

        assertTrue(cube.needAutoMerge());

        // no segment at first
        assertEquals(0, cube.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        mgr.updateCubeSegStatus(seg1, SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(2000L, 4000L));
        mgr.updateCubeSegStatus(seg3, SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size());

        SegmentRange mergedSeg = cube.autoMergeCubeSegments();

        assertTrue(mergedSeg == null);

        // append a new seg which will be merged

        CubeSegment seg4 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(4000L, 8000L));
        mgr.updateCubeSegStatus(seg4, SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(3, cube.getSegments().size());

        mergedSeg = cube.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);
        assertTrue((Long) mergedSeg.getStart() == 2000 && (Long) mergedSeg.getEnd() == 8000);

        // fill the gap

        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        mgr.updateCubeSegStatus(seg2, SegmentStatusEnum.READY);

        cube = mgr.getCube(cube.getName());
        assertEquals(4, cube.getSegments().size());

        mergedSeg = cube.autoMergeCubeSegments();

        assertTrue(mergedSeg != null);
        assertTrue((Long) mergedSeg.getStart() == 0 && (Long) mergedSeg.getEnd() == 8000);
    }

    @Test
    public void testGetCubeNameWithNamespace() {
        System.setProperty("kylin.storage.hbase.table-name-prefix", "HELLO_");
        try {
            CubeManager mgr = CubeManager.getInstance(getTestConfig());
            String tablename = mgr.generateStorageLocation();
            assertTrue(tablename.startsWith("HELLO_"));
        } finally {
            System.clearProperty("kylin.storage.hbase.table-name-prefix");
        }
    }

    public CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }
}
