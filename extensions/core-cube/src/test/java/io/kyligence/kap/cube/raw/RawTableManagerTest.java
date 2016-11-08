package io.kyligence.kap.cube.raw;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
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
        final String cubeName = "test_kylin_cube_with_slr_empty";

        // init
        RawTableManager mgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance existing = mgr.getRawTableInstance(cubeName);

        Assert.assertNotNull(existing);

        RawTableDesc desc = existing.getRawTableDesc();
        Assert.assertNotNull(desc);

        // update
        RawTableInstance toUpdate = mgr.getRawTableInstance(cubeName);
        toUpdate.setVersion("dummy");
        RawTableUpdate builder = new RawTableUpdate(toUpdate);
        mgr.updateRawTable(builder);
        mgr.reloadAllRawTableInstance();
        RawTableInstance updated = mgr.getRawTableInstance(cubeName);

        assertEquals("dummy", updated.getVersion());
    }

    @Test
    public void testCreateAndDrop() throws Exception {
        final String cubeName = "a_whole_new_rawtable";
        final String cubeDesc = "test_kylin_cube_with_slr_desc";

        // cube for raw dependency
        createCubeInstance(cubeName, cubeDesc);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableDescManager descMgr = RawTableDescManager.getInstance(getTestConfig());
        ResourceStore store = getStore();
        ProjectManager prjMgr = ProjectManager.getInstance(getTestConfig());

        store.deleteResource("/raw_table_instance/" + cubeName + ".json");

        RawTableDesc desc = descMgr.getRawTableDesc(cubeDesc);
        RawTableInstance createdRaw = rawMgr.createRawTableInstance(cubeName, ProjectInstance.DEFAULT_PROJECT_NAME, desc, null);

        assertTrue(createdRaw == rawMgr.getRawTableInstance(cubeName));
        assertTrue(prjMgr.listAllRealizations(ProjectInstance.DEFAULT_PROJECT_NAME).contains(createdRaw));

        RawTableInstance droppedRaw = RawTableManager.getInstance(getTestConfig()).dropRawTableInstance(cubeName, false);

        assertTrue(createdRaw == droppedRaw);
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

        assertTrue(createdCube == cubeMgr.getCube(cubeName));
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
        String cubeName = "test_kylin_cube_with_slr_empty";

        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube(cubeName);

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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000);
        seg2.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        RawTableUpdate rawBuilder = new RawTableUpdate(raw);

        mgr.updateCube(cubeBuilder);
        rawMgr.updateRawTable(rawBuilder);

        assertEquals(2, cube.getSegments().size());
        assertEquals(2, raw.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);
        Pair<Long, Long> mergedRawSeg = rawMgr.autoMergeRawTableSegments(raw);

        assertTrue(mergedSeg != null);
        assertTrue(mergedRawSeg != null);
    }

    @Test
    public void testGetAllRawTables() throws Exception {
        final ResourceStore store = ResourceStore.getStore(getTestConfig());
        final NavigableSet<String> rawPath = store.listResources(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT);
        assertTrue(rawPath.size() > 1);

        final List<RawTableInstance> raws = store.getAllResources(RawTableInstance.RAW_TABLE_INSTANCE_RESOURCE_ROOT, RawTableInstance.class, RawTableManager.INSTANCE_SERIALIZER);
        assertEquals(rawPath.size(), raws.size());
    }

    @Test
    public void testAutoMergeWithGap() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_empty";

        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube(cubeName);

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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        CubeSegment seg3 = mgr.appendSegment(cube, 2000, 4000);
        seg3.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg3 = rawMgr.appendSegment(raw, seg3);
        rawSeg3.setStatus(SegmentStatusEnum.READY);

        assertEquals(2, cube.getSegments().size());
        assertEquals(2, raw.getSegments().size());

        Pair<Long, Long> mergedSeg = mgr.autoMergeCubeSegments(cube);
        Pair<Long, Long> mergedRawSeg = rawMgr.autoMergeRawTableSegments(raw);

        assertTrue(mergedSeg == null);
        assertTrue(mergedRawSeg == null);

        // append a new seg which will be merged

        CubeSegment seg4 = mgr.appendSegment(cube, 4000, 8000);
        seg4.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg4 = rawMgr.appendSegment(raw, seg4);
        rawSeg4.setStatus(SegmentStatusEnum.READY);

        assertEquals(3, cube.getSegments().size());
        assertEquals(3, raw.getSegments().size());

        mergedSeg = mgr.autoMergeCubeSegments(cube);
        mergedRawSeg = rawMgr.autoMergeRawTableSegments(raw);

        assertTrue(mergedSeg != null);
        assertTrue(mergedSeg.getFirst() == 2000 && mergedSeg.getSecond() == 8000);

        assertTrue(mergedRawSeg.getFirst() == 2000 && mergedRawSeg.getSecond() == 8000);
        assertTrue(mergedRawSeg != null);

        // fill the gap

        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000);
        seg2.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        assertEquals(4, cube.getSegments().size());
        assertEquals(4, raw.getSegments().size());

        mergedSeg = mgr.autoMergeCubeSegments(cube);
        mergedRawSeg = rawMgr.autoMergeRawTableSegments(raw);

        assertTrue(mergedSeg != null);
        assertTrue(mergedSeg.getFirst() == 0 && mergedSeg.getSecond() == 8000);

        assertTrue(mergedRawSeg != null);
        assertTrue(mergedRawSeg.getFirst() == 0 && mergedRawSeg.getSecond() == 8000);
    }
}
