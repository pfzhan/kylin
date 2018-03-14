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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TimeRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class RawTableSegmentsTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testAppendNonPartitioned() throws IOException {
        String cubeName = "ci_left_join_cube";
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube(cubeName);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        // override partition desc
        cube.getModel().setPartitionDesc(new PartitionDesc());
        raw.getModel().setPartitionDesc(new PartitionDesc());

        // first append, creates a new & single segment
        CubeSegment seg = mgr.appendSegment(cube, null);
        assertEquals(new TimeRange(0L, Long.MAX_VALUE), seg.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE), seg.getSegRange());

        assertEquals(0, cube.getSegments().size()); // older cube not changed
        cube = mgr.getCube(cube.getName());
        assertEquals(1, cube.getSegments().size()); // the updated cube

        RawTableSegment rawSeg = rawMgr.appendSegment(raw, seg);
        assertEquals(new TimeRange(0L, Long.MAX_VALUE), rawSeg.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE), rawSeg.getSegRange());
        assertEquals(1, raw.getSegments().size());

        // second append, throw IllegalStateException because the first segment is not built
        try {
            rawMgr.appendSegment(raw, seg);
            fail();
        } catch (IllegalStateException ex) {
        }
    }

    @Test
    public void testAppendNonPartitioned2() throws IOException {
        String cubeName = "ci_left_join_cube";
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube(cubeName);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        // append the first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        cube = readySegment(cube, seg1);
        assertEquals(1, cube.getSegments().size());

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);
        assertEquals(1, raw.getSegments().size());

        // override partition desc
        cube.getModel().setPartitionDesc(new PartitionDesc());
        raw.getModel().setPartitionDesc(new PartitionDesc());

        // assert one ready segment
        assertEquals(1, cube.getSegments().size());
        CubeSegment seg = cube.getSegments(SegmentStatusEnum.READY).get(0);
        assertEquals(SegmentStatusEnum.READY, seg.getStatus());

        // append again, for non-partitioned cube, it becomes a full refresh
        CubeSegment seg2 = mgr.appendSegment(cube, null);
        assertEquals(new TimeRange(0L, Long.MAX_VALUE), seg2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE), seg2.getSegRange());

        assertEquals(1, cube.getSegments().size()); // older cube not changed
        cube = mgr.getCube(cube.getName());
        assertEquals(2, cube.getSegments().size()); // the updated cube

        assertEquals(1, raw.getSegments().size());
        RawTableSegment rawSeg = raw.getSegments(SegmentStatusEnum.READY).get(0);
        assertEquals(SegmentStatusEnum.READY, rawSeg.getStatus());

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        assertEquals(new TimeRange(0L, Long.MAX_VALUE), rawSeg2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE), rawSeg2.getSegRange());
        assertEquals(2, raw.getSegments().size());

        // non-partitioned cannot merge, throw exception
        try {
            rawMgr.mergeSegments(raw, null, new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE), false);
            fail();
        } catch (IllegalStateException ex) {
            // good
        }
    }

    @Test
    public void testPartitioned() throws IOException {
        String cubeName = "ci_left_join_cube";
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube(cubeName);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        assertEquals(0, raw.getSegments().size());

        // append first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        cube = readySegment(cube, seg1);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        // append second
        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        cube = readySegment(cube, seg2);
        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);

        assertEquals(2, cube.getSegments().size());
        assertEquals(new TimeRange(1000L, 2000L), seg2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L), seg2.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, seg2.getStatus()); // older version of seg2
        assertEquals(SegmentStatusEnum.READY, cube.getSegments().get(1).getStatus()); // newer version of seg2

        assertEquals(2, raw.getSegments().size());
        assertEquals(new TimeRange(1000L, 2000L), rawSeg2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L), rawSeg2.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, rawSeg2.getStatus());
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        // merge first and second
        CubeSegment merge = mgr.mergeSegments(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), true);
        RawTableSegment rawMerge = rawMgr.mergeSegments(raw, merge.getUuid(),
                new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), true);

        cube = mgr.getCube(cube.getName()); // get the updated version of cube
        assertEquals(3, cube.getSegments().size());
        assertEquals(new TimeRange(0L, 2000L), merge.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), merge.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, merge.getStatus());

        assertEquals(3, raw.getSegments().size());
        assertEquals(new TimeRange(0L, 2000L), rawMerge.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), rawMerge.getSegRange());
        assertEquals(SegmentStatusEnum.NEW, rawMerge.getStatus());

        // segments are strictly ordered
        assertEquals(seg1.getUuid(), cube.getSegments().get(0).getUuid());
        assertEquals(merge.getUuid(), cube.getSegments().get(1).getUuid());
        assertEquals(seg2.getUuid(), cube.getSegments().get(2).getUuid());

        assertEquals(rawSeg1.getUuid(), raw.getSegments().get(0).getUuid());
        assertEquals(rawMerge.getUuid(), raw.getSegments().get(1).getUuid());
        assertEquals(rawSeg2.getUuid(), raw.getSegments().get(2).getUuid());

        // drop the merge
        cube = mgr.updateCubeDropSegments(cube, merge);
        raw.getSegments().remove(rawMerge);

        // try merge at start/end at middle of segments
        try {
            rawMgr.mergeSegments(raw, null, new SegmentRange.TimePartitionedSegmentRange(500L, 2500L), true);
            fail();
        } catch (IllegalArgumentException ex) {
            // good
        }

        CubeSegment merge2 = mgr.mergeSegments(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 2500L), true);
        cube = mgr.getCube(cube.getName()); // get the updated version of cube
        merge2 = cube.getSegmentById(merge2.getUuid());
        assertEquals(3, cube.getSegments().size());
        assertEquals(new TimeRange(0L, 2000L), merge2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), merge2.getSegRange());

        RawTableSegment rawMerge2 = rawMgr.mergeSegments(raw, merge2.getUuid(),
                new SegmentRange.TimePartitionedSegmentRange(0L, 2500L), true);
        assertEquals(3, raw.getSegments().size());
        assertEquals(new TimeRange(0L, 2000L), rawMerge2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 2000L), rawMerge2.getSegRange());

        merge2.setLastBuildJobID("mockupfortest");
        mgr.promoteNewlyBuiltSegments(cube, merge2);
        rawMerge2.setLastBuildJobID("mockupfortest");
        rawMgr.promoteNewlyBuiltSegments(raw, rawMerge2);
    }

    @Test
    public void testAllowGap() throws IOException {

        String cubeName = "ci_left_join_cube";
        CubeManager mgr = mgr();
        CubeInstance cube = mgr.getCube(cubeName);

        RawTableManager rawMgr = RawTableManager.getInstance(getTestConfig());
        RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

        // no segment at first
        assertEquals(0, cube.getSegments().size());
        assertEquals(0, raw.getSegments().size());

        // append the first
        CubeSegment seg1 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 1000L));
        cube = readySegment(cube, seg1);
        assertEquals(1, cube.getSegments().size());

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);
        assertEquals(1, raw.getSegments().size());

        // append the third
        CubeSegment seg3 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(2000L, 3000L));
        cube = readySegment(cube, seg3);
        assertEquals(2, cube.getSegments().size());

        RawTableSegment rawSeg3 = rawMgr.appendSegment(raw, seg3);
        rawSeg3.setStatus(SegmentStatusEnum.READY);
        assertEquals(2, raw.getSegments().size());

        // reject overlap
        try {
            mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2500L));
            fail();
        } catch (IllegalStateException ex) {
            // good
        }

        // append the second
        CubeSegment seg2 = mgr.appendSegment(cube, new SegmentRange.TimePartitionedSegmentRange(1000L, 2000L));
        cube = readySegment(cube, seg2);
        assertEquals(3, cube.getSegments().size());

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);
        assertEquals(3, raw.getSegments().size());

        // merge all
        CubeSegment merge2 = mgr.mergeSegments(cube, new SegmentRange.TimePartitionedSegmentRange(0L, 3500L), true);
        cube = mgr.getCube(cube.getName()); // get latest version of cube
        merge2 = cube.getSegmentById(merge2.getUuid());
        assertEquals(4, cube.getSegments().size());
        assertEquals(new TimeRange(0L, 3000L), merge2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 3000L), merge2.getSegRange());

        RawTableSegment rawMerge2 = rawMgr.mergeSegments(raw, merge2.getUuid(),
                new SegmentRange.TimePartitionedSegmentRange(0L, 3500L), true);
        assertEquals(4, raw.getSegments().size());
        assertEquals(new TimeRange(0L, 3000L), rawMerge2.getTSRange());
        assertEquals(new SegmentRange.TimePartitionedSegmentRange(0L, 3000L), rawMerge2.getSegRange());

        merge2.setLastBuildJobID("mockupfortest");
        mgr.promoteNewlyBuiltSegments(cube, merge2);
        rawMerge2.setLastBuildJobID("mockupfortest");
        rawMgr.promoteNewlyBuiltSegments(raw, rawMerge2);

        cube = mgr.getCube(cube.getName()); // get latest version of cube
        assertEquals(1, raw.getSegments().size());
        assertEquals(1, cube.getSegments().size());
        assertEquals(merge2.getUuid(), cube.getSegments().get(0).getUuid());
        assertEquals(rawMerge2.getUuid(), raw.getSegments().get(0).getUuid());
    }

    private CubeInstance readySegment(CubeInstance cube, CubeSegment seg) throws IOException {
        return mgr().updateCubeSegStatus(seg, SegmentStatusEnum.READY);
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }
}