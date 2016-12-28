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
import org.apache.kylin.metadata.model.SegmentStatusEnum;
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
        CubeSegment seg = mgr.appendSegment(cube);
        assertEquals(0, seg.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, seg.getDateRangeEnd());
        assertEquals(0, seg.getSourceOffsetStart());
        assertEquals(Long.MAX_VALUE, seg.getSourceOffsetEnd());
        assertEquals(1, cube.getSegments().size());

        RawTableSegment rawSeg = rawMgr.appendSegment(raw, seg);
        assertEquals(0, rawSeg.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, rawSeg.getDateRangeEnd());
        assertEquals(0, rawSeg.getSourceOffsetStart());
        assertEquals(Long.MAX_VALUE, rawSeg.getSourceOffsetEnd());
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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);
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
        CubeSegment seg2 = mgr.appendSegment(cube);
        assertEquals(0, seg2.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, seg2.getDateRangeEnd());
        assertEquals(0, seg2.getSourceOffsetStart());
        assertEquals(Long.MAX_VALUE, seg2.getSourceOffsetEnd());
        assertEquals(2, cube.getSegments().size());

        assertEquals(1, raw.getSegments().size());
        RawTableSegment rawSeg = raw.getSegments(SegmentStatusEnum.READY).get(0);
        assertEquals(SegmentStatusEnum.READY, rawSeg.getStatus());

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        assertEquals(0, rawSeg2.getDateRangeStart());
        assertEquals(Long.MAX_VALUE, rawSeg2.getDateRangeEnd());
        assertEquals(0, rawSeg2.getSourceOffsetStart());
        assertEquals(Long.MAX_VALUE, rawSeg2.getSourceOffsetEnd());
        assertEquals(2, raw.getSegments().size());

        // non-partitioned cannot merge, throw exception
        try {
            rawMgr.mergeSegments(raw, null, 0, 0, 0, Long.MAX_VALUE, false);
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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);

        // append second
        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000);
        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);

        assertEquals(2, cube.getSegments().size());
        assertEquals(1000, seg2.getDateRangeStart());
        assertEquals(2000, seg2.getDateRangeEnd());
        assertEquals(1000, seg2.getSourceOffsetStart());
        assertEquals(2000, seg2.getSourceOffsetEnd());
        assertEquals(SegmentStatusEnum.NEW, seg2.getStatus());
        seg2.setStatus(SegmentStatusEnum.READY);

        assertEquals(2, raw.getSegments().size());
        assertEquals(1000, rawSeg2.getDateRangeStart());
        assertEquals(2000, rawSeg2.getDateRangeEnd());
        assertEquals(1000, rawSeg2.getSourceOffsetStart());
        assertEquals(2000, rawSeg2.getSourceOffsetEnd());
        assertEquals(SegmentStatusEnum.NEW, rawSeg2.getStatus());
        rawSeg2.setStatus(SegmentStatusEnum.READY);

        // merge first and second
        CubeSegment merge = mgr.mergeSegments(cube, 0, 2000, 0, 0, true);
        RawTableSegment rawMerge = rawMgr.mergeSegments(raw, merge.getUuid(), 0, 2000, 0, 0, true);

        assertEquals(3, cube.getSegments().size());
        assertEquals(0, merge.getDateRangeStart());
        assertEquals(2000, merge.getDateRangeEnd());
        assertEquals(0, merge.getSourceOffsetStart());
        assertEquals(2000, merge.getSourceOffsetEnd());
        assertEquals(SegmentStatusEnum.NEW, merge.getStatus());

        assertEquals(3, raw.getSegments().size());
        assertEquals(0, rawMerge.getDateRangeStart());
        assertEquals(2000, rawMerge.getDateRangeEnd());
        assertEquals(0, rawMerge.getSourceOffsetStart());
        assertEquals(2000, rawMerge.getSourceOffsetEnd());
        assertEquals(SegmentStatusEnum.NEW, rawMerge.getStatus());

        // segments are strictly ordered
        assertEquals(seg1, cube.getSegments().get(0));
        assertEquals(merge, cube.getSegments().get(1));
        assertEquals(seg2, cube.getSegments().get(2));

        assertEquals(rawSeg1, raw.getSegments().get(0));
        assertEquals(rawMerge, raw.getSegments().get(1));
        assertEquals(rawSeg2, raw.getSegments().get(2));

        // drop the merge
        cube.getSegments().remove(merge);
        raw.getSegments().remove(rawMerge);

        // try merge at start/end at middle of segments
        try {
            rawMgr.mergeSegments(raw, null, 500, 2500, 0, 0, true);
            fail();
        } catch (IllegalArgumentException ex) {
            // good
        }

        CubeSegment merge2 = mgr.mergeSegments(cube, 0, 2500, 0, 0, true);
        assertEquals(3, cube.getSegments().size());
        assertEquals(0, merge2.getDateRangeStart());
        assertEquals(2000, merge2.getDateRangeEnd());
        assertEquals(0, merge2.getSourceOffsetStart());
        assertEquals(2000, merge2.getSourceOffsetEnd());

        RawTableSegment rawMerge2 = rawMgr.mergeSegments(raw, merge2.getUuid(), 0, 2500, 0, 0, true);
        assertEquals(3, raw.getSegments().size());
        assertEquals(0, rawMerge2.getDateRangeStart());
        assertEquals(2000, rawMerge2.getDateRangeEnd());
        assertEquals(0, rawMerge2.getSourceOffsetStart());
        assertEquals(2000, rawMerge2.getSourceOffsetEnd());

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
        CubeSegment seg1 = mgr.appendSegment(cube, 0, 1000);
        seg1.setStatus(SegmentStatusEnum.READY);
        assertEquals(1, cube.getSegments().size());

        RawTableSegment rawSeg1 = rawMgr.appendSegment(raw, seg1);
        rawSeg1.setStatus(SegmentStatusEnum.READY);
        assertEquals(1, raw.getSegments().size());

        // append the third
        CubeSegment seg3 = mgr.appendSegment(cube, 2000, 3000);
        seg3.setStatus(SegmentStatusEnum.READY);
        assertEquals(2, cube.getSegments().size());

        RawTableSegment rawSeg3 = rawMgr.appendSegment(raw, seg3);
        rawSeg3.setStatus(SegmentStatusEnum.READY);
        assertEquals(2, raw.getSegments().size());

        // reject overlap
        try {
            mgr.appendSegment(cube, 1000, 2500);
            fail();
        } catch (IllegalStateException ex) {
            // good
        }

        // append the second
        CubeSegment seg2 = mgr.appendSegment(cube, 1000, 2000);
        seg2.setStatus(SegmentStatusEnum.READY);
        assertEquals(3, cube.getSegments().size());

        RawTableSegment rawSeg2 = rawMgr.appendSegment(raw, seg2);
        rawSeg2.setStatus(SegmentStatusEnum.READY);
        assertEquals(3, raw.getSegments().size());

        // merge all
        CubeSegment merge2 = mgr.mergeSegments(cube, 0, 3500, 0, 0, true);
        assertEquals(4, cube.getSegments().size());
        assertEquals(0, merge2.getDateRangeStart());
        assertEquals(3000, merge2.getDateRangeEnd());
        assertEquals(0, merge2.getSourceOffsetStart());
        assertEquals(3000, merge2.getSourceOffsetEnd());

        RawTableSegment rawMerge2 = rawMgr.mergeSegments(raw, merge2.getUuid(), 0, 3500, 0, 0, true);
        assertEquals(4, raw.getSegments().size());
        assertEquals(0, rawMerge2.getDateRangeStart());
        assertEquals(3000, rawMerge2.getDateRangeEnd());
        assertEquals(0, rawMerge2.getSourceOffsetStart());
        assertEquals(3000, rawMerge2.getSourceOffsetEnd());

        merge2.setLastBuildJobID("mockupfortest");
        mgr.promoteNewlyBuiltSegments(cube, merge2);
        rawMerge2.setLastBuildJobID("mockupfortest");
        rawMgr.promoteNewlyBuiltSegments(raw, rawMerge2);

        assertEquals(1, raw.getSegments().size());
        assertEquals(1, cube.getSegments().size());
        assertEquals(merge2.getUuid(), cube.getSegments().get(0).getUuid());
        assertEquals(rawMerge2.getUuid(), raw.getSegments().get(0).getUuid());
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }
}