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

package org.apache.kylin.job.common;

import java.util.UUID;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(TimeZoneTestRunner.class)
@PrepareForTest({ SegmentUtil.class })
public class SegmentsTest {

    @Test
    public void testGetSegmentStatusToDisplay_Building() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.NEW);
        segments.add(seg);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, seg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.LOADING);

        seg.setStatus(SegmentStatusEnum.READY);
        PowerMockito.mockStatic(SegmentUtil.class);
        PowerMockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg)).thenCallRealMethod();
        PowerMockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(true);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.LOADING);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Ready() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setDataflow(new NDataflow());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);
        PowerMockito.mockStatic(SegmentUtil.class);
        PowerMockito.when(SegmentUtil.getSegmentStatusToDisplay(segments, seg)).thenCallRealMethod();
        PowerMockito.when(SegmentUtil.anyIndexJobRunning(seg)).thenReturn(false);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, seg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.ONLINE);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Refreshing() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setId(UUID.randomUUID().toString());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);

        val newSeg = new NDataSegment();
        newSeg.setId(UUID.randomUUID().toString());
        newSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        newSeg.setStatus(SegmentStatusEnum.NEW);
        segments.add(newSeg);
        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, newSeg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.REFRESHING);

        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Merging() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setId(UUID.randomUUID().toString());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);

        val seg2 = new NDataSegment();
        seg2.setId(UUID.randomUUID().toString());
        seg2.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        seg2.setStatus(SegmentStatusEnum.READY);
        segments.add(seg2);

        val newSeg = new NDataSegment();
        newSeg.setId(UUID.randomUUID().toString());
        newSeg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 20L));
        newSeg.setStatus(SegmentStatusEnum.NEW);
        segments.add(newSeg);

        SegmentStatusEnumToDisplay status = SegmentUtil.getSegmentStatusToDisplay(segments, newSeg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.MERGING);

        SegmentStatusEnumToDisplay status2 = SegmentUtil.getSegmentStatusToDisplay(segments, seg);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);

        SegmentStatusEnumToDisplay status3 = SegmentUtil.getSegmentStatusToDisplay(segments, seg2);
        Assert.assertEquals(status3, SegmentStatusEnumToDisplay.LOCKED);

    }

    public NDataSegment newReadySegment(Long startTime, Long endTime) {
        val seg = new NDataSegment();
        seg.setId(UUID.randomUUID().toString());
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(startTime, endTime));
        seg.setStatus(SegmentStatusEnum.READY);
        return seg;
    }

}
