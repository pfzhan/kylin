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

package io.kyligence.kap.metadata.model;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(TimeZoneTestRunner.class)
public class SegmentsTest {
    KylinConfig config;

    @Before
    public void setUp() throws Exception {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        config = KylinConfig.getInstanceFromEnv();
    }

    @Test
    public void testGetMergeEnd_ByHour() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 03:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 03:00:00"), AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 04:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByHour() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.HOUR, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 01:03:00"), start);

        start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 03:00:00"), AutoMergeTimeEnum.HOUR, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-10 02:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByDay() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-10 09:00:00"), AutoMergeTimeEnum.DAY);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-11 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-29 09:00:00"), AutoMergeTimeEnum.DAY);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByDay() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-10 02:03:00"), AutoMergeTimeEnum.DAY, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-09 02:03:00"), start);

        start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-01 11:00:00"), AutoMergeTimeEnum.DAY, -2);
        Assert.assertEquals(DateFormat.stringToMillis("2012-01-30 11:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-05 01:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-06 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-06 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-13 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-08 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-13 00:00:00"), end);
    }

    @Test
    public void testGetRetentionStart_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-05 09:00:00"), AutoMergeTimeEnum.WEEK, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-01-29 09:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekSunday() {
        config.setProperty("kylin.metadata.first-day-of-week", "sunday");
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-05 01:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-06 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-11 04:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-12 00:00:00"), end);
        config.setProperty("kylin.metadata.first-day-of-week", "monday");

    }


    @Test
    public void testGetMergeEnd_ByWeek_AWeekOverlapTwoMonth() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.WEEK);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);

    }

    @Test
    public void testGetMergeEnd_ByMonth() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.MONTH);
        Assert.assertEquals(DateFormat.stringToMillis("2012-03-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-03-01 00:00:00"), AutoMergeTimeEnum.MONTH);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

    }

    @Test
    public void testGetMergeEnd_ByQuarter() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-03-01 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-04-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-04-28 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-07-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2012-05-01 00:00:00"), AutoMergeTimeEnum.QUARTER);
        Assert.assertEquals(DateFormat.stringToMillis("2012-07-01 00:00:00"), end);

    }

    @Test
    public void testGetRetentionStart_ByMonth() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-03-31 00:00:00"), AutoMergeTimeEnum.MONTH, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-29 00:00:00"), start);
    }

    @Test
    public void testGetMergeEnd_ByYear() {
        Segments segments = new Segments();
        long end = segments.getMergeEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.YEAR);
        Assert.assertEquals(DateFormat.stringToMillis("2013-01-01 00:00:00"), end);

        end = segments.getMergeEnd(DateFormat.stringToMillis("2013-01-01 00:00:00"), AutoMergeTimeEnum.YEAR);
        Assert.assertEquals(DateFormat.stringToMillis("2014-01-01 00:00:00"), end);

    }

    @Test
    public void testGetRetentionStart_ByYear() {
        Segments segments = new Segments();
        long start = segments.getRetentionEnd(DateFormat.stringToMillis("2012-02-28 00:00:00"), AutoMergeTimeEnum.YEAR, -1);
        Assert.assertEquals(DateFormat.stringToMillis("2011-02-28 00:00:00"), start);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Building() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.NEW);
        segments.add(seg);
        SegmentStatusEnumToDisplay status = segments.getSegmentStatusToDisplay(seg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.LOADING);
    }

    @Test
    public void testGetSegmentStatusToDisplay_Ready() {
        Segments segments = new Segments();
        val seg = new NDataSegment();
        seg.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 10L));
        seg.setStatus(SegmentStatusEnum.READY);
        segments.add(seg);
        SegmentStatusEnumToDisplay status = segments.getSegmentStatusToDisplay(seg);
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
        SegmentStatusEnumToDisplay status = segments.getSegmentStatusToDisplay(newSeg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.REFRESHING);

        SegmentStatusEnumToDisplay status2 = segments.getSegmentStatusToDisplay(seg);
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

        SegmentStatusEnumToDisplay status = segments.getSegmentStatusToDisplay(newSeg);
        Assert.assertEquals(status, SegmentStatusEnumToDisplay.MERGING);

        SegmentStatusEnumToDisplay status2 = segments.getSegmentStatusToDisplay(seg);
        Assert.assertEquals(status2, SegmentStatusEnumToDisplay.LOCKED);

        SegmentStatusEnumToDisplay status3 = segments.getSegmentStatusToDisplay(seg2);
        Assert.assertEquals(status3, SegmentStatusEnumToDisplay.LOCKED);

    }
}
