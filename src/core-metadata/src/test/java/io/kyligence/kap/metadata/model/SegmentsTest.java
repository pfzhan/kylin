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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.Segments;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        //2012-02-10 02:03:00
        long end = segments.getMergeEnd(1328810580000L, AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(1328814000000L, end);

        //2012-02-10 03:00:00
        end = segments.getMergeEnd(1328814000000L, AutoMergeTimeEnum.HOUR);
        Assert.assertEquals(1328817600000L, end);
    }

    @Test
    public void testGetRetentionStart_ByHour() {
        Segments segments = new Segments();
        //2012-02-10 02:03:00
        long start = segments.getRetentionEnd(1328810580000L, AutoMergeTimeEnum.HOUR, -1);
        //2012-02-10 01:03:00
        Assert.assertEquals(1328806980000L, start);

        //2012-02-10 03:00:00
        start = segments.getRetentionEnd(1328814000000L, AutoMergeTimeEnum.HOUR, -1);
        //2012-02-10 02:00:00
        Assert.assertEquals(1328810400000L, start);
    }

    @Test
    public void testGetMergeEnd_ByDay() {
        Segments segments = new Segments();
        //2012-02-10 02:03:00
        long end = segments.getMergeEnd(1328835600000L, AutoMergeTimeEnum.DAY);
        Assert.assertEquals(1328918400000L, end);

        //2012-02-28 03:00:00
        end = segments.getMergeEnd(1330477200000L, AutoMergeTimeEnum.DAY);
        Assert.assertEquals(1330560000000L, end);
    }

    @Test
    public void testGetRetentionStart_ByDay() {
        Segments segments = new Segments();
        //2012-02-10 02:03:00
        long start = segments.getRetentionEnd(1328810580000L, AutoMergeTimeEnum.DAY, -1);
        //2012-02-09 02:03:00
        Assert.assertEquals(1328724180000L, start);

        //2010-02-01 11:00:00
        start = segments.getRetentionEnd(1264993200000L, AutoMergeTimeEnum.DAY, -2);
        //2010-01-30 11:00:00
        Assert.assertEquals(1264820400000L, start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        //2012-02-5 1:00:00 sunday
        long end = segments.getMergeEnd(1328403600000L, AutoMergeTimeEnum.WEEK);
        //2012-02-6 00:00:00 next monday
        Assert.assertEquals(1328486400000L, end);

        //2012-02-6 00:00:00 monday
        end = segments.getMergeEnd(1328486400000L, AutoMergeTimeEnum.WEEK);
        //2012-02-13 00:00:00 monday
        Assert.assertEquals(1329091200000L, end);

        //2012-02-8 00:00:00 Wednesday
        end = segments.getMergeEnd(1328659200000L, AutoMergeTimeEnum.WEEK);
        //2012-02-13 00:00:00 monday
        Assert.assertEquals(1329091200000L, end);
    }

    @Test
    public void testGetRetentionStart_ByWeek_FirstDayOfWeekMonday() {
        Segments segments = new Segments();
        //2012-02-5 9:00:00 sunday
        long start = segments.getRetentionEnd(1328403600000L, AutoMergeTimeEnum.WEEK, -1);
        //2012-01-29 09:00:00 sunday
        Assert.assertEquals(1327798800000L, start);
    }

    @Test
    public void testGetMergeEnd_ByWeek_FirstDayOfWeekSunday() {
        config.setProperty("kylin.metadata.first-day-of-week", "sunday");
        Segments segments = new Segments();
        //2012-02-5 1:00:00 sunday
        long end = segments.getMergeEnd(1328403600000L, AutoMergeTimeEnum.WEEK);
        //2012-02-12 00:00:00 next sunday
        Assert.assertEquals(1329004800000L, end);

        //2012-02-6 00:00:00 monday
        end = segments.getMergeEnd(1328486400000L, AutoMergeTimeEnum.WEEK);
        //2012-02-12 00:00:00 sunday
        Assert.assertEquals(1329004800000L, end);

        //2012-02-11 04:00:00 Saturday
        end = segments.getMergeEnd(1328932800000L, AutoMergeTimeEnum.WEEK);
        //2012-02-12 00:00:00 sunday
        Assert.assertEquals(1329004800000L, end);
        config.setProperty("kylin.metadata.first-day-of-week", "monday");

    }


    @Test
    public void testGetMergeEnd_ByWeek_AWeekOverlapTwoMonth() {
        Segments segments = new Segments();
        //2012-02-28 00:00:00 Tuesday
        long end = segments.getMergeEnd(1330387200000L, AutoMergeTimeEnum.WEEK);
        //2012-03-01 00:00:00 Wednesday
        Assert.assertEquals(1330560000000L, end);

    }

    @Test
    public void testGetMergeEnd_ByMonth() {
        Segments segments = new Segments();
        //2012-02-28 00:00:00
        long end = segments.getMergeEnd(1330387200000L, AutoMergeTimeEnum.MONTH);
        //2012-03-01 00:00:00
        Assert.assertEquals(1330560000000L, end);

        //2012-03-01 00:00:00
        end = segments.getMergeEnd(1330560000000L, AutoMergeTimeEnum.MONTH);
        //2012-04-01 00:00:00
        Assert.assertEquals(1333238400000L, end);

    }

    @Test
    public void testGetRetentionStart_ByMonth() {
        Segments segments = new Segments();
        //2012-03-31 00:00:00
        long start = segments.getRetentionEnd(1333123200000L, AutoMergeTimeEnum.MONTH, -1);
        //2012-03-01 00:00:00
        Assert.assertEquals(1330531200000L, start);
    }

    @Test
    public void testGetMergeEnd_ByYear() {
        Segments segments = new Segments();
        //2012-02-28 00:00:00
        long end = segments.getMergeEnd(1330387200000L, AutoMergeTimeEnum.YEAR);
        //2013-01-01 00:00:00
        Assert.assertEquals(1356998400000L, end);

        //2013-01-01 00:00:00
        end = segments.getMergeEnd(1356998400000L, AutoMergeTimeEnum.YEAR);
        //2014-01-01 08:00:00
        Assert.assertEquals(1388534400000L, end);

    }

    @Test
    public void testGetRetentionStart_ByYear() {
        Segments segments = new Segments();
        //2012-02-28 00:00:00
        long start = segments.getRetentionEnd(1330387200000L, AutoMergeTimeEnum.YEAR, -1);
        //2011-02-28 08:00:00
        Assert.assertEquals(1298851200000L, start);
    }
}
