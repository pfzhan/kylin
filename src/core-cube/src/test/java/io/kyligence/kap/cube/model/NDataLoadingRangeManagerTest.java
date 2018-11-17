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

import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class NDataLoadingRangeManagerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NDataLoadingRangeManager dataLoadingRangeManager;
    private String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetInstance() {
        NDataLoadingRangeManager mgrSsb = NDataLoadingRangeManager.getInstance(getTestConfig(), "ssb");
        Assert.assertNotEquals(DEFAULT_PROJECT, mgrSsb);

    }

    @Test
    public void testAppendSegRangeErrorCase() throws IOException {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        long start = 1505277121000L;
        long end = 1536813121000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        // test error case, add a segRange with has the overlaps/gap
        start = 1536813121000L;
        end = 1536813191000L;
        SegmentRange.TimePartitionedSegmentRange range5 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("has overlaps/gap with existing segmentRanges");
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range5);
        start = 0L;
        end = 1005277100000L;
        SegmentRange.TimePartitionedSegmentRange range6 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("has overlaps/gap with existing segmentRanges");
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range6);

    }

    @Test
    public void testCreateAndUpdateDataLoadingRange() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        long start = 1505277121000L;
        long end = 1536813121000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        int waterMarkEnd = savedDataLoadingRange.getWaterMarkEnd();
        int waterMarkStart = savedDataLoadingRange.getWaterMarkStart();
        Assert.assertTrue(waterMarkStart == -1);
        Assert.assertTrue(waterMarkEnd == -1);

        // add segmentRange at tail
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range);
        Assert.assertNotNull(savedDataLoadingRange.getSegmentRanges());
        Assert.assertEquals(savedDataLoadingRange.getSegmentRanges().size(), 1);

        start = 1505277111000L;
        end = 1505277121000L;
        // add segmentRange at first
        SegmentRange.TimePartitionedSegmentRange range2 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range2);
        Assert.assertNotNull(savedDataLoadingRange.getSegmentRanges());
        Assert.assertEquals(savedDataLoadingRange.getSegmentRanges().size(), 2);

        savedDataLoadingRange.setWaterMarkEnd(0);
        savedDataLoadingRange = dataLoadingRangeManager.copyForWrite(savedDataLoadingRange);
        NDataLoadingRange updatedDataLoadingRange = dataLoadingRangeManager
                .updateDataLoadingRange(savedDataLoadingRange);
        Assert.assertTrue(updatedDataLoadingRange.getWaterMarkEnd() == 0);

        // add segmentRange at tail when waterMarkEnd != -1
        start = 1536813121000L;
        end = 1536813191000L;
        SegmentRange.TimePartitionedSegmentRange range3 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(updatedDataLoadingRange, range3);
        Assert.assertNotNull(savedDataLoadingRange.getSegmentRanges());
        Assert.assertEquals(savedDataLoadingRange.getSegmentRanges().size(), 3);
        Assert.assertTrue(savedDataLoadingRange.getWaterMarkStart() == -1);
        Assert.assertTrue(savedDataLoadingRange.getWaterMarkEnd() == 0);

        // add segmentRange at first when waterMarkEnd != -1
        start = 0L;
        end = 1505277111000L;
        SegmentRange.TimePartitionedSegmentRange range4 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range4);
        Assert.assertNotNull(savedDataLoadingRange.getSegmentRanges());
        Assert.assertEquals(savedDataLoadingRange.getSegmentRanges().size(), 4);
        Assert.assertTrue(savedDataLoadingRange.getWaterMarkStart() == 0);
        Assert.assertTrue(savedDataLoadingRange.getWaterMarkEnd() == 1);

        SegmentRange coveredSegmentRange = savedDataLoadingRange.getCoveredSegmentRange();
        Assert.assertEquals(1536813191000L, (long) coveredSegmentRange.getEnd());
        Assert.assertEquals(0L, (long) coveredSegmentRange.getStart());

        SegmentRange coveredReadySegmentRange = savedDataLoadingRange.getCoveredReadySegmentRange();
        Assert.assertEquals(1505277121000L, (long) coveredReadySegmentRange.getEnd());
        Assert.assertEquals(1505277111000L, (long) coveredReadySegmentRange.getStart());

        dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);

        NDataLoadingRange dataLoadingRange2 = dataLoadingRangeManager.getDataLoadingRange(tableName);
        Assert.assertEquals(dataLoadingRange2.getWaterMarkEnd(), -1);


    }

}
