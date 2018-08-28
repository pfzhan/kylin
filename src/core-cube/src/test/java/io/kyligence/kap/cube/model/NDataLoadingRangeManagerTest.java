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

import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

import java.io.IOException;

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
    public void testCreateAndUpdateDataLoadingRange() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2013-01-01");
        SegmentRange.TimePartitionedDataLoadingRange range = new SegmentRange.TimePartitionedDataLoadingRange(start, end);
        dataLoadingRange.setDataLoadingRange(range);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        SegmentRange.TimePartitionedDataLoadingRange savedRange = (SegmentRange.TimePartitionedDataLoadingRange) savedDataLoadingRange.getDataLoadingRange();
        Assert.assertTrue(savedRange.getStart().equals(savedRange.getWaterMark()));

        range.setWaterMark(range.getEnd());
        savedDataLoadingRange.setDataLoadingRange(range);
        NDataLoadingRange updatedDataLoadingRange = dataLoadingRangeManager.updateDataLoadingRange(savedDataLoadingRange);
        SegmentRange.TimePartitionedDataLoadingRange updatedRange = (SegmentRange.TimePartitionedDataLoadingRange) updatedDataLoadingRange.getDataLoadingRange();

        Assert.assertTrue(updatedRange.getEnd().equals(updatedRange.getWaterMark()));

        NDataLoadingRange dataLoadingRange1 = dataLoadingRangeManager.getDataLoadingRange(tableName);
        SegmentRange.TimePartitionedDataLoadingRange updatedRange1 = (SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange1.getDataLoadingRange();

        Assert.assertTrue(updatedRange1.getWaterMark().equals(updatedRange.getWaterMark()));


        dataLoadingRangeManager.updateDataLoadingRangeWaterMark(tableName);

        NDataLoadingRange dataLoadingRange2 = dataLoadingRangeManager.getDataLoadingRange(tableName);
        SegmentRange.TimePartitionedDataLoadingRange updatedRange2 = (SegmentRange.TimePartitionedDataLoadingRange) dataLoadingRange2.getDataLoadingRange();
        Assert.assertTrue(updatedRange2.getWaterMark().equals(updatedRange.getWaterMark()));

    }


}
