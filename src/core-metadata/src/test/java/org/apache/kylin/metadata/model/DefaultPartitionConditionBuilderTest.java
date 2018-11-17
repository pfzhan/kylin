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

package org.apache.kylin.metadata.model;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.common.util.DateFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultPartitionConditionBuilderTest {

    private PartitionDesc.DefaultPartitionConditionBuilder partitionConditionBuilder;
    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
        partitionConditionBuilder = new PartitionDesc.DefaultPartitionConditionBuilder();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testDatePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'",
                condition);

        range = new SegmentRange.TimePartitionedSegmentRange(0L, 0L);
        condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("1=1", condition);
    }

    @Test
    public void testTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col);
        partitionDesc.setPartitionTimeColumn(col.getCanonicalName());
        partitionDesc.setPartitionTimeFormat("HH");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.HOUR_COLUMN >= '00' AND UNKNOWN_ALIAS.HOUR_COLUMN < '01'", condition);
    }

    @Test
    public void testDateAndTimePartition() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col1 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "string");
        partitionDesc.setPartitionDateColumnRef(col1);
        partitionDesc.setPartitionDateColumn(col1.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        TblColRef col2 = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 2, "HOUR_COLUMN", "string");
        partitionDesc.setPartitionTimeColumnRef(col2);
        partitionDesc.setPartitionTimeColumn(col2.getCanonicalName());
        partitionDesc.setPartitionTimeFormat("H");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22 00:00:00"), DateFormat.stringToMillis("2016-02-23 01:00:00"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-22' AND UNKNOWN_ALIAS.HOUR_COLUMN >= '0') OR (UNKNOWN_ALIAS.DATE_COLUMN > '2016-02-22')) AND ((UNKNOWN_ALIAS.DATE_COLUMN = '2016-02-23' AND UNKNOWN_ALIAS.HOUR_COLUMN < '1') OR (UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23'))",
                condition);
    }

    @Test
    public void testDatePartition_BigInt() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "bigint");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223",
                condition);
    }

    @Test
    public void testDatePartition_Date() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "date");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy/MM/dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= '2016/02/22' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016/02/23'",
                condition);
    }

    @Test
    public void testDatePartition_Long() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "long");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyyMMdd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223",
                condition);
    }

    @Test
    public void testDatePartition_Int() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "int");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals("UNKNOWN_ALIAS.DATE_COLUMN >= 20160222 AND UNKNOWN_ALIAS.DATE_COLUMN < 20160223",
                condition);
    }

    @Test
    public void testDatePartition_Timestamp() {
        PartitionDesc partitionDesc = new PartitionDesc();
        TblColRef col = TblColRef.mockup(TableDesc.mockup("DEFAULT.TABLE_NAME"), 1, "DATE_COLUMN", "timestamp");
        partitionDesc.setPartitionDateColumnRef(col);
        partitionDesc.setPartitionDateColumn(col.getCanonicalName());
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd HH:mm:ss");
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(
                DateFormat.stringToMillis("2016-02-22"), DateFormat.stringToMillis("2016-02-23"));
        String condition = partitionConditionBuilder.buildDateRangeCondition(partitionDesc, null, range);
        Assert.assertEquals(
                "UNKNOWN_ALIAS.DATE_COLUMN >= '2016-02-22 00:00:00' AND UNKNOWN_ALIAS.DATE_COLUMN < '2016-02-23 00:00:00'",
                condition);
    }

}
