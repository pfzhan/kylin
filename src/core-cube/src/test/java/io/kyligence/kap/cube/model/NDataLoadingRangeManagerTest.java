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

import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
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
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        // test error case, add a segRange with has the overlaps/gap
        long start = 1536813121000L;
        long end = 1536813191000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        savedDataLoadingRange = dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range);
        start = 0L;
        end = 1005277100000L;
        SegmentRange.TimePartitionedSegmentRange range1 = new SegmentRange.TimePartitionedSegmentRange(start, end);
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("has overlaps/gap with existing segmentRanges");
        dataLoadingRangeManager.appendSegmentRange(savedDataLoadingRange, range1);
    }

    @Test
    public void testCreateAndUpdateDataLoadingRange() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        long start = 1505277121000L;
        long end = 1536813121000L;
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
    }


    @Test
    public void testCreateDataLoadingRange_StringColumn() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LSTG_FORMAT_NAME";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        long start = 1505277121000L;
        long end = 1536813121000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        Assert.assertTrue(savedDataLoadingRange.getColumnName().equals(columnName));
    }

    @Test
    public void testCreateDataLoadingRange_IntegerColumn() throws IOException {

        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LEAF_CATEG_ID";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setPartitionDateFormat("YYYY");
        dataLoadingRange.setColumnName(columnName);
        long start = 1505277121000L;
        long end = 1536813121000L;
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
        Assert.assertTrue(savedDataLoadingRange.getProject().equals(DEFAULT_PROJECT));
        Assert.assertTrue(savedDataLoadingRange.getColumnName().equals(columnName));
    }

    @Test
    public void testGetSegRangeToBuildForNewDataflow_HasCandidateModel() {
        long start = 1293194019000L;
        long end = 1325680419000L;

        val loadingRange = createDataLoadingRange(start, end);

        removeAllSegments();
        val segments = new Segments<NDataSegment>();


        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        start = 1293194019000L;
        end = 1305680419000L;
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);


        start = 1305680419000L;
        end = 1325680419000L;
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals("1293194019000", ranges.get(0).getStart().toString());
        Assert.assertEquals("1305680419000", ranges.get(0).getEnd().toString());
        Assert.assertEquals("1305680419000", ranges.get(1).getStart().toString());
        Assert.assertEquals("1325680419000", ranges.get(1).getEnd().toString());
    }


    @Test
    public void testGetSegRangeToBuildForNewDataflow_MonthAndWeek() {
        //2012/12/25-2013/01/15
        long start = 1356416834000L;
        long end = 1358231234000L;
        val loadingRange = createDataLoadingRange(start, end);
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(4, ranges.size());
        //12/12/25-13/01/01 00:00
        Assert.assertEquals("1356416834000", ranges.get(0).getStart().toString());
        Assert.assertEquals("1356998400000", ranges.get(0).getEnd().toString());
        //13/01/01 00:00 - 13/01/07
        Assert.assertEquals("1356998400000", ranges.get(1).getStart().toString());
        Assert.assertEquals("1357516800000", ranges.get(1).getEnd().toString());
        //13/01/07 00:00 - 13/01/14
        Assert.assertEquals("1357516800000", ranges.get(2).getStart().toString());
        Assert.assertEquals("1358121600000", ranges.get(2).getEnd().toString());

        //13/01/14 00:00 - 13/01/15
        Assert.assertEquals("1358121600000", ranges.get(3).getStart().toString());
        Assert.assertEquals("1358231234000", ranges.get(3).getEnd().toString());
    }

    @Test
    public void testGetSegRangeToBuildForNewDataflow_3DaysVolatile() {
        //2013/01/01-2013/01/15
        long start = 1356998400000L;
        long end = 1358231234000L;

        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().getVolatileRange().setVolatileRangeNumber(3);
        copy.getSegmentConfig().getVolatileRange().setVolatileRangeEnabled(true);

        prjManager.updateProject(copy);

        val loadingRange = createDataLoadingRange(start, end);
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(5, ranges.size());
        //13/01/01 00:00 - 13/01/07
        Assert.assertEquals("1356998400000", ranges.get(0).getStart().toString());
        Assert.assertEquals("1357516800000", ranges.get(0).getEnd().toString());
        //13/01/07 00:00 - 13/01/12
        Assert.assertEquals("1357516800000", ranges.get(1).getStart().toString());
        Assert.assertEquals("1357972034000", ranges.get(1).getEnd().toString());

        //13/01/12 00:00 - 13/01/13
        Assert.assertEquals("1357972034000", ranges.get(2).getStart().toString());
        Assert.assertEquals("1358058434000", ranges.get(2).getEnd().toString());

        //13/01/13 00:00 - 13/01/14
        Assert.assertEquals("1358058434000", ranges.get(3).getStart().toString());
        Assert.assertEquals("1358144834000", ranges.get(3).getEnd().toString());

        //13/01/14 00:00 - 13/01/15
        Assert.assertEquals("1358144834000", ranges.get(4).getStart().toString());
        Assert.assertEquals("1358231234000", ranges.get(4).getEnd().toString());

    }


    @Test
    public void testGetSegRangeToBuildForNewDataflow_YearMonthAndWeek() {
        //2010/12/24-2012/01/04
        long start = 1293194019000L;
        long end = 1325680419000L;
        val loadingRange = createDataLoadingRange(start, end);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        val prj = prjManager.getProject("default");
        val copy = prjManager.copyForWrite(prj);
        copy.getSegmentConfig().getAutoMergeTimeRanges().add(AutoMergeTimeEnum.YEAR);
        prjManager.updateProject(copy);
        val ranges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(loadingRange);
        Assert.assertEquals(4, ranges.size());
        //10/12/24 00:00 - 11/01/01
        Assert.assertEquals("1293194019000", ranges.get(0).getStart().toString());
        Assert.assertEquals("1293840000000", ranges.get(0).getEnd().toString());
        //11/01/01 00:00 - 12/01/01
        Assert.assertEquals("1293840000000", ranges.get(1).getStart().toString());
        Assert.assertEquals("1325376000000", ranges.get(1).getEnd().toString());

        //12/01/01 00:00 - 12/01/02
        Assert.assertEquals("1325376000000", ranges.get(2).getStart().toString());
        Assert.assertEquals("1325462400000", ranges.get(2).getEnd().toString());

        //12/01/02 00:00 - 12/01/04
        Assert.assertEquals("1325462400000", ranges.get(3).getStart().toString());
        Assert.assertEquals("1325680419000", ranges.get(3).getEnd().toString());
    }


    private NDataLoadingRange createDataLoadingRange(long start, long end) {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.LEAF_CATEG_ID";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start, end);
        dataLoadingRange.setCoveredRange(range);
        return dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);
    }

    @Test
    public void testGetQuerableSegmentRange_NoModel() {
        long start = 1293194019000L;
        long end = 1325680419000L;
        val loadingRange = createDataLoadingRange(start, end);
        loadingRange.setTableName("DEFAULT.TEST_ACCOUNT");
        val range = dataLoadingRangeManager.getQuerableSegmentRange(loadingRange);
        Assert.assertEquals("1293194019000", range.getStart().toString());
        Assert.assertEquals("1325680419000", range.getEnd().toString());
    }

    @Test
    public void testGetQuerableSegmentRange_HasModels() {
        long start = 1293194019000L;
        long end = 1325680419000L;
        removeAllSegments();
        val segments = new Segments<NDataSegment>();
        val segments2 = new Segments<NDataSegment>();


        val loadingRange = createDataLoadingRange(start, end);
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        start = 1293194019000L;
        end = 1305680419000L;
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments2.add(dataSegment);

        start = 1305680419000L;
        end = 1325680419000L;
        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        segments.add(dataSegment);

        segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.NEW);
        segments2.add(dataSegment);

        NDataflowUpdate update = new NDataflowUpdate(dataflowManager.getDataflowByModelAlias("nmodel_basic").getUuid());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        update = new NDataflowUpdate(dataflowManager.getDataflowByModelAlias("nmodel_basic_inner").getUuid());
        update.setToUpdateSegs(segments2.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);


        val range = dataLoadingRangeManager.getQuerableSegmentRange(loadingRange);
        Assert.assertEquals("1293194019000", range.getStart().toString());
        Assert.assertEquals("1305680419000", range.getEnd().toString());

        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        val segs = df.getQuerableSegments();
        Assert.assertEquals(1, segs.size());

        Assert.assertEquals("1293194019000", segs.get(0).getSegRange().getStart().toString());
        Assert.assertEquals("1305680419000", segs.get(0).getSegRange().getEnd().toString());


    }

    private void removeAllSegments() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflowByModelAlias("nmodel_basic_inner");
        // remove the existed seg
        update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }
}
