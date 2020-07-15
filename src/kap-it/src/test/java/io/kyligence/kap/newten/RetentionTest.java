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
package io.kyligence.kap.newten;

import java.io.IOException;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.junit.TimeZoneTestRunner;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.RetentionRange;
import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class RetentionTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private void removeAllSegments() {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private void mockAddSegmentSuccess() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataflowManager.handleRetention(df);
    }

    private NDataLoadingRange createDataloadingRange() throws IOException {
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        return NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT)
                .createDataLoadingRange(dataLoadingRange);
    }

    @Test
    public void testRetention_2Week() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //two days,not enough for a week
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000L;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000L;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        //no retention
        Assert.assertEquals(2, df.getSegments().size());
    }

    @Test
    public void testRetention_2Week_3WeekDataCornerCase() throws Exception {
        removeAllSegments();
        val loadingRange = createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //3 week data last period is full week
        for (int i = 0; i <= 2; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        dataModelManager.updateDataModelDesc(modelUpdate);

        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val copy = dataLoadingRangeManager.copyForWrite(loadingRange);
        copy.setCoveredRange(df.getCoveredRange());
        dataLoadingRangeManager.updateDataLoadingRange(copy);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(2, df.getSegments().size());
        //01-11
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //01-18
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-25 00:00:00"),
                df.getSegments().get(1).getSegRange().getEnd());

        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(loadingRange.getTableName());

        Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"),
                dataLoadingRange.getCoveredRange().getStart());

        Assert.assertEquals(DateFormat.stringToMillis("2010-01-25 00:00:00"),
                dataLoadingRange.getCoveredRange().getEnd());

    }

    @Test
    public void testRetention_2Week_3WeekAndOneDayData() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //3 week data last period is full week
        for (int i = 0; i <= 2; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        //one more day
        start = SegmentRange.dateToLong("2010-01-25");
        end = SegmentRange.dateToLong("2010-01-26");
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        dataflowManager.appendSegment(df, segmentRange);

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(2);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.WEEK);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");

        Assert.assertEquals(3, df.getSegments().size());
        //01/11
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-11 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //01/26
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-26 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());
    }

    @Test
    public void testRetention_1Month_9WeekData() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //5 week data last period is not full month
        for (int i = 0; i <= 8; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(1);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.MONTH);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        //retention
        Assert.assertEquals(4, df.getSegments().size());
        //02/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-02-08 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //03/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-03-08 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());

    }

    @Test
    public void testRetention_1Month_5WeekData() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val model = dataModelManager.getDataModelDescByAlias("nmodel_basic");
        NDataflow df;
        long start;
        long end;
        //5 week data last period is not full month
        for (int i = 0; i <= 4; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000L * 7;
            end = SegmentRange.dateToLong("2010-01-11") + i * 86400000L * 7;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
            dataflowManager.appendSegment(df, segmentRange);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        val retentionRange = new RetentionRange();
        retentionRange.setRetentionRangeEnabled(true);
        retentionRange.setRetentionRangeNumber(1);
        retentionRange.setRetentionRangeType(AutoMergeTimeEnum.MONTH);
        modelUpdate.getSegmentConfig().setRetentionRange(retentionRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        mockAddSegmentSuccess();
        df = dataflowManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(5, df.getSegments().size());
        //01/04
        Assert.assertEquals(DateFormat.stringToMillis("2010-01-04 00:00:00"),
                df.getSegments().get(0).getSegRange().getStart());
        //02/08
        Assert.assertEquals(DateFormat.stringToMillis("2010-02-08 00:00:00"),
                df.getSegments().getLastSegment().getSegRange().getEnd());
    }

}
