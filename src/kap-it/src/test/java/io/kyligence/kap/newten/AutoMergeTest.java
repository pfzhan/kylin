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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.VolatileRange;
import io.kyligence.kap.event.handle.AddSegmentHandler;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;

public class AutoMergeTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    private void removeAllSegments() throws IOException {
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        // remove the existed seg
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
    }

    private void mockAddSegmentSuccess()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Class clazz = AddSegmentHandler.class;
        Method method = clazz.getDeclaredMethod("onJobSuccess", EventContext.class);
        method.setAccessible(true);
        Event addEvent = new AddSegmentEvent();
        addEvent.setProject(DEFAULT_PROJECT);
        addEvent.setCubePlanName("ncube_basic");
        addEvent.setModelName("nmodel_basic");
        EventContext eventContext = new EventContext(addEvent, getTestConfig());
        method.invoke(new AddSegmentHandler(), eventContext);
    }

    private void createDataloadingRange() throws IOException {
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        dataLoadingRange.setColumnName("TEST_KYLIN_FACT.CAL_DT");
        NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT).createDataLoadingRange(dataLoadingRange);
    }

    @Test
    public void testAutoMergeSegmentsByWeek_FridayAndSaturday_NotMerge() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //two days,not enough for a week ,not merge
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        //clear all events
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(0, events.size());
    }

    @Test
    public void testAutoMergeSegmentsByWeek_WithoutVolatileRange_Merge() throws Exception {
        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 4 days ,2010/01/01 8:00 - 2010/01/04 8:00， friday to monday, merge
        for (int i = 0; i <= 3; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //clear all events
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        //merge 2010/01/01 00:00 - 2010/01/04 00:00
        Assert.assertEquals(1262304000000L, events.get(0).getSegmentRange().getStart());
        Assert.assertEquals(1262563200000L, events.get(0).getSegmentRange().getEnd());

    }

    @Test
    public void testAutoMergeSegmentsByWeek_WithThreeDaysVolatileRange_MergeFirstWeek() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 9 days ,2010/01/01 - 2010/01/10， volatileRange 3 days
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        //set 3days volatile ,and just merge the first week
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        VolatileRange volatileRange = new VolatileRange();
        volatileRange.setVolatileRangeNumber(3);
        volatileRange.setVolatileRangeEnabled(true);
        volatileRange.setVolatileRangeType(AutoMergeTimeEnum.DAY);
        modelUpdate.setVolatileRange(volatileRange);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(1262304000000L, events.get(0).getSegmentRange().getStart());
        Assert.assertEquals(1262563200000L, events.get(0).getSegmentRange().getEnd());
    }

    @Test
    public void testAutoMergeSegmentsByWeek_SegmentsHasOneDayGap_MergeSecondWeek() throws Exception {

        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 week,and the first week has gap segment
        //test 9 days ,2010/01/01 00:00 - 2010/01/10 00:00，remove 2010/01/02,merge the second week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        df = dataflowManager.getDataflow("ncube_basic");
        update = new NDataflowUpdate(df.getName());
        //remove 2010-01-02
        update.setToRemoveSegs(new NDataSegment[] { df.getSegment(1) });
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(events.size(), 1);
        start = Long.parseLong(events.get(0).getSegmentRange().getStart().toString());
        end = Long.parseLong(events.get(0).getSegmentRange().getEnd().toString());
        Assert.assertEquals(1262563200000L, start);
        Assert.assertEquals(1263168000000L, end);
    }

    @Test
    public void testAutoMergeSegmentsByWeek_WhenSegmentsContainBuildingSegment() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 week,and the first week has building segment
        //test 9 days ,2010/01/01 00:00 - 2010/01/10 00:00， 2010/01/02 building,merge the second week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            if (i != 1) {
                dataSegment.setStatus(SegmentStatusEnum.READY);
            }
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(events.size(), 1);
        start = Long.parseLong(events.get(0).getSegmentRange().getStart().toString());
        end = Long.parseLong(events.get(0).getSegmentRange().getEnd().toString());
        Assert.assertEquals(1262563200000L, start);
        Assert.assertEquals(1263168000000L, end);
    }

    @Test
    public void testAutoMergeSegmentsByWeek_HasBigSegment_Merge() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 days and a big segment,merge the first week
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-01") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-02") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            NDataflow df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        //a big segment
        start = 1262476800000L;
        end = 1262476800000L + 86400000 * 8;
        SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
        dataSegment.setStatus(SegmentStatusEnum.READY);
        dataSegment.setId(2);
        segments.add(dataSegment);

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(events.size(), 1);
        start = Long.parseLong(events.get(0).getSegmentRange().getStart().toString());
        end = Long.parseLong(events.get(0).getSegmentRange().getEnd().toString());
        Assert.assertEquals(1262304000000L, start);
        Assert.assertEquals(1262476800000L, end);
    }

    @Test
    public void testAutoMergeSegmentsByWeek_FirstDayOfWeekWSunday_Merge() throws Exception {
        getTestConfig().setProperty("kylin.metadata.first-day-of-week", "sunday");
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //test 2 days and a big segment,merge the first week
        for (int i = 0; i <= 9; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-03") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-04") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(events.size(), 1);
        start = Long.parseLong(events.get(0).getSegmentRange().getStart().toString());
        end = Long.parseLong(events.get(0).getSegmentRange().getEnd().toString());
        //2010/1/3 sunday - 2010/1/10 sunday
        Assert.assertEquals(1262476800000L, start);
        Assert.assertEquals(1263081600000L, end);
    }

    @Test
    public void testAutoMergeSegmentsByHour_PASS() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //13min per segment
        for (int i = 0; i <= 5; i++) {
            //01-01 00:00 - 01:05 merge one hour
            start = SegmentRange.dateToLong("2010-01-01") + i * 1000 * 60 * 13;
            end = SegmentRange.dateToLong("2010-01-01") + (i + 1) * 1000 * 60 * 13;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> ranges = new ArrayList<>();
        ranges.add(AutoMergeTimeEnum.HOUR);
        modelUpdate.setAutoMergeTimeRanges(ranges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(1262304000000L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        Assert.assertEquals(1262307120000L, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));

    }

    @Test
    public void testAutoMergeSegmentsByMonth() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> ranges = new ArrayList<>();
        ranges.add(AutoMergeTimeEnum.MONTH);
        modelUpdate.setAutoMergeTimeRanges(ranges);
        dataModelManager.updateDataModelDesc(modelUpdate);

        //4week segment ,and four one day segment ,2010/12/01 - 2011/1/02
        for (int i = 0; i <= 3; i++) {
            start = SegmentRange.dateToLong("2010-12-01") + i * 604800000;
            end = SegmentRange.dateToLong("2010-12-08") + i * 604800000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        for (int i = 0; i <= 3; i++) {
            start = SegmentRange.dateToLong("2010-12-29") + i * 86400000;
            end = SegmentRange.dateToLong("2010-12-30") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i + 4);
            segments.add(dataSegment);
        }
        update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);
        //clear all events
        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(1291161600000L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        Assert.assertEquals(1293840000000L, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));

    }

    @Test
    public void testAutoMergeSegmentsByYear() throws Exception {
        removeAllSegments();
        createDataloadingRange();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");
        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10月 -2011/2月 merge 2010/10-2010/12
        for (int i = 0; i <= 4; i++) {
            start = SegmentRange.dateToLong("2010-10-01") + i * 2592000000L;
            end = SegmentRange.dateToLong("2010-10-31") + i * 2592000000L;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.YEAR);
        modelUpdate.setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();
        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(1285891200000L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        Assert.assertEquals(1293667200000L, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));

    }

    @Test
    public void testAutoMergeSegmentsByDay() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/01 8:00 - 2010/10/02 06:15
        for (int i = 0; i <= 9; i++) {
            start = SegmentRange.dateToLong("2010-10-01 08:00:00") + i * 8100000L;
            end = start + 8100000L;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.DAY);
        modelUpdate.setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();

        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(1, events.size());
        Assert.assertEquals(1285920000000L, Long.parseLong(events.get(0).getSegmentRange().getStart().toString()));
        Assert.assertEquals(1285976700000L, Long.parseLong(events.get(0).getSegmentRange().getEnd().toString()));

    }

    @Test
    public void testAutoMergeSegmentsByWeek_BigGapOverlapTwoSection_NotMerge() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/04 2010/10/05
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-04") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-05") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        //2010/10/11 2010/10/12
        for (int i = 2; i <= 3; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-09") + i * 86400000;
            end = SegmentRange.dateToLong("2010-01-10") + i * 86400000;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        List<AutoMergeTimeEnum> timeRanges = new ArrayList<>();
        timeRanges.add(AutoMergeTimeEnum.WEEK);
        modelUpdate.setAutoMergeTimeRanges(timeRanges);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();

        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(0, events.size());

    }

    @Test
    public void testAutoMergeSegmentsByWeek_FirstWeekNoSegment_NotMerge() throws Exception {
        removeAllSegments();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataModelManager dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        NDataflow df = dataflowManager.getDataflow("ncube_basic");
        NDataModel model = dataModelManager.getDataModelDesc("nmodel_basic");

        List<NDataSegment> segments = new ArrayList<>();
        long start;
        long end;
        //2010/10/04 2010/10/05
        for (int i = 0; i <= 1; i++) {
            //01-01 friday
            start = SegmentRange.dateToLong("2010-01-03") + i * 86400000 * 2;
            end = SegmentRange.dateToLong("2010-01-05") + i * 86400000 * 2;
            SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
            df = dataflowManager.getDataflow("ncube_basic");
            NDataSegment dataSegment = dataflowManager.appendSegment(df, segmentRange);
            dataSegment.setStatus(SegmentStatusEnum.READY);
            dataSegment.setId(i);
            segments.add(dataSegment);
        }

        NDataModel modelUpdate = dataModelManager.copyForWrite(model);
        modelUpdate.setManagementType(ManagementType.MODEL_BASED);
        dataModelManager.updateDataModelDesc(modelUpdate);
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToUpdateSegs(segments.toArray(new NDataSegment[segments.size()]));
        dataflowManager.updateDataflow(update);

        EventDao eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventDao.deleteAllEvents();

        mockAddSegmentSuccess();
        List<Event> events = eventDao.getEvents();
        Assert.assertEquals(0, events.size());

    }

}
