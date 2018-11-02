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
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.event.handle.AddCuboidHandler;
import io.kyligence.kap.event.handle.AddSegmentHandler;
import io.kyligence.kap.event.handle.LoadingRangeRefreshHandler;
import io.kyligence.kap.event.handle.RefreshSegmentHandler;
import io.kyligence.kap.event.handle.RemoveCuboidBySqlHandler;
import io.kyligence.kap.event.model.AccelerateEvent;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.LoadingRangeRefreshEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.model.NDataLoadingRange;
import io.kyligence.kap.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.event.handle.LoadingRangeUpdateHandler;
import io.kyligence.kap.event.handle.ModelUpdateHandler;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventStatus;
import io.kyligence.kap.event.model.LoadingRangeUpdateEvent;

public class NEventFlowTest extends NLocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(NEventFlowTest.class);

    EventManager eventManager;
    NDataflowManager dsMgr;

    KylinConfig config ;

    @Override
    protected void init() throws Exception{
        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        this.createTestMetadata();
    }

    @Before
    public void setup() throws Exception {
        init();
        EventOrchestratorManager.destroyInstance();
        config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        eventManager = EventManager.getInstance(config, getProject());
        dsMgr= NDataflowManager.getInstance(config, getProject());

        System.setProperty("noBuild", "false");
        System.setProperty("isDeveloperMode", "false");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "30");

        new RemoveCuboidBySqlHandler();
        new ModelUpdateHandler();
        new LoadingRangeUpdateHandler();
        new LoadingRangeRefreshHandler();
    }

    @After
    public void after() {
        EventOrchestratorManager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    private List<DefaultChainedExecutable> genMockJobs(int size, ExecutableState state) {
        List<DefaultChainedExecutable> jobs = Lists.newArrayList();
        if (size <= 0) {
            return jobs;
        }
        for (int i = 0; i < size; i++) {
            DefaultChainedExecutable job = Mockito.spy(DefaultChainedExecutable.class);
            Mockito.doReturn(state).when(job).getStatus();
            jobs.add(job);
        }
        return jobs;
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void testEventFlow() throws Exception {
        // mock success job
        List<DefaultChainedExecutable> successJobs = genMockJobs(11, ExecutableState.SUCCEED);

        AddSegmentHandler addSegmentHandler = Mockito.spy(AddSegmentHandler.class);
        AddCuboidHandler addCuboidHandler = Mockito.spy(AddCuboidHandler.class);
        RefreshSegmentHandler refreshSegmentHandler = Mockito.spy(RefreshSegmentHandler.class);

        Mockito.doReturn(successJobs.get(0), successJobs.get(1), successJobs.get(2), successJobs.get(3)).when(addSegmentHandler).createJob(Mockito.any(EventContext.class));
        Mockito.doReturn(successJobs.get(4), successJobs.get(5), successJobs.get(6)).when(addCuboidHandler).createJob(Mockito.any(EventContext.class));
        Mockito.doReturn(successJobs.get(7), successJobs.get(8), successJobs.get(9), successJobs.get(10)).when(refreshSegmentHandler).createJob(Mockito.any(EventContext.class));

        testLoadingRangeFlow();
        testRefreshFlow();
        testCuboidEventFlow();
        testRemoveEventFlow();
        testEventErrorFlow();
    }

    private void testRefreshFlow() throws PersistentException, InterruptedException, IOException, NoSuchFieldException, IllegalAccessException {
        String tableName = "DEFAULT.TEST_KYLIN_FACT";

        NDataLoadingRange dataLoadingRange = NDataLoadingRangeManager.getInstance(config, getProject()).getDataLoadingRange(tableName);
        Class<?> clazz = dataLoadingRange.getClass();
        Field field = clazz.getDeclaredField("waterMarkEnd");
        field.setAccessible(true);
        field.set(dataLoadingRange, 0);

        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-08-01");

        LoadingRangeRefreshEvent loadingRangeRefreshEvent = new LoadingRangeRefreshEvent();
        loadingRangeRefreshEvent.setApproved(true);
        loadingRangeRefreshEvent.setProject(getProject());
        loadingRangeRefreshEvent.setTableName(tableName);
        loadingRangeRefreshEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(start, end));

        eventManager.post(loadingRangeRefreshEvent);

        waitForEventFinished(config);

        Event updatedEvent = EventDao.getInstance(config, getProject()).getEvent(loadingRangeRefreshEvent.getUuid());
        Assert.assertEquals(EventStatus.ERROR, updatedEvent.getStatus());
        Assert.assertTrue(updatedEvent.getMsg().contains("is out of range the coveredReadySegmentRange of dataLoadingRange"));

        start = SegmentRange.dateToLong("2010-01-01");
        end = SegmentRange.dateToLong("2012-06-01");

        loadingRangeRefreshEvent = new LoadingRangeRefreshEvent();
        loadingRangeRefreshEvent.setApproved(true);
        loadingRangeRefreshEvent.setProject(getProject());
        loadingRangeRefreshEvent.setTableName(tableName);
        loadingRangeRefreshEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(start, end));

        eventManager.post(loadingRangeRefreshEvent);

        waitForEventFinished(config);

        Map<Class, List<Event>> eventsMap = getEventsMap();
        List<Event> refreshSegmentEvents = eventsMap.get(RefreshSegmentEvent.class);

        Assert.assertNotNull(refreshSegmentEvents);
        Assert.assertEquals(4, refreshSegmentEvents.size());
    }

    private void testEventErrorFlow() throws PersistentException, InterruptedException {
        LoadingRangeUpdateEvent loadingRangeUpdateEvent = new LoadingRangeUpdateEvent();
        loadingRangeUpdateEvent.setApproved(true);
        loadingRangeUpdateEvent.setProject(getProject());
        loadingRangeUpdateEvent.setTableName("errorTable");
        loadingRangeUpdateEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));

        eventManager.post(loadingRangeUpdateEvent);
        waitForEventFinished(config);

        Event updatedEvent = EventDao.getInstance(config, getProject()).getEvent(loadingRangeUpdateEvent.getUuid());
        Assert.assertEquals(EventStatus.ERROR, updatedEvent.getStatus());
        Assert.assertTrue(updatedEvent.getMsg().contains("TableDesc 'errorTable' does not exist"));
    }

    private void testRemoveEventFlow() throws PersistentException, InterruptedException {
        int layoutCount = 0;
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), getProject());
        NCubePlan cubePlan1 = cubePlanManager.getCubePlan("all_fixed_length");
        layoutCount += cubePlan1.getAllCuboidLayouts().size();

        AccelerateEvent event = new AccelerateEvent();
        event.setProject(getProject());
        event.setFavoriteMark(false);
        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT"));
        event.setApproved(true);
        eventManager.post(event);


        waitForEventFinished(config);

        int newLayoutCount = 0;
        NCubePlan cubePlan3 = cubePlanManager.getCubePlan("all_fixed_length");
        newLayoutCount += cubePlan3.getAllCuboidLayouts().size();

        // the num of cuboidLayouts should be reduced by one
        Assert.assertEquals(layoutCount - 1, newLayoutCount);

    }

    public void testLoadingRangeFlow() throws Exception {

        // cleanup all segments first
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        df = dsMgr.getDataflow("all_fixed_length");
        update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        df = dsMgr.getDataflow("ncube_basic_inner");
        update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        df = dsMgr.getDataflow("ut_inner_join_cube_partial");
        update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");

        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(),
                getProject());
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(getProject());
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        SegmentRange.TimePartitionedSegmentRange range = new SegmentRange.TimePartitionedSegmentRange(start,
                end);
        dataLoadingRange.getSegmentRanges().add(range);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        LoadingRangeUpdateEvent loadingRangeUpdateEvent = new LoadingRangeUpdateEvent();
        loadingRangeUpdateEvent.setApproved(true);
        loadingRangeUpdateEvent.setProject(getProject());
        loadingRangeUpdateEvent.setTableName(tableName);
        loadingRangeUpdateEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(start, end));

        eventManager.post(loadingRangeUpdateEvent);

        waitForEventFinished(config);

        Map<Class, List<Event>> eventsMap = getEventsMap();
        List<Event> addSegmentEvents = eventsMap.get(AddSegmentEvent.class);

        Assert.assertNotNull(addSegmentEvents);
        Assert.assertEquals(4, addSegmentEvents.size());
    }

    public void testCuboidEventFlow() throws Exception {

        int layoutCount = 0;
        NCubePlanManager cubePlanManager = NCubePlanManager.getInstance(getTestConfig(), getProject());
        NCubePlan cubePlan1 = cubePlanManager.getCubePlan("all_fixed_length");
        layoutCount += cubePlan1.getAllCuboidLayouts().size();

        AccelerateEvent event = new AccelerateEvent();
        event.setProject(getProject());
        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT"));
        event.setApproved(true);
        eventManager.post(event);

        event = new AccelerateEvent();
        event.setProject(getProject());
        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME"));
        event.setApproved(true);
        eventManager.post(event);

        event = new AccelerateEvent();
        event.setProject(getProject());
        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME"));
        event.setApproved(true);
        eventManager.post(event);

        waitForEventFinished(config);

        int newLayoutCount = 0;
        NCubePlan cubePlan3 = cubePlanManager.getCubePlan("all_fixed_length");
        newLayoutCount += cubePlan3.getAllCuboidLayouts().size();

        // the num of cuboidLayouts should be added by three
        Assert.assertEquals(layoutCount + 3 , newLayoutCount);
    }

    private void waitForEventFinished(KylinConfig config) throws PersistentException, InterruptedException {
        boolean wait = true;
        EventDao eventDao = EventDao.getInstance(config, getProject());
        List<Event> events;
        while (wait) {
            int finishedEventNum = 0;
            events = eventDao.getEvents();
            for (Event event : events) {
                EventStatus status = event.getStatus();
                if (status.equals(EventStatus.SUCCEED) || status.equals(EventStatus.ERROR)) {
                    finishedEventNum++;
                } else {
                    Thread.sleep(5 * 1000);
                    break;
                }

            }
            if (finishedEventNum == events.size()) {
                wait = false;
            }

        }

    }

    public Map<Class, List<Event>> getEventsMap() throws PersistentException {
        Map<Class, List<Event>> eventsMap = Maps.newHashMap();
        List<Event> eventList = EventDao.getInstance(config, getProject()).getEvents();
        if (CollectionUtils.isEmpty(eventList)) {
            return eventsMap;
        }
        for (Event event : eventList) {
            List<Event> events = eventsMap.get(event.getClass());
            if (CollectionUtils.isEmpty(events)) {
                events = Lists.newArrayList();
                eventsMap.put(event.getClass(), events);
            }
            events.add(event);
        }

        return eventsMap;
    }
}
