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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.kyligence.kap.cube.model.NDataCuboid;
import io.kylingence.kap.event.handle.AddCuboidHandler;
import io.kylingence.kap.event.handle.AddSegmentHandler;
import io.kylingence.kap.event.handle.MergeSegmentHandler;
import io.kylingence.kap.event.handle.RemoveCuboidHandler;
import io.kylingence.kap.event.handle.RemoveSegmentHandler;
import io.kylingence.kap.event.model.RemoveCuboidEvent;
import io.kylingence.kap.event.model.RemoveSegmentEvent;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
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
import io.kylingence.kap.event.handle.LoadingRangeUpdateHandler;
import io.kylingence.kap.event.handle.ModelUpdateHandler;
import io.kylingence.kap.event.handle.ProjectHandler;
import io.kylingence.kap.event.manager.EventDao;
import io.kylingence.kap.event.manager.EventManager;
import io.kylingence.kap.event.manager.EventOrchestratorManager;
import io.kylingence.kap.event.model.AddProjectEvent;
import io.kylingence.kap.event.model.AddSegmentEvent;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.EventStatus;
import io.kylingence.kap.event.model.LoadingRangeUpdateEvent;
import io.kylingence.kap.event.model.MergeSegmentEvent;
import io.kylingence.kap.event.model.ModelUpdateEvent;

public class NEventFlowTest extends NLocalWithSparkSessionTest {

    public static final String DEFAULT_PROJECT = "default";
    private static final Logger logger = LoggerFactory.getLogger(NEventFlowTest.class);

    EventOrchestratorManager manager;
    EventManager eventManager;
    NDataflowManager dsMgr;

    KylinConfig config ;


    @Before
    public void setup() throws Exception {
        init();
        config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");

        manager = EventOrchestratorManager.getInstance(config);
        eventManager = EventManager.getInstance(config, DEFAULT_PROJECT);
        dsMgr= NDataflowManager.getInstance(config, DEFAULT_PROJECT);

        System.setProperty("noBuild", "false");
        System.setProperty("isDeveloperMode", "false");
        System.setProperty("kylin.job.scheduler.poll-interval-second", "30");

        new ProjectHandler();
        new AddSegmentHandler();
        new MergeSegmentHandler();
        new RemoveCuboidHandler();
        new RemoveSegmentHandler();
        new AddCuboidHandler();
        new ModelUpdateHandler();
    }

    @After
    public void after() {
        manager.destroyInstance();
        NDefaultScheduler.destroyInstance();
        cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void testEventFlow() throws Exception {
        testSegEventFlow();
        testCuboidEventFlow();
        waitForEventFinished(config);
        testMergeEventFlow();
        testRemoveEventFlow();
    }

    public void testSegEventFlow() throws Exception {

        Event event = new AddProjectEvent(DEFAULT_PROJECT);
        eventManager.post(event);

        // cleanup all segments first
        NDataflow df = dsMgr.getDataflow("all_fixed_length");
        NDataflowUpdate update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        df = dsMgr.getDataflow("ncube_basic");
        update = new NDataflowUpdate(df.getName());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);

        long start = SegmentRange.dateToLong("2010-01-01");
        long end = SegmentRange.dateToLong("2012-06-01");

        NDataLoadingRangeManager dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(),
                DEFAULT_PROJECT);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        SegmentRange.TimePartitionedDataLoadingRange range = new SegmentRange.TimePartitionedDataLoadingRange(start,
                end);
        dataLoadingRange.setDataLoadingRange(range);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment dataSegment = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(start, end));
        AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setProject(DEFAULT_PROJECT);
        addSegmentEvent.setApproved(true);
        addSegmentEvent.setModelName("nmodel_basic");
        addSegmentEvent.setCubePlanName("ncube_basic");
        addSegmentEvent.setSegmentIds(Lists.newArrayList(dataSegment.getId()));
        eventManager.post(addSegmentEvent);
        Thread.sleep(1 * 1000);

        start = SegmentRange.dateToLong("2012-06-01");
        end = SegmentRange.dateToLong("2013-01-01");
        df = dsMgr.getDataflow("ncube_basic");
        dataSegment = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(start, end));
        addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setApproved(true);
        addSegmentEvent.setProject(DEFAULT_PROJECT);
        addSegmentEvent.setModelName("nmodel_basic");
        addSegmentEvent.setCubePlanName("ncube_basic");
        addSegmentEvent.setSegmentIds(Lists.newArrayList(dataSegment.getId()));

        eventManager.post(addSegmentEvent);
        Thread.sleep(1 * 1000);


    }

    private void testMergeEventFlow() throws PersistentException, InterruptedException {
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(df.getSegments(SegmentStatusEnum.READY).size() == 2);

        Event event = new MergeSegmentEvent();
        event.setProject(DEFAULT_PROJECT);
        event.setApproved(true);
        event.setModelName("nmodel_basic");
        event.setCubePlanName("ncube_basic");
        event.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")));

        eventManager.post(event);
        waitForEventFinished(config);
        df = dsMgr.getDataflow("ncube_basic");
        Assert.assertTrue(df.getSegments(SegmentStatusEnum.READY).size() == 1);

    }

    private void testRemoveEventFlow() throws PersistentException, InterruptedException {
        NDataflow df = dsMgr.getDataflow("ncube_basic");
        NDataSegment readySeg = df.getSegments(SegmentStatusEnum.READY).get(0);
        Integer tobeRemoveSegId = readySeg.getId();
        Long tobeRemoveLayoutId = readySeg.getCuboidsMap().keySet().iterator().next();

        RemoveCuboidEvent removeCuboidEvent = new RemoveCuboidEvent();
        removeCuboidEvent.setProject(DEFAULT_PROJECT);
        removeCuboidEvent.setApproved(true);
        removeCuboidEvent.setModelName("nmodel_basic");
        removeCuboidEvent.setCubePlanName("ncube_basic");
        List<Long> layoutIds = new ArrayList<>();
        layoutIds.add(tobeRemoveLayoutId);
        removeCuboidEvent.setLayoutIds(layoutIds);
        eventManager.post(removeCuboidEvent);

        waitForEventFinished(config);
        df = dsMgr.getDataflow("ncube_basic");
        NDataCuboid cuboid = df.getSegment(tobeRemoveSegId).getCuboid(tobeRemoveLayoutId);
        Assert.assertNull(cuboid);

        RemoveSegmentEvent removeSegmentEvent = new RemoveSegmentEvent();
        removeSegmentEvent.setProject(DEFAULT_PROJECT);
        removeSegmentEvent.setApproved(true);
        removeSegmentEvent.setModelName("nmodel_basic");
        removeSegmentEvent.setCubePlanName("ncube_basic");
        List<Integer> segmentIds = new ArrayList<>();
        segmentIds.add(tobeRemoveSegId);
        removeSegmentEvent.setSegmentIds(segmentIds);
        eventManager.post(removeSegmentEvent);

        waitForEventFinished(config);
        df = dsMgr.getDataflow("ncube_basic");
        NDataSegment updatedSeg = df.getSegment(tobeRemoveSegId);
        Assert.assertNull(updatedSeg);
    }

    @Test
    @Ignore
    @SuppressWarnings("MethodLength")
    public void testLoadingRangeFlow() throws Exception {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.MockedDistributedLock$MockedFactory");
        config.setProperty("kap.storage.columnar.ii-spill-threshold-mb", "128");
        System.setProperty("noBuild", "false");
        System.setProperty("isDeveloperMode", "false");

        EventOrchestratorManager manager = EventOrchestratorManager.getInstance(config);
        manager.register(new AddSegmentHandler());
        manager.register(new LoadingRangeUpdateHandler());

        EventManager eventManager = EventManager.getInstance(config, DEFAULT_PROJECT);

        // cleanup all segments first
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
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
                DEFAULT_PROJECT);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setProject(DEFAULT_PROJECT);
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        SegmentRange.TimePartitionedDataLoadingRange range = new SegmentRange.TimePartitionedDataLoadingRange(start,
                end);
        dataLoadingRange.setDataLoadingRange(range);
        NDataLoadingRange savedDataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        LoadingRangeUpdateEvent loadingRangeUpdateEvent = new LoadingRangeUpdateEvent();
        loadingRangeUpdateEvent.setApproved(true);
        loadingRangeUpdateEvent.setProject(DEFAULT_PROJECT);
        List<String> modelNames = new ArrayList<>();
        modelNames.add("nmodel_basic");
        loadingRangeUpdateEvent.setModelNames(modelNames);
        loadingRangeUpdateEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(start, end));

        eventManager.post(loadingRangeUpdateEvent);

        waitForEventFinished(config);

        NDataLoadingRange finalDataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(tableName);

        SegmentRange.TimePartitionedDataLoadingRange range1 = (SegmentRange.TimePartitionedDataLoadingRange) finalDataLoadingRange
                .getDataLoadingRange();
        Assert.assertTrue(range1.getWaterMark().equals(end));
    }

    public void testCuboidEventFlow() throws Exception {



        EventManager eventManager = EventManager.getInstance(config, DEFAULT_PROJECT);

        // cleanup all segments first
//        NDataflowManager dsMgr = NDataflowManager.getInstance(config, DEFAULT_PROJECT);
//        NDataflow df = dsMgr.getDataflow("ncube_basic");
//        NDataflowUpdate update = new NDataflowUpdate(df.getName());
//        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
//        dsMgr.updateDataflow(update);
        ModelUpdateEvent event = new ModelUpdateEvent();
        event.setProject(DEFAULT_PROJECT);
        event.setSqlMap(new HashMap<String, Long>(){{put("select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT", 1L);}});
        event.setApproved(true);
        eventManager.post(event);

        event = new ModelUpdateEvent();
        event.setProject(DEFAULT_PROJECT);
        event.setSqlMap(new HashMap<String, Long>(){{put("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME", 2L);}});
        event.setApproved(true);
        eventManager.post(event);

        event = new ModelUpdateEvent();
        event.setProject(DEFAULT_PROJECT);
        event.setSqlMap(new HashMap<String, Long>(){{put("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME", 3L);}});
        event.setApproved(true);
        eventManager.post(event);

    }

    private void waitForEventFinished(KylinConfig config) throws PersistentException, InterruptedException {
        boolean wait = true;
        EventDao eventDao = EventDao.getInstance(config, DEFAULT_PROJECT);
        List<Event> events;
        while (wait) {
            int successEventNum = 0;
            events = eventDao.getEvents();
            for (Event event : events) {
                EventStatus status = event.getStatus();
                if (status.equals(EventStatus.ERROR)) {
                    throw new RuntimeException("Event run error : " + event.getMsg());
                } else if (status.equals(EventStatus.SUCCEED)) {
                    successEventNum++;
                } else {
                    Thread.sleep(5 * 1000);
                    break;
                }

            }
            if (successEventNum == events.size()) {
                wait = false;
            }

        }

    }

}
