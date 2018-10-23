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
package io.kylingence.kap.event.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kylingence.kap.event.model.AddCuboidEvent;
import io.kylingence.kap.event.model.AddSegmentEvent;
import io.kylingence.kap.event.model.Event;
import io.kylingence.kap.event.model.LoadingRangeUpdateEvent;
import io.kylingence.kap.event.model.MergeSegmentEvent;
import io.kylingence.kap.event.model.ModelUpdateEvent;
import io.kylingence.kap.event.model.RemoveCuboidBySqlEvent;
import io.kylingence.kap.event.model.RemoveSegmentEvent;

public class EventOrchestratorTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testSectionalizeEvents() throws InterruptedException {
        getTestConfig().setProperty("kylin.server.mode", "query");
        EventOrchestrator eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());
        EventOrchestrator.FetcherRunner fetcherRunner = eventOrchestrator.new FetcherRunner();
        List<Event> events = initEvents();
        
        Map<String, List<EventOrchestrator.EventSetManager>> eventsToBeProcessed = fetcherRunner.sectionalizeEvents(events);
        Assert.assertEquals(eventsToBeProcessed.size(), 3);

        List<EventOrchestrator.EventSetManager> defaultEventSetManagers = eventsToBeProcessed.get(EventOrchestrator.DEFAULT_GROUP);
        Assert.assertEquals(defaultEventSetManagers.size(), 4);

        List<EventOrchestrator.EventSetManager> oneEventSetManagers = eventsToBeProcessed.get("nmodel_basic_ncube_basic");
        Assert.assertEquals(oneEventSetManagers.size(), 7);
        Assert.assertEquals(oneEventSetManagers.get(0).getEvents().size(), 2);
        Assert.assertEquals(oneEventSetManagers.get(1).getEvents().size(), 1);
        Assert.assertEquals(oneEventSetManagers.get(oneEventSetManagers.size() - 1).getEvents().size(), 2);

        List<EventOrchestrator.EventSetManager> secondEventSetManagers = eventsToBeProcessed.get("nmodel_basic_all_fixed_length");
        Assert.assertEquals(secondEventSetManagers.size(), 1);

        getTestConfig().setProperty("kylin.server.mode", "all");
    }

    private List<Event> initEvents() throws InterruptedException {
        List<Event> initEvents = Lists.newArrayList();
        LoadingRangeUpdateEvent loadingRangeUpdateEvent = new LoadingRangeUpdateEvent();
        loadingRangeUpdateEvent.setApproved(true);
        loadingRangeUpdateEvent.setProject(DEFAULT_PROJECT);
        loadingRangeUpdateEvent.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRangeUpdateEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, Long.MAX_VALUE));
        initEvents.add(loadingRangeUpdateEvent);

        Thread.sleep(10);
        AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setProject(DEFAULT_PROJECT);
        addSegmentEvent.setApproved(true);
        addSegmentEvent.setModelName("nmodel_basic");
        addSegmentEvent.setCubePlanName("ncube_basic");
        addSegmentEvent.setSegmentIds(Lists.<Integer>newArrayList());
        initEvents.add(addSegmentEvent);

        Thread.sleep(10);
        addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setProject(DEFAULT_PROJECT);
        addSegmentEvent.setApproved(true);
        addSegmentEvent.setModelName("nmodel_basic");
        addSegmentEvent.setCubePlanName("all_fixed_length");
        addSegmentEvent.setSegmentIds(Lists.<Integer>newArrayList());
        initEvents.add(addSegmentEvent);

        Thread.sleep(10);
        addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setProject(DEFAULT_PROJECT);
        addSegmentEvent.setApproved(true);
        addSegmentEvent.setModelName("nmodel_basic");
        addSegmentEvent.setCubePlanName("ncube_basic");
        addSegmentEvent.setSegmentIds(Lists.<Integer>newArrayList());
        initEvents.add(addSegmentEvent);

        Thread.sleep(10);
        ModelUpdateEvent modelUpdateEvent = new ModelUpdateEvent();
        modelUpdateEvent.setProject(DEFAULT_PROJECT);
        modelUpdateEvent.setSqlMap(new HashMap<String, String>(){{put("select CAL_DT, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT", "1");}});
        modelUpdateEvent.setApproved(true);
        initEvents.add(modelUpdateEvent);

        Thread.sleep(10);
        modelUpdateEvent = new ModelUpdateEvent();
        modelUpdateEvent.setProject(DEFAULT_PROJECT);
        modelUpdateEvent.setSqlMap(new HashMap<String, String>(){{put("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME", "2");}});
        modelUpdateEvent.setApproved(true);
        initEvents.add(modelUpdateEvent);

        Thread.sleep(10);
        modelUpdateEvent = new ModelUpdateEvent();
        modelUpdateEvent.setProject(DEFAULT_PROJECT);
        modelUpdateEvent.setSqlMap(new HashMap<String, String>(){{put("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME", "3");}});
        modelUpdateEvent.setApproved(true);
        initEvents.add(modelUpdateEvent);

        Thread.sleep(10);
        MergeSegmentEvent mergeSegmentEvent = new MergeSegmentEvent();
        mergeSegmentEvent.setProject(DEFAULT_PROJECT);
        mergeSegmentEvent.setApproved(true);
        mergeSegmentEvent.setModelName("nmodel_basic");
        mergeSegmentEvent.setCubePlanName("ncube_basic");
        mergeSegmentEvent.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2010-01-01"),
                SegmentRange.dateToLong("2013-01-01")));
        initEvents.add(mergeSegmentEvent);

        Thread.sleep(10);
        RemoveSegmentEvent removeSegmentEvent = new RemoveSegmentEvent();
        removeSegmentEvent.setProject(DEFAULT_PROJECT);
        removeSegmentEvent.setApproved(true);
        removeSegmentEvent.setModelName("nmodel_basic");
        removeSegmentEvent.setCubePlanName("ncube_basic");
        List<Integer> segmentIds = new ArrayList<>();
        removeSegmentEvent.setSegmentIds(segmentIds);
        initEvents.add(removeSegmentEvent);

        Thread.sleep(10);
        RemoveCuboidBySqlEvent removeCuboidEvent = new RemoveCuboidBySqlEvent();
        removeCuboidEvent.setSqlList(Lists.<String>newArrayList());
        removeCuboidEvent.setApproved(true);
        removeCuboidEvent.setProject(DEFAULT_PROJECT);
        removeCuboidEvent.setModelName("nmodel_basic");
        removeCuboidEvent.setCubePlanName("ncube_basic");
        initEvents.add(removeCuboidEvent);

        Thread.sleep(10);
        AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setApproved(true);
        addCuboidEvent.setProject(DEFAULT_PROJECT);
        addCuboidEvent.setModelName("nmodel_basic");
        addCuboidEvent.setCubePlanName("ncube_basic");
        initEvents.add(addCuboidEvent);

        Thread.sleep(10);
        removeCuboidEvent = new RemoveCuboidBySqlEvent();
        removeCuboidEvent.setSqlList(Lists.<String>newArrayList());
        removeCuboidEvent.setApproved(true);
        removeCuboidEvent.setProject(DEFAULT_PROJECT);
        removeCuboidEvent.setModelName("nmodel_basic");
        removeCuboidEvent.setCubePlanName("ncube_basic");
        initEvents.add(removeCuboidEvent);


        Thread.sleep(10);
        addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setApproved(true);
        addCuboidEvent.setProject(DEFAULT_PROJECT);
        addCuboidEvent.setModelName("nmodel_basic");
        addCuboidEvent.setCubePlanName("ncube_basic");
        initEvents.add(addCuboidEvent);

        Thread.sleep(10);
        addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setApproved(true);
        addCuboidEvent.setProject(DEFAULT_PROJECT);
        addCuboidEvent.setModelName("nmodel_basic");
        addCuboidEvent.setCubePlanName("ncube_basic");
        initEvents.add(addCuboidEvent);


        return initEvents;
    }
}
