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
package io.kyligence.kap.event.manager;

import static org.awaitility.Awaitility.await;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.JobRelatedEvent;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class EventOrchestratorTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    private static EventOrchestrator eventOrchestrator;
    private static EventOrchestrator.EventChecker eventChecker;
    private static NExecutableManager manager;
    private static EventDao eventDao;
    private static EventManager eventManager;

    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {
        this.createTestMetadata();
        System.setProperty("kylin.job.event.poll-interval-second", "1");
        eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());
        eventChecker = eventOrchestrator.new EventChecker();
        manager = Mockito.spy(NExecutableManager.getInstance(getTestConfig(), DEFAULT_PROJECT));
        Field filed = getTestConfig().getClass().getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed
                .get(getTestConfig());
        managersByPrjCache.get(NExecutableManager.class).put(DEFAULT_PROJECT, manager);
        eventDao = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT);
        eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
        eventOrchestrator.forceShutdown();
    }

    @Test
    public void testChooseEventForeachModelWhenExistErrorJob(){
        List<Event> events = initEvents();

        Map<String, List<String>> modelExecutables = initModelExecutables(events);
        Mockito.doReturn(modelExecutables).when(manager).getModelExecutables(Mockito.anySet(), Mockito.any());

        Map<String, Event> modelEvents = eventChecker.chooseEventForeachModel(events);

        Assert.assertEquals(1, modelEvents.size());
        Assert.assertEquals(events.get(6), modelEvents.values().iterator().next());

    }

    @Test
    public void testDeleteEventWhenFetchEvent() {
        List<Event> events = initEvents();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val modelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
            modelManager.dropModel(modelManager.getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa"));
            return null;
        }, DEFAULT_PROJECT);
        events.forEach(eventManager::post);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0, eventDao.getEvents().size());
        });
    }

    @Test
    public void testHandleEventErrorOnExpertMode() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(DEFAULT_PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstanceUpdate);
        List<Event> events = initEvents();
        events.forEach(event -> event.setRunTimes(6));
        events.forEach(eventManager::post);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(
                RealizationStatusEnum.BROKEN,
                NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataflow(modelId).getStatus()));
    }

    @Test
    public void testHandleEventErrorOnSmartMode() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        List<Event> events = initEvents();
        events.forEach(event -> event.setRunTimes(6));
        events.forEach(eventManager::post);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert
                .assertNull(NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataflow(modelId)));
    }

    private Map<String, List<String>> initModelExecutables(List<Event> events) {
        Map<String, List<String>> map = Maps.newHashMap();
        Event first = events.get(0);
        map.put(first.getModelId(), Lists.newArrayList(((JobRelatedEvent) first).getJobId()));
        return map;
    }

    private List<Event> initEvents() {
        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        List<Event> initEvents = Lists.newArrayList();

        val addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setJobId(UUID.randomUUID().toString());
        addSegmentEvent.setModelId(modelId);
        initEvents.add(addSegmentEvent);

        val postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
        postAddSegmentEvent.setModelId(modelId);
        initEvents.add(postAddSegmentEvent);

        val addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setJobId(UUID.randomUUID().toString());
        addCuboidEvent.setModelId(modelId);
        initEvents.add(addCuboidEvent);

        val postAddCuboidEvent = new PostAddCuboidEvent();
        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
        postAddCuboidEvent.setModelId(modelId);
        initEvents.add(postAddCuboidEvent);

        val mergeSegmentEvent = new MergeSegmentEvent();
        mergeSegmentEvent.setJobId(UUID.randomUUID().toString());
        mergeSegmentEvent.setModelId(modelId);
        initEvents.add(mergeSegmentEvent);

        val postMergeSegmentEvent = new PostMergeOrRefreshSegmentEvent();
        postMergeSegmentEvent.setJobId(mergeSegmentEvent.getJobId());
        postMergeSegmentEvent.setModelId(modelId);
        initEvents.add(postMergeSegmentEvent);

        val addSegmentEvent2 = new AddSegmentEvent();
        addSegmentEvent2.setJobId(UUID.randomUUID().toString());
        addSegmentEvent2.setModelId(modelId);
        initEvents.add(addSegmentEvent2);

        val postAddCuboidEvent2 = new PostAddCuboidEvent();
        postAddCuboidEvent2.setJobId(addSegmentEvent2.getJobId());
        postAddCuboidEvent2.setModelId(modelId);
        initEvents.add(postAddCuboidEvent2);

        return initEvents;
    }
}
