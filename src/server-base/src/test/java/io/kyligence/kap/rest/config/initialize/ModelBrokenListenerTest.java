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

package io.kyligence.kap.rest.config.initialize;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.manager.EventOrchestrator;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.event.model.PostMergeOrRefreshSegmentEvent;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.AclTCRService;
import io.kyligence.kap.rest.service.CSVSourceTestCase;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;
import lombok.var;

public class ModelBrokenListenerTest extends CSVSourceTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ModelBrokenListenerTest.class);

    private static final String DEFAULT_PROJECT = "default";

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Mock
    private TableService tableService = Mockito.spy(TableService.class);

    @Mock
    private AclTCRService aclTCRService = Mockito.spy(AclTCRService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private TableExtService tableExtService = Mockito.spy(new TableExtService());

    private EventOrchestrator eventOrchestrator;

    @Before
    public void setup() {
        logger.info("ModelBrokenListenerTest setup");
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("kylin.job.event.poll-interval-second", "3");
        super.setup();
        SchedulerEventBusFactory.getInstance(getTestConfig()).register(modelBrokenListener);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
    }

    @After
    public void cleanup() {
        logger.info("ModelBrokenListenerTest cleanup");
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(modelBrokenListener);
        SchedulerEventBusFactory.restart();
        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
        super.cleanup();
    }

    private void generateEvents(String modelId, String project) {
        val eventManager = EventManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setModelId(modelId);
        addCuboidEvent.setJobId(UUID.randomUUID().toString());
        addCuboidEvent.setOwner("ADMIN");
        eventManager.post(addCuboidEvent);

        PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
        postAddCuboidEvent.setModelId(modelId);
        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
        postAddCuboidEvent.setOwner("ADMIN");
        eventManager.post(postAddCuboidEvent);
        Assert.assertEquals(2, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
    }

    @Test
    public void testModelBrokenListener_DropModel() {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_DropModel");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateEvents(project, modelId);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        System.setProperty("kylin.metadata.broken-model-deleted-on-smart-mode", "true");

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(modelManager.getDataModelDesc(modelId));
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });

        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
    }

    @Test
    public void testModelBrokenListener_TableOriented() throws Exception {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_TableOriented");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateEvents(project, modelId);
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());

        });
    }

    @Test
    public void testModelBrokenListener_BrokenReason() throws Exception {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_BrokenReason");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setBrokenReason(NDataModel.BrokenReason.EVENT);
        modelManager.updateDataModelDesc(copyForUpdate);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });
    }

    @Test
    public void testModelBrokenListener_FullBuild() throws Exception {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_FullBuild");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setPartitionDesc(null);
        modelManager.updateDataModelDesc(copyForUpdate);

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertTrue(dataflow.getCoveredRange().isInfinite());
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });
    }

    @Test
    public void testEventError() throws InterruptedException {
        logger.info("ModelBrokenListenerTest testEventError");

        eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());

        val projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(DEFAULT_PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstanceUpdate);

        val dfManager = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);

        val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), DEFAULT_PROJECT);

        ErrorEvent event = new ErrorEvent();
        event.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event.setOwner("ADMIN");

        ErrorEvent event2 = new ErrorEvent();
        event2.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event2.setOwner("ADMIN");

        ErrorEvent event3 = new ErrorEvent();
        event3.setModelId("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        event3.setOwner("ADMIN");

        eventManager.post(event);

        eventManager.post(event2);

        eventManager.post(event3);

        Assert.assertEquals(3, eventDao.getEvents().size());

        val eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());

        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT)
                        .getEventsByModel("89af4ee2-2cdb-4b07-b39e-4c29856309aa").size()));

        eventOrchestrator.shutdown();

        await().atMost(6000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val df2 = NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT)
                    .getDataflowByModelAlias("nmodel_basic");
            Assert.assertEquals(RealizationStatusEnum.BROKEN, df2.getStatus());
            Assert.assertEquals(true, df2.isEventError());
        });
        eventOrchestrator.forceShutdown();

    }

    @Test
    public void testHandleEventErrorOnExpertMode() {
        logger.info("ModelBrokenListenerTest testHandleEventErrorOnExpertMode");

        eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());

        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

        val projectManager = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = projectManager.getProject(DEFAULT_PROJECT);
        ProjectInstance projectInstanceUpdate = projectManager.copyForWrite(projectInstance);
        projectInstanceUpdate.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectManager.updateProject(projectInstanceUpdate);
        List<Event> events = initEvents();
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        events.forEach(event -> event.setRunTimes(6));
        events.forEach(eventManager::post);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> Assert.assertEquals(
                RealizationStatusEnum.BROKEN,
                NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataflow(modelId).getStatus()));
        eventOrchestrator.forceShutdown();

    }

    @Test
    public void testHandleEventErrorOnSmartMode() {
        logger.info("ModelBrokenListenerTest testHandleEventErrorOnSmartMode");

        eventOrchestrator = new EventOrchestrator(DEFAULT_PROJECT, getTestConfig());

        String modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        System.setProperty("kylin.metadata.broken-model-deleted-on-smart-mode", "true");
        List<Event> events = initEvents();
        val eventManager = EventManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        events.forEach(event -> event.setRunTimes(6));
        events.forEach(eventManager::post);
        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            try {
                Assert.assertNull(NDataflowManager.getInstance(getTestConfig(), DEFAULT_PROJECT).getDataflow(modelId));
            } catch (NullPointerException ignored) {
            }
        });
        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
        eventOrchestrator.forceShutdown();

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
