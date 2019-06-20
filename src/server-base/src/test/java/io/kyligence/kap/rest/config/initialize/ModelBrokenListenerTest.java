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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.kyligence.kap.event.manager.EventManager;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.service.CSVSourceTestCase;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;
import lombok.var;

public class ModelBrokenListenerTest extends CSVSourceTestCase {

    private static final String PROJECT = "default";

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Mock
    private TableService tableService = Mockito.spy(TableService.class);

    @InjectMocks
    private TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        super.setup();

        SchedulerEventBusFactory.getInstance(getTestConfig()).register(modelBrokenListener);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);

    }

    @After
    public void cleanup() {
        SchedulerEventBusFactory.getInstance(getTestConfig()).unRegister(modelBrokenListener);
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
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateEvents(project, modelId);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        System.setProperty("kylin.metadata.broken-model-deleted-on-smart-mode", "true");

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(modelManager.getDataModelDesc(modelId));
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });

        System.clearProperty("kylin.metadata.broken-model-deleted-on-smart-mode");
    }

    @Test
    public void testModelBrokenListener_TableOriented() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateEvents(project, modelId);
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
            Assert.assertEquals(0, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
            Assert.assertEquals(2, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());

        });
    }

    @Test
    public void testModelBrokenListener_BrokenReason() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setBrokenReason(NDataModel.BrokenReason.EVENT);
        modelManager.updateDataModelDesc(copyForUpdate);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });
    }

    @Test
    public void testModelBrokenListener_FullBuild() throws Exception {
        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var originModel = modelManager.getDataModelDesc(modelId);
        val copyForUpdate = modelManager.copyForWrite(originModel);
        copyForUpdate.setPartitionDesc(null);
        modelManager.updateDataModelDesc(copyForUpdate);

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT");
        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(0,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });

        tableExtService.loadTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project);

        await().atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertTrue(dataflow.getCoveredRange().isInfinite());
            Assert.assertEquals(2, EventDao.getInstance(getTestConfig(), project).getEventsByModel(modelId).size());
        });
    }
}
