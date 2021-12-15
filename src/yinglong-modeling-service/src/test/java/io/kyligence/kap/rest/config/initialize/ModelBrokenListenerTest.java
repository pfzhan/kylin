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

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
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

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.service.AclTCRServiceSupporter;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.JobSupporter;
import io.kyligence.kap.rest.service.SourceTestCase;
import io.kyligence.kap.rest.service.TableExtService;
import io.kyligence.kap.rest.service.TableService;
import lombok.val;
import lombok.var;

public class ModelBrokenListenerTest extends SourceTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ModelBrokenListenerTest.class);

    private static final String DEFAULT_PROJECT = "default";

    private final ModelBrokenListener modelBrokenListener = new ModelBrokenListener();

    @Mock
    private TableService tableService = Mockito.spy(TableService.class);

    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @Mock
    private AclTCRServiceSupporter aclTCRService = Mockito.spy(AclTCRServiceSupporter.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private TableExtService tableExtService = Mockito.spy(new TableExtService());

    @InjectMocks
    private final JobSupporter jobService = Mockito.spy(JobSupporter.class);

    @Before
    public void setup() {
        logger.info("ModelBrokenListenerTest setup");
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.job.event.poll-interval-second", "3");
        super.setup();
        EventBusFactory.getInstance().register(modelBrokenListener, false);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableService, "aclTCRService", aclTCRService);
        ReflectionTestUtils.setField(tableService, "fusionModelService", fusionModelService);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
        ReflectionTestUtils.setField(tableService, "jobService", jobService);
    }

    @After
    public void cleanup() {
        logger.info("ModelBrokenListenerTest cleanup");
        EventBusFactory.getInstance().unregister(modelBrokenListener);
        EventBusFactory.getInstance().restart();
        super.cleanup();
    }

    private void generateJob(String modelId, String project) {
        val jobManager = JobManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        jobManager.addIndexJob(new JobParam(modelId, "ADMIN"));

    }

    @Test
    public void testModelBrokenListener_DropModel() {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_DropModel");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateJob(modelId, project);
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        overwriteSystemProp("kylin.metadata.broken-model-deleted-on-smart-mode", "true");

        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertNull(modelManager.getDataModelDesc(modelId));
        });
    }

    @Test
    public void testModelBrokenListener_TableOriented() throws Exception {
        logger.info("ModelBrokenListenerTest testModelBrokenListener_TableOriented");

        val project = "default";
        val modelId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        generateJob(modelId, project);
        tableService.unloadTable(project, "DEFAULT.TEST_KYLIN_FACT", false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
        });

        tableExtService.loadDbTables(new String[] { "DEFAULT.TEST_KYLIN_FACT" }, project, false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            Assert.assertEquals(1,
                    NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId).getSegments().size());
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

        tableExtService.loadDbTables(new String[] { "TEST_KYLIN_FACT" }, project, false);

        await().atMost(60000, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(modelId);
            Assert.assertEquals(1, dataflow.getSegments().size());
            Assert.assertTrue(dataflow.getCoveredRange().isInfinite());
        });
    }

}
