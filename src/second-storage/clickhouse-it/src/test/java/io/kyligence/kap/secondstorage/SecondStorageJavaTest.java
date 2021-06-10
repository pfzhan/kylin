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

package io.kyligence.kap.secondstorage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.RecoverRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.test.EnableClickHouseJob;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.NExecutableManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;

public class SecondStorageJavaTest implements JobWaiter {
    static private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    static private final String project = "table_index";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions")
    );

    @Rule
    public EnableTestUser enableTestUser = new EnableTestUser();
    @Rule
    public EnableClickHouseJob test = new EnableClickHouseJob(1, 1, project, modelId, "src/test/resources/ut_meta");
    private SecondStorageService secondStorageService = new SecondStorageService();
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Before
    public void setUp() throws Exception {
        secondStorageEndpoint.setSecondStorageService(secondStorageService);
    }

    @Test
    public void testModelUpdate() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        secondStorageService.onUpdate(project, modelId);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(manager.getAllExecutables().stream().anyMatch(ClickHouseModelCleanJob.class::isInstance));
        secondStorageService.disableModelSecondStorage(project, modelId);
        int jobNum = manager.getAllExecutables().size();
        secondStorageService.onUpdate(project, modelId);
        Assert.assertEquals(jobNum, manager.getAllExecutables().size());
    }

    @Test
    public void testCleanSegment() throws Exception {
        val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), project);
        Assert.assertTrue(tableFlowManager.isPresent());
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        int segmentNum = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(1, segmentNum);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());
        val request = new StorageRequest();
        request.setProject(project);
        request.setModel(modelId);
        request.setSegmentIds(segs);
        secondStorageEndpoint.cleanStorage(request, segs);
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getAllExecutables().stream().filter(ClickHouseSegmentCleanJob.class::isInstance).findFirst();
        Assert.assertTrue(job.isPresent());
        waitJobFinish(project, job.get().getId());
        int partitionNum = tableFlowManager.get().get(modelId).orElseThrow(() -> new IllegalStateException("tableflow not found")).getTableDataList().get(0).getPartitions().size();
        Assert.assertEquals(0, partitionNum);
    }

    @Test
    public void testDoubleTriggerSegmentLoad() throws Exception {
        NLocalWithSparkSessionTest.fullBuildAllCube(modelId, project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflow = dataflowManager.getDataflow(modelId);
        val segs = dataflow.getQueryableSegments().stream().map(NDataSegment::getId).collect(Collectors.toList());val request = new StorageRequest();
        triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        try {
            triggerClickHouseLoadJob(project, modelId, enableTestUser.getUser(), segs);
        } catch (KylinException e) {
            Assert.assertEquals(FAILED_CREATE_JOB.toErrorCode(), e.getErrorCode());
            return;
        }
        Assert.fail();
    }

    @Test(expected = IllegalStateException.class)
    public void testRecoverModelNotEnableSecondStorage() throws Exception {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelId(modelId);
        secondStorageService.disableModelSecondStorage(project, modelId);
        secondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test(expected = KylinException.class)
    public void testRecoverModelNotExist() throws Exception {
        val request = new RecoverRequest();
        request.setProject(project);
        request.setModelId(modelId+"123");
        secondStorageEndpoint.recoverModel(request);
        Assert.fail();
    }

    @Test
    public void testModelCleanJobWithoutSegments() {
        val jobId = triggerModelCleanJob(project, modelId, enableTestUser.getUser());
        val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val job = manager.getJob(jobId);
        Assert.assertTrue(job.getDataRangeStart() < job.getDataRangeEnd());
    }

    @Test
    public void testEnableModelWithoutBaseLayout() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() ->{
            NIndexPlanManager manager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.updateIndexPlan(modelId, copy -> copy.removeLayouts(Sets.newHashSet(copy.getBaseTableLayout().getId()), true, true));
            return null;
        }, project);
        secondStorageService.onUpdate(project, modelId);
        secondStorageService.disableModelSecondStorage(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        secondStorageService.onUpdate(project, modelId);
        secondStorageService.enableModelSecondStorage(project, modelId);
        Assert.assertTrue(SecondStorageUtil.isModelEnable(project, modelId));
    }
}
