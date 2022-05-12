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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseIndexCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseProjectCleanJob;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.guava20.shaded.common.collect.ImmutableSet;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.rest.controller.NModelController;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.response.JobInfoResponse;
import io.kyligence.kap.rest.response.NDataModelResponse;
import io.kyligence.kap.rest.response.NDataSegmentResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.IndexPlanService;
import io.kyligence.kap.rest.service.JobService;
import io.kyligence.kap.rest.service.ModelBuildService;
import io.kyligence.kap.rest.service.ModelQueryService;
import io.kyligence.kap.rest.service.ModelSemanticHelper;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.SegmentHelper;
import io.kyligence.kap.rest.service.params.MergeSegmentParams;
import io.kyligence.kap.rest.service.params.RefreshSegmentParams;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.enums.LockOperateTypeEnum;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.management.SecondStorageEndpoint;
import io.kyligence.kap.secondstorage.management.SecondStorageScheduleService;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.management.request.ProjectLoadRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectNodeRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectRecoveryResponse;
import io.kyligence.kap.secondstorage.management.request.ProjectTableSyncResponse;
import io.kyligence.kap.secondstorage.management.request.SecondStorageMetadataRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.PartitionType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import io.kyligence.kap.secondstorage.test.EnableScheduler;
import io.kyligence.kap.secondstorage.test.EnableTestUser;
import io.kyligence.kap.secondstorage.test.SharedSparkSession;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import io.kyligence.kap.util.ExecAndComp;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import lombok.var;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.handler.SecondStorageSegmentLoadJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.kyligence.kap.newten.clickhouse.ClickHouseUtils.configClickhouseWith;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_PROJECT_STATUS_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(JUnit4.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*", "javax.script.*"})
@PrepareForTest({SpringContext.class, InsertInto.class})
public class SecondStorageLockTest implements JobWaiter {
    private final String modelId = "acfde546-2cc9-4eec-bc92-e3bd46d4e2ee";
    private final String userName = "ADMIN";

    @ClassRule
    public static SharedSparkSession sharedSpark = new SharedSparkSession(
            ImmutableMap.of("spark.sql.extensions", "io.kyligence.kap.query.SQLPushDownExtensions",
                    "spark.sql.broadcastTimeout", "900")
    );

    public EnableTestUser enableTestUser = new EnableTestUser();

    public EnableScheduler enableScheduler = new EnableScheduler("table_index_incremental", "src/test/resources/ut_meta");

    @Rule
    public TestRule rule = RuleChain.outerRule(enableTestUser).around(enableScheduler);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private final JobService jobService = Mockito.spy(JobService.class);
    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @InjectMocks
    private SecondStorageService secondStorageService = Mockito.spy(new SecondStorageService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private SecondStorageEndpoint secondStorageEndpoint = new SecondStorageEndpoint();

    @Mock
    private SecondStorageScheduleService secondStorageScheduleService = new SecondStorageScheduleService();

    @Mock
    private final IndexPlanService indexPlanService = Mockito.spy(new IndexPlanService());

    @Mock
    private final ModelSemanticHelper modelSemanticHelper = Mockito.spy(new ModelSemanticHelper());

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final ModelBuildService modelBuildService = Mockito.spy(ModelBuildService.class);

    @Mock
    private final SegmentHelper segmentHelper = Mockito.spy(new SegmentHelper());

    @Mock
    private final FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @Mock
    private final NModelController nModelController = Mockito.spy(new NModelController());

    @Mock
    private final ModelQueryService modelQueryService = Mockito.spy(new ModelQueryService());


    private EmbeddedHttpServer _httpServer = null;
    protected IndexDataConstructor indexDataConstructor;
    private final SparkSession ss = sharedSpark.getSpark();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.when(SpringContext.getBean(SecondStorageUpdater.class)).thenAnswer((Answer<SecondStorageUpdater>) invocation -> secondStorageService);

        secondStorageEndpoint.setSecondStorageService(secondStorageService);
        secondStorageEndpoint.setModelService(modelService);

        secondStorageService.setAclEvaluate(aclEvaluate);

        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        ReflectionTestUtils.setField(modelQueryService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(indexPlanService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(modelService, "indexPlanService", indexPlanService);
        ReflectionTestUtils.setField(modelService, "semanticUpdater", modelSemanticHelper);
        ReflectionTestUtils.setField(modelService, "modelBuildService", modelBuildService);
        ReflectionTestUtils.setField(modelService, "modelQuerySupporter", modelQueryService);

        ReflectionTestUtils.setField(modelBuildService, "modelService", modelService);
        ReflectionTestUtils.setField(modelBuildService, "segmentHelper", segmentHelper);
        ReflectionTestUtils.setField(modelBuildService, "aclEvaluate", aclEvaluate);

        ReflectionTestUtils.setField(nModelController, "modelService", modelService);
        ReflectionTestUtils.setField(nModelController, "fusionModelService", fusionModelService);

        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);


        System.setProperty("kylin.job.scheduler.poll-interval-second", "1");
        System.setProperty("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());

        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());

        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @Test
    public void testIncrementBuildLockedLayout() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testIncrementBuildLockedLayout(1, clickhouse1);
        }
    }


    @Test
    public void testLockedSegmentLoadCH() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testLockedSegmentLoadCH(1, clickhouse1);
        }
    }

    @Test
    public void testLockedSegmentLoadCHAndRefresh() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testLockedSegmentLoadCHAndRefresh(1, clickhouse1);
        }
    }

    @Test
    public void testStatic() {
        JobParam jobParam = new JobParam();
        jobParam.setSecondStorageDeleteLayoutIds(null);
        assertNull(jobParam.getSecondStorageDeleteLayoutIds());

        SecondStorageCleanJobBuildParams params = new SecondStorageCleanJobBuildParams(null, jobParam, null);
        params.setSecondStorageDeleteLayoutIds(jobParam.getSecondStorageDeleteLayoutIds());
        assertNull(params.getSecondStorageDeleteLayoutIds());
    }

    @Test
    public void testSegmentLoadWithRetry() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testSegmentLoadWithRetry(1, clickhouse1);
        }
    }

    @Test
    public void testModelCleanJobWithAddColumn() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                long jobCnt = getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count();

                ModelRequest request = getChangedModelRequest("TRANS_ID");
                EnvelopeResponse<BuildBaseIndexResponse> res1 = nModelController.updateSemantic(request);
                assertEquals("000", res1.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequest("LEAF_CATEG_ID");
                request.setWithSecondStorage(false);
                EnvelopeResponse<BuildBaseIndexResponse> res2 = nModelController.updateSemantic(request);
                assertEquals("000", res2.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count());

                return null;
            });
        }
    }

    @Test
    public void testModelCleanJobWithChangePartition() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse1.getJdbcUrl(), clickhouse1.getDriverClassName());

                val partitionDesc = getNDataModel().getPartitionDesc();
                partitionDesc.setPartitionDateFormat("yyyy-MM-dd");

                long jobCnt = getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count();

                getNDataModelManager().updateDataModel(modelId, copier -> copier.setManagementType(ManagementType.MODEL_BASED));

                ModelRequest request = getChangedModelRequestWithNoPartition("TRANS_ID");
                EnvelopeResponse<BuildBaseIndexResponse> res1 = nModelController.updateSemantic(request);
                assertEquals("000", res1.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequestWithPartition("LEAF_CATEG_ID", partitionDesc);
                EnvelopeResponse<BuildBaseIndexResponse> res2 = nModelController.updateSemantic(request);
                assertEquals("000", res2.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count());

                request = getChangedModelRequestWithNoPartition("TEST_COUNT_DISTINCT_BITMAP");
                request.setWithSecondStorage(false);
                EnvelopeResponse<BuildBaseIndexResponse> res3 = nModelController.updateSemantic(request);
                assertEquals("000", res3.getCode());
                jobCnt++;
                assertEquals(jobCnt, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseModelCleanJob.class::isInstance).count());

                return null;
            });
        }
    }

    @Test
    public void testDropModelWithSecondStorage() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());
                ModelRequest request = new ModelRequest();
                request.setWithSecondStorage(true);
                request.setUuid(modelId);
                BuildBaseIndexResponse changedResponse = mock(BuildBaseIndexResponse.class);
                Mockito.doCallRealMethod().when(modelService).changeSecondStorageIfNeeded(eq("default"), eq(request), eq(() -> true));
                when(changedResponse.hasTableIndexChange()).thenReturn(true);

                modelService.dropModel(modelId, getProject());

                val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
                val tableFlow = tableFlowManager.get().get(modelId);
                Assert.assertFalse(tableFlow.isPresent());

                return null;
            });
        }
    }

    @Test
    public void changeSecondStorageIfNeeded() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());
                ModelRequest request = new ModelRequest();
                request.setWithSecondStorage(false);
                request.setUuid(modelId);

                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertFalse(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(true);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertTrue(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(true);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertTrue(SecondStorageUtil.isModelEnable(getProject(), modelId));

                request.setWithSecondStorage(false);
                modelService.changeSecondStorageIfNeeded(getProject(), request, () -> true);
                Assert.assertFalse(SecondStorageUtil.isModelEnable(getProject(), modelId));

                return null;
            });
        }
    }

    @Test
    public void testPurgeModelWithSecondStorage() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

            configClickhouseWith(new JdbcDatabaseContainer[]{clickhouse1}, 1, catalog, () -> {
                secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
                Assert.assertEquals(1, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);

                Assert.assertTrue(getIndexPlan().containBaseTableLayout());

                modelService.purgeModel(modelId, getProject());

                val tableFlowManager = SecondStorageUtil.tableFlowManager(KylinConfig.getInstanceFromEnv(), getProject());
                val tableFlow = tableFlowManager.get().get(modelId);
                Assert.assertFalse(tableFlow.isPresent());
                return null;
            });
        }
    }

    public void testSegmentLoadWithRetry(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        PowerMockito.mockStatic(InsertInto.class);
        PowerMockito.when(InsertInto.insertInto(Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new SQLException("broken pipe HTTPSession"));

        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            buildIncrementalLoadQuery(); // build table index
            checkHttpServer(); // check http server

            val segments = new HashSet<>(getDataFlow().getSegments());
            AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
            JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), getDataFlow().getModel().getUuid(), "ADMIN",
                    segments.stream().map(NDataSegment::getId));
            String jobId = ClickHouseUtils.simulateJobMangerAddJob(jobParam, localHandler);
            TimeUnit.MILLISECONDS.sleep(20000);
            PowerMockito.doCallRealMethod().when(InsertInto.class);
            InsertInto.insertInto(Mockito.anyString(), Mockito.anyString());
            waitJobFinish(getProject(), jobId);
            return true;
        });
    }

    @Test
    public void testDeleteShard() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                ProjectNodeRequest request = new ProjectNodeRequest();
                request.setProject("wrong");
                Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));
                request.setProject(getProject());
                Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));

                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);

                val clickhouseNew = new JdbcDatabaseContainer[]{clickhouse1, clickhouse2};
                ClickHouseUtils.internalConfigClickHouse(clickhouseNew, replica);
                secondStorageService.changeProjectSecondStorageState(getProject(), ImmutableList.of("pair1"), true);
                assertEquals(clickhouseNew.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                EnvelopeResponse<ProjectTableSyncResponse> response = secondStorageEndpoint.tableSync(getProject());
                assertEquals("000", response.getCode());

                deleteShardParamsCheck(request);

                request.setForce(false);
                val shardNames2 = ImmutableList.of("pair0");
                assertThrows(TransactionException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames2));

                EnvelopeResponse<List<String>> res1 = this.secondStorageEndpoint.deleteProjectNodes(request, ImmutableList.of("pair1"));
                assertEquals("000", res1.getCode());
                assertTrue(res1.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair0"), Collections.singletonList("pair1"));

                assertFalse(LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));

                assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeProjectSecondStorageState(getProject(), ImmutableList.of("pair1"), true);
                assertEquals(clickhouseNew.length, SecondStorageUtil.listProjectNodes(getProject()).size());

                EnvelopeResponse<ProjectTableSyncResponse> response2 = secondStorageEndpoint.tableSync(getProject());
                assertEquals("000", response2.getCode());

                secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.LOCK.name());

                request.setForce(true);
                List<String> shardNames = ImmutableList.of("pair0");
                EnvelopeResponse<List<String>> res2 = this.secondStorageEndpoint.deleteProjectNodes(request, shardNames);
                assertEquals("000", res2.getCode());
                assertFalse(res2.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair1"), Collections.singletonList("pair0"));
                assertTrue(getTableFlow().getTableDataList().isEmpty());

                assertEquals(1, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseProjectCleanJob.class::isInstance).filter(s -> s.getId().equals(res2.getData().get(0))).count());

                assertTrue(LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));
                secondStorageService.lockOperate(getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()), LockOperateTypeEnum.UNLOCK.name());

                return true;
            });
        }
    }

    @Test
    public void testSizeInNode() throws Exception {
        SecondStorageMetadataRequest request = new SecondStorageMetadataRequest();
        request.setProject("");
        Assert.assertThrows(
                MsgPicker.getMsg().getEMPTY_PROJECT_NAME(),
                KylinException.class,
                () -> this.secondStorageEndpoint.sizeInNode(request));
        request.setProject("123");
        Assert.assertThrows("123", KylinException.class, () -> this.secondStorageEndpoint.sizeInNode(request));
        request.setProject(getProject());
        Assert.assertThrows(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_PROJECT_ENABLED(), getProject()),
                KylinException.class, () -> this.secondStorageEndpoint.sizeInNode(request));

        Assert.assertThrows(
                MsgPicker.getMsg().getEMPTY_PROJECT_NAME(),
                KylinException.class,
                () -> this.secondStorageEndpoint.tableSync(""));
        Assert.assertThrows("123", KylinException.class, () -> this.secondStorageEndpoint.tableSync("123"));
        String project = getProject();
        Assert.assertThrows(
                String.format(Locale.ROOT, MsgPicker.getMsg().getSECOND_STORAGE_PROJECT_ENABLED(), getProject()),
                KylinException.class, () -> this.secondStorageEndpoint.tableSync(project));
    }

    @Test
    public void testDeleteShardHA() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse2 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse3 = ClickHouseUtils.startClickHouse();
             JdbcDatabaseContainer<?> clickhouse4 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1, clickhouse2, clickhouse3, clickhouse4};
            int replica = 2;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);
                checkSegmentDisplay(replica, clickhouse.length / replica);

                ProjectNodeRequest request = new ProjectNodeRequest();
                request.setProject(getProject());
                request.setForce(true);
                EnvelopeResponse<List<String>> res2 = this.secondStorageEndpoint.deleteProjectNodes(request, ImmutableList.of("pair1"));
                assertEquals("000", res2.getCode());
                assertFalse(res2.getData().isEmpty());
                checkDeletedStatus(Collections.singletonList("pair0"), Collections.singletonList("pair1"));
                assertTrue(getTableFlow().getTableDataList().isEmpty());

                assertFalse(LockTypeEnum.locked(LockTypeEnum.LOAD.name(), SecondStorageUtil.getProjectLocks(getProject())));
                assertEquals(1, getNExecutableManager().getAllExecutables().stream().filter(ClickHouseProjectCleanJob.class::isInstance).filter(s -> s.getId().equals(res2.getData().get(0))).count());
                return true;
            });
        }
    }

    private void deleteShardParamsCheck(ProjectNodeRequest request) {
        request.setProject(getProject());
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, null));

        List<String> shardNames1 = Collections.emptyList();
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames1));

        List<String> shardNames2 = ImmutableList.of("test");
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames2));

        List<String> shardNames3 = ImmutableList.of("pair0", "test");
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames3));

        List<String> shardNames4 = ImmutableList.of("pair0", "pair1");
        Assert.assertThrows(KylinException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames4));

        List<String> shardNames5 = ImmutableList.of("pair0");
        Assert.assertThrows(TransactionException.class, () -> this.secondStorageEndpoint.deleteProjectNodes(request, shardNames5));
    }

    private void checkDeletedStatus(List<String> shards, List<String> deletedShards) {
        List<NodeGroup> nodeGroups = getNodeGroups();
        Map<String, List<Node>> clusters = ClickHouseConfigLoader.getInstance().getCluster().getCluster();
        assertEquals(clusters.get(shards.get(0)).size(), nodeGroups.size());
        assertEquals(shards.size(), nodeGroups.get(0).getNodeNames().size());

        shards.forEach(shard ->
                nodeGroups.forEach(nodeGroup ->
                        assertFalse(CollectionUtils.intersection(clusters.get(shard).stream().map(Node::getName).collect(Collectors.toList()), nodeGroup.getNodeNames()).isEmpty()))
        );

        deletedShards.forEach(shard ->
                nodeGroups.forEach(nodeGroup ->
                        assertTrue(CollectionUtils.intersection(clusters.get(shard).stream().map(Node::getName).collect(Collectors.toList()), nodeGroup.getNodeNames()).isEmpty()))
        );

        getTableFlow().getTableDataList().forEach(tableData -> tableData.getPartitions().forEach(p -> {
            assertEquals(shards.size(), p.getShardNodes().size());

            shards.stream().flatMap(shard -> clusters.get(shard).stream()).map(Node::getName).forEach(nodeName -> {
                        assertTrue(p.getSizeInNode().containsKey(nodeName));
                        assertTrue(p.getNodeFileMap().containsKey(nodeName));
                    }
            );

            deletedShards.stream().flatMap(shard -> clusters.get(shard).stream()).map(Node::getName).forEach(nodeName -> {
                        assertFalse(p.getSizeInNode().containsKey(nodeName));
                        assertFalse(p.getNodeFileMap().containsKey(nodeName));
                    }
            );
        }));
    }

    @Test
    public void testProjectSecondStorageJobs() {
        try {
            secondStorageEndpoint.getProjectSecondStorageJobs("error");
        } catch (KylinException e) {
            assertEquals(SECOND_STORAGE_PROJECT_STATUS_ERROR.toErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testProjectLoadWithOneNodeDown() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {

            final String catalog = "default";

            Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
            Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
            val clickhouse = new JdbcDatabaseContainer[]{clickhouse1};
            int replica = 1;
            configClickhouseWith(clickhouse, replica, catalog, () -> {
                List<String> allPairs = SecondStorageNodeHelper.getAllPairs();
                secondStorageService.changeProjectSecondStorageState(getProject(), allPairs, true);
                Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
                secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
                setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

                getBuildBaseLayout(new HashSet<>(), new HashSet<>(), clickhouse, replica);

                Map<String, Map<String, Boolean>> nodeStatusMap = ImmutableMap.of("pair0", ImmutableMap.of("node00", false));
                secondStorageEndpoint.updateNodeStatus(nodeStatusMap);

                ProjectLoadRequest request = new ProjectLoadRequest();
                request.setProjects(ImmutableList.of(getProject()));
                EnvelopeResponse<List<ProjectRecoveryResponse>> response = this.secondStorageEndpoint.projectLoad(request);
                assertEquals("000", response.getCode());
                waitAllJobFinish();

                getTableFlow().getTableDataList().forEach(tableData -> tableData.getPartitions().forEach(p -> assertNotEquals(0, (long) p.getSizeInNode().getOrDefault("node00", 0L))));

                return true;
            });
        }
    }

    public void testSegmentLoadWithoutRetry() throws Exception {
        try (JdbcDatabaseContainer<?> clickhouse1 = ClickHouseUtils.startClickHouse()) {
            testSegmentLoadWithoutRetry(1, clickhouse1);
        }
    }

    public void testSegmentLoadWithoutRetry(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        PowerMockito.mockStatic(InsertInto.class);
        PowerMockito.when(InsertInto.insertInto(Mockito.anyString(), Mockito.anyString()))
                .thenAnswer((Answer<InsertInto>) invocation -> new InsertInto(TableIdentifier.table("def", "tab")));

        final String catalog = "default";
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            buildIncrementalLoadQuery(); // build table index
            checkHttpServer(); // check http server

            val segments = new HashSet<>(getDataFlow().getSegments());
            AbstractJobHandler localHandler = new SecondStorageSegmentLoadJobHandler();
            JobParam jobParam = SecondStorageJobParamUtil.of(getProject(), getDataFlow().getModel().getUuid(), "ADMIN",
                    segments.stream().map(NDataSegment::getId));
            String jobId = ClickHouseUtils.simulateJobMangerAddJob(jobParam, localHandler);
            TimeUnit.MILLISECONDS.sleep(20000);
            PowerMockito.doCallRealMethod().when(InsertInto.class);
            InsertInto.insertInto(Mockito.anyString(), Mockito.anyString());
            waitJobEnd(getProject(), jobId);
            return true;
        });
    }

    private void testLockedSegmentLoadCH(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        final String catalog = "default";

        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            Set<Long> existTablePlanLayoutIds = new HashSet<>();
            Set<Long> existTableDataLayoutIds = new HashSet<>();

            val layout01 = getBuildBaseLayout(existTablePlanLayoutIds, existTableDataLayoutIds, clickhouse, replica);
            val layout02 = updateIndex("LSTG_SITE_ID");

            val segment = getDataFlow().getFirstSegment();
            buildSegments(layout02, segment);
            ClickHouseUtils.triggerClickHouseJob(getDataFlow());

            existTablePlanLayoutIds.add(layout02);
            existTableDataLayoutIds.add(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout01);
            checkSecondStorageSegmentMetadata(ImmutableSet.of(segment.getId()), layout02);

            return true;
        });

    }

    private void testIncrementBuildLockedLayout(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        final String catalog = "default";

        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            AtomicInteger indexDeleteJobCnt = new AtomicInteger(0);
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            Set<Long> existTablePlanLayoutIds = new HashSet<>();
            Set<Long> existTableDataLayoutIds = new HashSet<>();

            // Test
            // Step1: create model and load to second storage
            val layout01 = getBuildBaseLayout(existTablePlanLayoutIds, existTableDataLayoutIds, clickhouse, replica);

            // Step2: change model and removed locked index
            val layout02 = updateIndex("LSTG_SITE_ID");

            buildSegmentAndLoadCH(layout02); // build new index segment
            existTablePlanLayoutIds.add(layout02);
            existTableDataLayoutIds.add(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout01);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);
            checkSegmentDisplay(replica, clickhouse.length / replica);

            // removed old index
            indexPlanService.removeIndexes(getProject(), modelId, ImmutableSet.of(layout01));
            waitAllJobFinish();
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream().filter(ClickHouseIndexCleanJob.class::isInstance).count());

            existTablePlanLayoutIds.remove(layout01);
            existTableDataLayoutIds.remove(layout01);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);

            // check load second storage button is enable
            assertTrue(checkNDataSegmentResponse());

            // Step3: change model and test remove segment of locked index
            val layout03 = updateIndex("IS_EFFECTUAL"); // update model to new
            existTablePlanLayoutIds.add(layout03);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);

            buildSegmentAndLoadCH(layout03); // build new index segment
            existTableDataLayoutIds.add(layout03);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout02);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            NDataSegment segmentTmp01 = getDataFlow().getSegments().getFirstSegment();
            removeIndexesFromSegments(segmentTmp01.getId(), layout02);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream().filter(ClickHouseIndexCleanJob.class::isInstance).count());

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getDataFlow().getSegments().stream().map(NDataSegment::getId).filter(segmentId -> !segmentId.equals(segmentTmp01.getId())).collect(Collectors.toSet()), layout02);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            // test remove all segment of locked index
            removeIndexesFromSegments(getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toList()), layout02);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream().filter(ClickHouseIndexCleanJob.class::isInstance).count());
            existTablePlanLayoutIds.remove(layout02);
            existTableDataLayoutIds.remove(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout03);

            // Step4: change model and test refresh segment

            long layout04 = testRefreshSegment(existTablePlanLayoutIds, existTableDataLayoutIds, layout03);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream().filter(ClickHouseIndexCleanJob.class::isInstance).count());

            long layout05 = testCleanSegment(existTablePlanLayoutIds, existTableDataLayoutIds, layout04);
            assertEquals(indexDeleteJobCnt.incrementAndGet(), getNExecutableManager().getAllExecutables().stream().filter(ClickHouseIndexCleanJob.class::isInstance).count());
            testMerge(existTablePlanLayoutIds, existTableDataLayoutIds, layout04, layout05);

            // Step7: test build new segment and load all to second storage
            Set<String> layout04Segments = getAllSegmentIds();
            buildIncrementalLoadQuery("2012-01-04", "2012-01-05", ImmutableSet.of(getIndexPlan().getLayoutEntity(layout05)));
            ClickHouseUtils.triggerClickHouseJob(getDataFlow());

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(layout04Segments, layout04);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

//            // Step7: delete segment
//            Set<String> deletedSegments = new HashSet<>();
//            for (NDataSegment segment : getDataFlow().getSegments()) {
//                deletedSegments.add(segment.getId());
//                deleteSegmentById(segment.getId());
//
//                if (deletedSegments.size() == getDataFlow().getSegments().size()) {
//                    existTablePlanLayoutIds.remove(layout04);
//                    existTableDataLayoutIds.remove(layout04);
//                }
//
//                checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
//                checkSecondStorageSegmentMetadata(getDataFlow().getSegments().stream().map(NDataSegment::getId).filter(segmentId -> !deletedSegments.contains(segmentId)).collect(Collectors.toSet()), layout05);
//                checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);
//            }

            // Step8: close second storage
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, false);
            return true;
        });
    }

    private void testLockedSegmentLoadCHAndRefresh(int replica, JdbcDatabaseContainer<?>... clickhouse) throws Exception {
        final String catalog = "default";

        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, getSourceUrl());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());

        configClickhouseWith(clickhouse, replica, catalog, () -> {
            secondStorageService.changeProjectSecondStorageState(getProject(), SecondStorageNodeHelper.getAllPairs(), true);
            Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(getProject()).size());
            secondStorageService.changeModelSecondStorageState(getProject(), modelId, true);
            setQuerySession(catalog, clickhouse[0].getJdbcUrl(), clickhouse[0].getDriverClassName());

            Set<Long> existTablePlanLayoutIds = new HashSet<>();
            Set<Long> existTableDataLayoutIds = new HashSet<>();

            // Test
            // Step1: create model and load to second storage
            val layout01 = updateIndex("TRANS_ID");
            existTablePlanLayoutIds.add(layout01);
            buildIncrementalLoadQuery(); // build table index
            checkHttpServer(); // check http server

            val layout02 = updateIndex("LSTG_SITE_ID");

            val segment = getDataFlow().getFirstSegment();
            List<String> segmentsOther = getDataFlow().getSegments().stream().filter(s -> !s.getId().equals(segment.getId()))
                    .map(NDataSegment::getId).collect(Collectors.toList());

            List<JobInfoResponse.JobInfo> jobInfos = modelBuildService.refreshSegmentById(new RefreshSegmentParams(getProject(), modelId,
                    new String[]{segment.getId()}, true));

            jobInfos.forEach(j -> waitJobFinish(getProject(), j.getJobId()));

            existTablePlanLayoutIds.add(layout02);
            existTableDataLayoutIds.add(layout02);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(getDataFlow().getSegments().stream()
                    .filter(s -> !segmentsOther.contains(s.getId())).map(NDataSegment::getId).collect(Collectors.toSet()), layout02);

            triggerSegmentLoad(segmentsOther);
            existTableDataLayoutIds.add(layout01);
            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(new HashSet<>(segmentsOther), layout01);
            return true;
        });
    }

    private long getBuildBaseLayout(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds, JdbcDatabaseContainer<?>[] clickhouse, int replica) throws Exception {
        // Test
        // Step1: create model and load to second storage
        val layout01 = updateIndex("TRANS_ID");

        buildIncrementalLoadQuery(); // build table index
        checkHttpServer(); // check http server
        ClickHouseUtils.triggerClickHouseJob(getDataFlow()); //load into clickhouse
        existTablePlanLayoutIds.add(layout01);
        existTableDataLayoutIds.add(layout01);

        checkSecondStorageBaseMetadata(true, clickhouse.length, replica);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout01);

        String sql1 = "select TRANS_ID from TEST_KYLIN_FACT where CAL_DT between '2012-01-01' and '2012-01-02' limit 1";
        assertQueryResult(sql1, layout01);

        return layout01;
    }

    private long testRefreshSegment(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds, long lockedLayoutId) throws IOException, InterruptedException {
        val layout04 = updateIndex("LEAF_CATEG_ID");
        existTablePlanLayoutIds.add(layout04);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);

        buildSegmentAndLoadCH(layout04); // build new index segment
        existTableDataLayoutIds.add(layout04);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout04);

        Set<String> notRefreshedSegments = getAllSegmentIds();

        for (NDataSegment segment : getDataFlow().getSegments()) {
            notRefreshedSegments.remove(segment.getId());
            List<JobInfoResponse.JobInfo> jobInfos = modelBuildService.refreshSegmentById(new RefreshSegmentParams(getProject(), modelId,
                    new String[]{segment.getId()}));

            jobInfos.forEach(j -> waitJobFinish(getProject(), j.getJobId()));

            if (notRefreshedSegments.size() == 0) {
                existTablePlanLayoutIds.remove(lockedLayoutId);
                existTableDataLayoutIds.remove(lockedLayoutId);
            }

            checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
            checkSecondStorageSegmentMetadata(notRefreshedSegments, lockedLayoutId);
            checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout04);
        }

        return layout04;
    }

    private long testCleanSegment(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds, long lockedLayoutId) throws IOException, InterruptedException {
        // Step5: change model and test clean second storage segment
        val layout05 = updateIndex("TEST_COUNT_DISTINCT_BITMAP");
        existTablePlanLayoutIds.add(layout05);
        buildSegmentAndLoadCH(layout05); // build new index segment
        existTableDataLayoutIds.add(layout05);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

        // fix ut coverage, it not work
//            cleanSegments(modelId + "1", null, null); // fix ut coverage, it not work
        getTableFlow().cleanTableData(null); // fix ut coverage, it not work
        getTablePlan().cleanTable(null); // fix ut coverage, it not work

        cleanSegments(getAllSegmentIds(), ImmutableSet.of(lockedLayoutId, layout05));

        ClickHouseUtils.triggerClickHouseJob(getDataFlow());
        existTableDataLayoutIds.add(lockedLayoutId);
        existTableDataLayoutIds.add(layout05);
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layout05);

        return layout05;
    }

    private void testMerge(Set<Long> existTablePlanLayoutIds, Set<Long> existTableDataLayoutIds, long lockedLayoutId, long layoutId) {
        // Step6: test merge
        Collections.sort(getDataFlow().getSegments());
        mergeSegment(ImmutableSet.of(getDataFlow().getSegments().get(0).getId(), getDataFlow().getSegments().get(1).getId()));
        checkSecondStorageMetadata(existTablePlanLayoutIds, existTableDataLayoutIds);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), lockedLayoutId);
        checkSecondStorageSegmentMetadata(getAllSegmentIds(), layoutId);
    }

    public String getProject() {
        return "table_index_incremental";
    }

    private void buildIncrementalLoadQuery() throws Exception {
        buildIncrementalLoadQuery("2012-01-01", "2012-01-02");
        buildIncrementalLoadQuery("2012-01-02", "2012-01-03");
        buildIncrementalLoadQuery("2012-01-03", "2012-01-04");

        waitAllJobFinish();
    }

    private void buildIncrementalLoadQuery(String start, String end) throws Exception {
        buildIncrementalLoadQuery(start, end, new HashSet<>(getIndexPlan().getAllLayouts()));
    }

    private void buildIncrementalLoadQuery(String start, String end, Set<LayoutEntity> layoutIds) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val dfName = modelId;
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        val timeRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        indexDataConstructor.buildIndex(dfName, timeRange, layoutIds, true);
    }


    @SneakyThrows
    private void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        assertTrue(content.length() > 0);
    }


    private void checkSecondStorageBaseMetadata(boolean isIncremental, int clickhouseNodeSize, int replicaSize) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflowManager dsMgr = NDataflowManager.getInstance(config, getProject());
        NDataflow df = dsMgr.getDataflow(modelId);
        // check TableFlow
        TablePlan plan = SecondStorage.tablePlanManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(plan);
        TableFlow flow = SecondStorage.tableFlowManager(config, getProject()).get(modelId).orElse(null);
        Assert.assertNotNull(flow);

        Set<LayoutEntity> allLayouts = df.getIndexPlan().getAllLayouts().stream()
                .filter(SecondStorageUtil::isBaseTableIndex).collect(Collectors.toSet());
        Assert.assertEquals(allLayouts.size(), flow.getTableDataList().size());

        for (LayoutEntity layoutEntity : allLayouts) {
            TableEntity tableEntity = plan.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(tableEntity);
            TableData data = flow.getEntity(layoutEntity).orElse(null);
            Assert.assertNotNull(data);
            Assert.assertEquals(isIncremental ? PartitionType.INCREMENTAL : PartitionType.FULL, data.getPartitionType());
            Assert.assertEquals(dsMgr.getDataflow(modelId).getQueryableSegments().size(),
                    data.getPartitions().size() / replicaSize);
            TablePartition partition = data.getPartitions().get(0);
            int shards = Math.min(clickhouseNodeSize / replicaSize, tableEntity.getShardNumbers());
            Assert.assertEquals(shards, partition.getShardNodes().size());
            Assert.assertEquals(shards, partition.getSizeInNode().size());
            Assert.assertTrue(partition.getSizeInNode().values().stream().reduce(Long::sum).orElse(0L) > 0L);
        }
    }

    private void checkSecondStorageMetadata(Set<Long> tablePlanLayoutIds, Set<Long> tableDataLayoutIds) {
        Set<Long> existTablePlanLayoutId = getTablePlan().getTableMetas().stream().map(TableEntity::getLayoutID).collect(Collectors.toSet());
        assertEquals(existTablePlanLayoutId.size(), tablePlanLayoutIds.size());
        assertTrue(existTablePlanLayoutId.containsAll(tablePlanLayoutIds));

        Set<Long> existTableDataLayoutId = getTableFlow().getTableDataList().stream().map(TableData::getLayoutID).collect(Collectors.toSet());
        assertEquals(existTableDataLayoutId.size(), tableDataLayoutIds.size());
        assertTrue(existTableDataLayoutId.containsAll(tableDataLayoutIds));
    }

    private void checkSecondStorageSegmentMetadata(Set<String> segmentIds, long layoutId) {
        if (segmentIds.isEmpty()) {
            return;
        }

        assertTrue(getTableFlow().getEntity(layoutId).isPresent());
        Set<String> existSegmentIds = getTableFlow().getEntity(layoutId).get().getAllSegments();
        assertEquals(existSegmentIds.size(), segmentIds.size());
        assertTrue(existSegmentIds.containsAll(segmentIds));
    }

    private void mergeSegment(Set<String> segmentIds) {
        JobInfoResponse.JobInfo jobInfo = modelBuildService.mergeSegmentsManually(new MergeSegmentParams(getProject(), modelId,
                segmentIds.toArray(new String[]{})));

        waitJobFinish(getProject(), jobInfo.getJobId());
    }

    private void checkSegmentDisplay(int replica, int shardCnt) {
        List<NDataSegmentResponse> segments = modelService.getSegmentsResponse(modelId, getProject(), "0", "" + (Long.MAX_VALUE - 1), null,
                null, null, false, null, false);
        segments.forEach(segment -> {
            assertEquals(shardCnt, segment.getSecondStorageNodes().size());
            assertNotNull(segment.getSecondStorageNodes().values());
            assertTrue(segment.getSecondStorageNodes().values().stream().findFirst().isPresent());
            assertEquals(replica, segment.getSecondStorageNodes().values().stream().findFirst().get().size());
        });

        val sum = segments.stream().mapToLong(NDataSegmentResponse::getSecondStorageSize).sum();

        DataResult<List<NDataModel>> result = modelService.getModels(modelId, null, true, getProject(),
                null, null, null, 0, 10, "last_modify", false, null,
                null, null, null, true);

        result.getValue().stream().filter(nDataModel -> modelId.equals(nDataModel.getId())).forEach(nDataModel -> {
            val nDataModelRes = (NDataModelResponse) nDataModel;
            assertEquals(sum, nDataModelRes.getSecondStorageSize());
            assertEquals(shardCnt, nDataModelRes.getSecondStorageNodes().size());
            assertEquals(replica, nDataModelRes.getSecondStorageNodes().get("pair0").size());
        });
    }

    @Data
    public static class BuildBaseIndexUT {
        @JsonProperty("base_table_index")
        public IndexInfo tableIndex;


        @Data
        public static class IndexInfo {
            @JsonProperty("layout_id")
            private long layoutId;
        }
    }

    private KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    private TableFlow getTableFlow() {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private TablePlan getTablePlan() {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(modelId).get();
    }

    private IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(modelId);
    }

    private NDataflow getDataFlow() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(modelId);
    }

    private NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private NDataModel getNDataModel() {
        return getNDataModelManager().getDataModelDesc(modelId);
    }

    private NExecutableManager getNExecutableManager() {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    private List<NodeGroup> getNodeGroups() {
        Preconditions.checkState(SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).isPresent());
        return SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).get().listAll();
    }

    private Set<String> getAllSegmentIds() {
        return getDataFlow().getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
    }

    private String getSourceUrl() {
        return _httpServer.uriAccessedByDocker.toString();
    }

    private static String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private ModelRequest getChangedModelRequest(String columnName) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        val partitionDesc = model.getPartitionDesc();
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        request.setPartitionDesc(model.getPartitionDesc());
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private ModelRequest getChangedModelRequestWithNoPartition(String columnName) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        request.setPartitionDesc(null);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private ModelRequest getChangedModelRequestWithPartition(String columnName, PartitionDesc partitionDesc) throws IOException {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");

        var model = getNDataModel();

        val request = JsonUtil.readValue(JsonUtil.writeValueAsString(model), ModelRequest.class);
        request.setProject(getProject());
        request.setUuid(modelId);
        request.setAllNamedColumns(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSimplifiedMeasures(model.getAllMeasures().stream().filter(m -> !m.isTomb())
                .map(SimplifiedMeasure::fromMeasure).collect(Collectors.toList()));
        request.setSimplifiedDimensions(model.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isDimension)
                .collect(Collectors.toList()));
        request.setSaveOnly(true);

        val columnDesc = model.getRootFactTable().getColumn(columnName).getColumnDesc(); // TRANS_ID
        request.getSimplifiedDimensions().add(getNamedColumn(columnDesc));
        request.setPartitionDesc(partitionDesc);
        request.setWithSecondStorage(true);

        return JsonUtil.readValue(JsonUtil.writeValueAsString(request), ModelRequest.class);
    }

    private NDataModel.NamedColumn getNamedColumn(ColumnDesc columnDesc) {
        NDataModel.NamedColumn transIdColumn = new NDataModel.NamedColumn();
        transIdColumn.setId(Integer.parseInt(columnDesc.getId()));
        transIdColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        transIdColumn.setName(columnDesc.getTable().getName() + "_" + columnDesc.getName());
        transIdColumn.setAliasDotColumn(columnDesc.getTable().getName() + "." + columnDesc.getName());
        return transIdColumn;
    }

    private void buildSegmentAndLoadCH(long layoutId) throws InterruptedException {
        // build new index segment
        for (NDataSegment segment : getDataFlow().getSegments()) {
            buildSegments(layoutId, segment);
        }

        waitAllJobFinish();
        ClickHouseUtils.triggerClickHouseJob(getDataFlow());
    }

    private void buildSegments(long layoutId, NDataSegment segment) throws InterruptedException {
        indexDataConstructor.buildSegment(modelId, segment, ImmutableSet.of(getDataFlow().getIndexPlan().getLayoutEntity(layoutId)), true, null);
    }

    private long updateIndex(String columnName) throws IOException {
        val indexResponse = modelService.updateDataModelSemantic(getProject(), getChangedModelRequest(columnName));
        val layoutId = JsonUtil.readValue(JsonUtil.writeValueAsString(indexResponse), BuildBaseIndexUT.class).tableIndex.layoutId;

        getNExecutableManager().getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
        return layoutId;
    }

    private boolean checkNDataSegmentResponse() {
        NDataSegmentResponse res = new NDataSegmentResponse(getDataFlow(), getDataFlow().getFirstSegment());
        return res.isHasBaseTableIndexData();
    }

    private void removeIndexesFromSegments(String segmentId, long indexId) {
        removeIndexesFromSegments(ImmutableList.of(segmentId), indexId);
    }

    private void removeIndexesFromSegments(List<String> segmentIds, long indexId) {
        modelService.removeIndexesFromSegments(getProject(), modelId, segmentIds, ImmutableList.of(indexId));
        waitAllJobFinish();
    }

    private void setQuerySession(String catalog, String jdbcUrl, String driverClassName) {
        System.setProperty("kylin.query.use-tableindex-answer-non-raw-query", "true");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog,
                "org.apache.spark.sql.execution.datasources.jdbc.v2.SecondStorageCatalog");
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".url", jdbcUrl);
        ss.sessionState().conf().setConfString("spark.sql.catalog." + catalog + ".driver", driverClassName);
    }

    private void assertQueryResult(String sql, long hitLayoutId) throws SQLException {
        OLAPContext.clearThreadLocalContexts();
        ExecAndComp.queryModel(getProject(), sql);
        assertTrue(OLAPContext.getNativeRealizations().stream().allMatch(NativeQueryRealization::isSecondStorage));
        assertTrue(OLAPContext.getNativeRealizations().stream().findFirst().isPresent());
        assertEquals(OLAPContext.getNativeRealizations().stream().findFirst().get().getLayoutId().longValue(), hitLayoutId);
    }

    public void cleanSegments(Set<String> segments, Set<Long> layoutIds) {
        SecondStorageUtil.cleanSegments(getProject(), modelId, segments, layoutIds);

        val jobHandler = new SecondStorageIndexCleanJobHandler();
        final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(getProject(), modelId,
                "ADMIN", layoutIds, segments);
        ClickHouseUtils.simulateJobMangerAddJob(param, jobHandler);
        waitAllJobFinish();
    }

    private void waitAllJobFinish() {
        NExecutableManager.getInstance(getConfig(), getProject()).getAllExecutables().forEach(exec -> waitJobFinish(getProject(), exec.getId()));
    }

    private void deleteSegmentById(String segmentId) {
        modelService.deleteSegmentById(modelId, getProject(), new String[]{segmentId}, true);
    }

    private void triggerSegmentLoad(List<String> segments) {
        val request = new StorageRequest();
        request.setProject(getProject());
        request.setModel(modelId);
        request.setSegmentIds(segments);
        EnvelopeResponse<List<String>> jobs = secondStorageEndpoint.getAllSecondStoragrJobs();
        assertEquals("000", jobs.getCode());
        assertEquals(0, jobs.getData().size());
        EnvelopeResponse<JobInfoResponse> res = secondStorageEndpoint.loadStorage(request);
        assertEquals("000", res.getCode());
        EnvelopeResponse<List<String>> jobs1 = secondStorageEndpoint.getProjectSecondStorageJobs(getProject());
        assertEquals("000", jobs1.getCode());
        assertEquals(1, jobs1.getData().size());
        for (JobInfoResponse.JobInfo job : res.getData().getJobs()) {
            waitJobFinish(getProject(), job.getJobId());
        }
    }
}
