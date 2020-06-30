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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.recommendation.LayoutRecommendationItem;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendationManager;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.PushDownProjectConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.source.file.S3KeyCredential;
import io.kyligence.kap.source.file.S3KeyCredentialOperator;
import lombok.val;
import lombok.var;

public class ProjectServiceTest extends ServiceTestBase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";
    private static final String PROJECT_ID = "a8f4da94-a8a4-464b-ab6f-b3012aba04d5";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AsyncTaskService asyncTaskService = Mockito.spy(AsyncTaskService.class);

    @Mock
    private AccessService accessService = Mockito.spy(AccessService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "asyncTaskService", asyncTaskService);
        ReflectionTestUtils.setField(projectService, "accessService", accessService);
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testCreateProject_AutoMaintain_Pass() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project11");
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertTrue(projectInstance2 != null);
        Assert.assertEquals(MaintainModelType.AUTO_MAINTAIN, projectInstance2.getMaintainModelType());
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProject_ManualMaintain_Pass() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project11");
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertNotNull(projectInstance2);
        Assert.assertTrue(projectInstance2.isSemiAutoMode());
        Assert.assertEquals(MaintainModelType.MANUAL_MAINTAIN, projectInstance2.getMaintainModelType());
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectException() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The project named 'default' already exists.");
        projectService.createProject(projectInstance.getName(), projectInstance);

    }

    @Test
    public void testGetReadableProjectsByName() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(PROJECT, true);
        Assert.assertTrue(projectInstances.size() == 1 && projectInstances.get(0).getName().equals(PROJECT));

    }

    @Test
    public void testGetReadableProjectsByFuzzyName() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("TOP", false);
        Assert.assertTrue(projectInstances.size() == 1 && projectInstances.get(0).getName().equals("top_n"));

        projectInstances = projectService.getReadableProjects("not_exist_project", false);
        Assert.assertEquals(0, projectInstances.size());
    }

    @Test
    public void testGetReadableProjects() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(16, projectInstances.size());

    }

    @Test
    public void testGetAdminProjects() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getAdminProjects();
        Assert.assertEquals(16, projectInstances.size());
    }

    @Test
    public void testGetReadableProjects_NoPermission() {
        Mockito.doReturn(false).when(aclEvaluate).hasProjectReadPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects();
        Assert.assertEquals(0, projectInstances.size());
    }

    @Test
    public void testGetReadableProjects_hasNoPermissionProject() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(16, projectInstances.size());

    }

    @Test
    public void testUpdateThreshold() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        projectService.updateQueryAccelerateThresholdConfig(PROJECT, 30, false);
        List<ProjectInstance> projectInstances = projectService.getReadableProjects(PROJECT, false);
        Assert.assertEquals("30",
                projectInstances.get(0).getOverrideKylinProps().get("kylin.favorite.query-accelerate-threshold"));
        Assert.assertEquals("false",
                projectInstances.get(0).getOverrideKylinProps().get("kylin.favorite.query-accelerate-tips-enable"));
    }

    @Test
    public void testGetThreshold() throws Exception {
        val response = projectService.getQueryAccelerateThresholdConfig(PROJECT);
        Assert.assertEquals(20, response.getThreshold());
        Assert.assertTrue(response.isTipsEnabled());
    }

    @Test
    public void testUpdateStorageQuotaConfig() throws Exception {
        thrown.expect(KylinException.class);
        thrown.expectMessage("No valid storage quota size, Please set an integer greater than or equal to 1TB "
                + "to 'storage_quota_size', unit byte.");
        projectService.updateStorageQuotaConfig(PROJECT, 2147483648L);

        projectService.updateStorageQuotaConfig(PROJECT, 1024L * 1024 * 1024 * 1024);
        Assert.assertEquals(1024L * 1024 * 1024 * 1024,
                NProjectManager.getInstance(getTestConfig()).getProject(PROJECT).getConfig().getStorageQuotaSize());

    }

    @Test
    public void testGetStorageVolumeInfoResponse() {
        prepareLayoutHitCount();
        String error = "do not use aclEvalute in getStorageVolumeInfoResponse, because backend thread would invoke this method in (BootstrapCommand.class)";
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectReadPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectOperationPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectWritePermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkProjectAdminPermission(Mockito.any());
        Mockito.doThrow(new RuntimeException(error)).when(aclEvaluate).checkIsGlobalAdmin();
        StorageVolumeInfoResponse storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse(PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfoResponse.getStorageQuotaSize());

        // for MODEL(MODEL_ID) layout-1000001 is manual and auto, it will be consider as manual layout
        Assert.assertEquals(2988131, storageVolumeInfoResponse.getGarbageStorageSize());
    }

    @Test
    public void testCleanupProjectGarbage() throws Exception {
        prepareLayoutHitCount();
        Mockito.doNothing().when(asyncTaskService).cleanupStorage();
        projectService.cleanupGarbage(PROJECT);
        val indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(MODEL_ID);
        Assert.assertEquals(4, indexPlan.getAllLayouts().size());
        val allLayoutsIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertTrue(allLayoutsIds.contains(1L));
        Assert.assertTrue(allLayoutsIds.contains(1000001L));
        Assert.assertTrue(allLayoutsIds.contains(20_000_020_001L));
        Assert.assertTrue(allLayoutsIds.contains(20_000_030_001L));
        // layout 10001 is not connected with a low frequency fq
        Assert.assertFalse(allLayoutsIds.contains(10001L));

        StorageVolumeInfoResponse storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse(PROJECT);
        Assert.assertEquals(0, storageVolumeInfoResponse.getGarbageStorageSize());
    }

    @Test
    public void testScheduledGarbageCleanup() {
        prepareLayoutHitCount();
        val aclManager = AclManager.getInstance(getTestConfig());
        val record = new AclRecord();
        record.setUuid(UUID.randomUUID().toString());

        //project not exist
        val info = new ObjectIdentityImpl("project", PROJECT_ID);
        record.setDomainObjectInfo(info);
        aclManager.save(record);

        var acl = aclManager.get(PROJECT_ID);
        Assert.assertNotNull(acl);

        // when broken model is in MANUAL_MAINTAIN project
        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), "broken_test");
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        // there are no any favorite queries
        projectService.garbageCleanup();
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName());
            if (projectInstance.getName().equals("smart_reuse_existed_models")) {
                continue;
            }
            if (projectInstance.getName().equals("broken_test")) {
                Assert.assertEquals(0, dataflowManager.listUnderliningDataModels(true).size());
            }

            if (projectInstance.getName().equalsIgnoreCase("gc_test")) {
                // more info please refer to ProjectStorageInfoCollectorTest.testSimilarLayoutGcStrategy
                NDataModel model = dataflowManager.listUnderliningDataModels().get(0);
                OptimizeRecommendation optimizeRecommendation = OptimizeRecommendationManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName())
                        .getOptimizeRecommendation(model.getUuid());
                List<LayoutRecommendationItem> layoutRecommendations = optimizeRecommendation
                        .getLayoutRecommendations();
                // default index optimizer will not handle manual layouts.
                Assert.assertEquals(2, layoutRecommendations.size());
                layoutRecommendations.sort(Comparator.comparingLong(rec -> rec.getLayout().getId()));
                Assert.assertEquals(70001L, layoutRecommendations.get(0).getLayout().getId());
                Assert.assertEquals(RecommendationType.REMOVAL, layoutRecommendations.get(0).getRecommendationType());
                Assert.assertEquals(80001L, layoutRecommendations.get(1).getLayout().getId());
                Assert.assertEquals(RecommendationType.REMOVAL, layoutRecommendations.get(1).getRecommendationType());
                continue;
            }

            for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
                NDataflow dataflow = dataflowManager.getDataflow(model.getId());
                IndexPlan indexPlan = dataflow.getIndexPlan();
                if (model.getId().equals(MODEL_ID)) {
                    val layouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                            .collect(Collectors.toSet());
                    Assert.assertEquals(4, layouts.size());
                    Assert.assertEquals(Sets.newHashSet(1L, 1000001L, 20000020001L, 20000030001L), layouts);
                    continue;
                }

                // all ready auto index layouts are deleted
                NDataSegment latestReadySegment = dataflow.getLatestReadySegment();
                if (latestReadySegment != null) {
                    List<LayoutEntity> allLayouts = dataflow.getIndexPlan().getAllLayouts();
                    List<LayoutEntity> autoLayouts = dataflow.getIndexPlan().getWhitelistLayouts();
                    Set<LayoutEntity> manualLayouts = allLayouts.stream().filter(LayoutEntity::isManual)
                            .collect(Collectors.toSet());
                    autoLayouts.removeIf(manualLayouts::contains); // if a layout is manual and auto, consider as manual
                    autoLayouts.removeIf(layout -> !latestReadySegment.getLayoutsMap().containsKey(layout.getId()));
                    allLayouts.removeIf(layout -> !latestReadySegment.getLayoutsMap().containsKey(layout.getId()));
                    Assert.assertTrue(autoLayouts.isEmpty());
                    allLayouts.forEach(layout -> Assert.assertTrue(layout.isManual()));
                }
            }
        }

        acl = AclManager.getInstance(getTestConfig()).get(PROJECT_ID);
        Assert.assertNull(acl);
    }

    private void prepareLayoutHitCount() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);

        long currentTime = System.currentTimeMillis();
        long currentDate = TimeUtil.getDayStart(currentTime);
        long dayInMillis = 24 * 60 * 60 * 1000L;

        dataflowManager.updateDataflow(MODEL_ID, copyForWrite -> {
            copyForWrite.setLayoutHitCount(new HashMap<Long, FrequencyMap>() {
                {
                    put(1L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate - 30 * dayInMillis, 12);
                        }
                    }));
                    put(10001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 7 * dayInMillis, 1);
                            put(currentDate - 30 * dayInMillis, 2);
                        }
                    }));
                    put(1000001L, new FrequencyMap(new TreeMap<Long, Integer>() {
                        {
                            put(currentDate - 30 * dayInMillis, 10);
                        }
                    }));
                }
            });
        });
    }

    @Test
    public void testGetProjectConfig() {
        val project = PROJECT;
        val response = projectService.getProjectConfig(project);
        Assert.assertEquals(20, response.getFavoriteQueryThreshold());
        Assert.assertEquals(true, response.isAutoMergeEnabled());
        Assert.assertEquals(false, response.getRetentionRange().isRetentionRangeEnabled());
        Assert.assertEquals(false, response.isExposeComputedColumn());
    }

    @Test
    public void testJobNotificationConfig() {
        val project = PROJECT;
        var response = projectService.getProjectConfig(project);
        val jobNotificationConfigRequest = new JobNotificationConfigRequest();
        jobNotificationConfigRequest.setDataLoadEmptyNotificationEnabled(false);
        jobNotificationConfigRequest.setJobErrorNotificationEnabled(false);
        jobNotificationConfigRequest.setJobNotificationEmails(
                Lists.newArrayList("user1@kyligence.io", "user2@kyligence.io", "user2@kyligence.io"));
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(2, response.getJobNotificationEmails().size());
        Assert.assertEquals(false, response.isJobErrorNotificationEnabled());
        Assert.assertEquals(false, response.isDataLoadEmptyNotificationEnabled());

        jobNotificationConfigRequest
                .setJobNotificationEmails(Lists.newArrayList("@kyligence.io", "user2@.io", "user2@kyligence.io"));
        thrown.expect(KylinException.class);
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        thrown = ExpectedException.none();
    }

    @Test
    public void testUpdateProjectConfig() throws IOException {
        val project = PROJECT;

        val description = "test description";
        val request = new ProjectGeneralInfoRequest();
        request.setDescription(description);
        projectService.updateProjectGeneralInfo(project, request);
        var response = projectService.getProjectConfig(project);
        Assert.assertEquals(description, response.getDescription());

        request.setSemiAutoMode(true);
        projectService.updateProjectGeneralInfo(project, request);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(false, response.isSemiAutomaticMode());

        Assert.assertNull(response.getDefaultDatabase());
        projectService.updateDefaultDatabase(project, "EDW");
        Assert.assertEquals("EDW", projectService.getProjectConfig(project).getDefaultDatabase());

        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.DAY));
        projectService.updateSegmentConfig(project, segmentConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(false, response.isAutoMergeEnabled());

        val pushDownConfigRequest = new PushDownConfigRequest();
        pushDownConfigRequest.setPushDownEnabled(false);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(false, response.isPushDownEnabled());

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.smart.query.mockup.MockupPushDownRunner");
        pushDownConfigRequest.setPushDownEnabled(true);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(true, response.isPushDownEnabled());

        // this config should not expose to end users.
        val shardNumConfigRequest = new ShardNumConfigRequest();
        Map<String, String> map = new HashMap<>();
        map.put("DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "100");
        map.put("DEFAULT.TEST_KYLIN_FACT.SELLER_ID", "50");
        shardNumConfigRequest.setColToNum(map);
        projectService.updateShardNumConfig(project, shardNumConfigRequest);
        val pi = NProjectManager.getInstance(getTestConfig()).getProject(project);
        Assert.assertEquals(
                JsonUtil.readValueAsMap(pi.getConfig().getExtendedOverrides().get("kylin.engine.shard-num-json")), map);

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        pushDownConfigRequest.setPushDownEnabled(true);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        val projectConfig = projectManager.getProject(project).getConfig();
        Assert.assertTrue(projectConfig.isPushDownEnabled());
        Assert.assertEquals(PushDownRunnerSparkImpl.class.getName(), projectConfig.getPushDownRunnerClassName());

        val pushDownProjectConfigRequest = new PushDownProjectConfigRequest();
        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        getTestConfig().setProperty("kylin.query.pushdown.converter-class-name", "");
        pushDownProjectConfigRequest.setRunnerClassName("io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl");
        pushDownProjectConfigRequest.setConverterClassNames("org.apache.kylin.query.util.PowerBIConverter");
        projectService.updatePushDownProjectConfig(project, pushDownProjectConfigRequest);
        String[] converterClassNames = new String[] { "org.apache.kylin.query.util.PowerBIConverter" };
        // response
        response = projectService.getProjectConfig(project);
        Assert.assertEquals("io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl", response.getRunnerClassName());
        Assert.assertEquals(String.join(",", converterClassNames), response.getConverterClassNames());
        // project config
        val projectConfig2 = projectManager.getProject(project).getConfig();
        Assert.assertEquals("io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl",
                projectConfig2.getPushDownRunnerClassName());
        Assert.assertArrayEquals(converterClassNames, projectConfig2.getPushDownConverterClassNames());
    }

    @Test
    public void testDropProject() {
        val project = "project12";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);

        val eventOrchestratorManager = EventOrchestratorManager.getInstance(getTestConfig());
        eventOrchestratorManager.addProject(project);
        Assert.assertNotNull(eventOrchestratorManager.getEventOrchestratorByProject(project));
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        Assert.assertTrue(scheduler.hasStarted());

        NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
        favoriteScheduler.init();
        Assert.assertTrue(favoriteScheduler.hasStarted());
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.dropProject(project);
            return null;
        }, project);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(eventOrchestratorManager.getEventOrchestratorByProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testClearManagerCache() throws NoSuchFieldException, IllegalAccessException {
        val config = getTestConfig();
        val modelManager = NDataModelManager.getInstance(config, "default");
        Field filed = KylinConfig.class.getDeclaredField("managersCache");
        Field filed2 = KylinConfig.class.getDeclaredField("managersByPrjCache");
        filed.setAccessible(true);
        filed2.setAccessible(true);
        ConcurrentHashMap<Class, Object> managersCache = (ConcurrentHashMap<Class, Object>) filed.get(config);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed2
                .get(config);

        Assert.assertTrue(managersByPrjCache.containsKey(NDataModelManager.class));
        Assert.assertTrue(managersByPrjCache.get(NDataModelManager.class).containsKey("default"));

        Assert.assertTrue(managersCache.containsKey(NProjectManager.class));
        projectService.clearManagerCache("default");

        managersCache = (ConcurrentHashMap<Class, Object>) filed.get(config);
        managersByPrjCache = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) filed2.get(config);
        //cleared
        Assert.assertTrue(!managersCache.containsKey(NProjectManager.class));
        Assert.assertTrue(!managersByPrjCache.get(NDataModelManager.class).containsKey("default"));

    }

    @Test
    public void testSetDataSourceType() {
        projectService.setDataSourceType("default", "11");
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        val prj = prjMgr.getProject("default");
        Assert.assertEquals(11, prj.getSourceType());
    }

    @Test
    public void testUpdateGarbageCleanupConfig() {
        val request = new GarbageCleanUpConfigRequest();
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK);
        request.setLowFrequencyThreshold(12L);
        projectService.updateGarbageCleanupConfig("default", request);
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        val prj = prjMgr.getProject("default");
        Assert.assertEquals(7, prj.getConfig().getFrequencyTimeWindowInDays());
        Assert.assertEquals(12, prj.getConfig().getLowFrequencyThreshold());
    }

    @Test
    public void testUpdateFileSourceCredential() {
        S3KeyCredentialOperator operator = new S3KeyCredentialOperator();
        S3KeyCredential s3KeyCredential = new S3KeyCredential();
        s3KeyCredential.setAccessKey("mockAccessKey");
        s3KeyCredential.setSecretKey("mockSecretKey");
        operator.setCredential(s3KeyCredential);
        projectService.updateFileSourceCredential(PROJECT, operator);
        Assert.assertEquals("AWS_S3_KEY",
                projectManager.getProject(PROJECT).getOverrideKylinProps().get("kylin.source.credential.type"));
        Assert.assertEquals("13",
                projectManager.getProject(PROJECT).getOverrideKylinProps().get("kylin.source.default"));
        Assert.assertEquals("{\"accessKey\":\"mockAccessKey\",\"secretKey\":\"mockSecretKey\",\"type\":\"AWS_S3_KEY\"}",
                projectManager.getProject(PROJECT).getOverrideKylinProps().get("kylin.source.credential.value"));
    }

    private void updateProject() {
        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.YEAR));
        projectService.updateSegmentConfig(PROJECT, segmentConfigRequest);

        val jobNotificationConfigRequest = new JobNotificationConfigRequest();
        jobNotificationConfigRequest.setDataLoadEmptyNotificationEnabled(true);
        jobNotificationConfigRequest.setJobErrorNotificationEnabled(true);
        jobNotificationConfigRequest.setJobNotificationEmails(
                Lists.newArrayList("user1@kyligence.io", "user2@kyligence.io", "user2@kyligence.io"));
        projectService.updateJobNotificationConfig(PROJECT, jobNotificationConfigRequest);

        projectService.updateQueryAccelerateThresholdConfig(PROJECT, 30, false);

        val request = new GarbageCleanUpConfigRequest();
        request.setFrequencyTimeWindow(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK);
        request.setLowFrequencyThreshold(12L);
        projectService.updateGarbageCleanupConfig("default", request);
    }

    @Test
    public void testResetProjectConfig() {
        updateProject();
        var response = projectService.getProjectConfig(PROJECT);
        Assert.assertEquals(2, response.getJobNotificationEmails().size());
        Assert.assertTrue(response.isJobErrorNotificationEnabled());
        Assert.assertTrue(response.isDataLoadEmptyNotificationEnabled());

        response = projectService.resetProjectConfig(PROJECT, "job_notification_config");
        Assert.assertEquals(0, response.getJobNotificationEmails().size());
        Assert.assertFalse(response.isJobErrorNotificationEnabled());
        Assert.assertFalse(response.isDataLoadEmptyNotificationEnabled());

        Assert.assertFalse(response.isFavoriteQueryTipsEnabled());
        Assert.assertEquals(30, response.getFavoriteQueryThreshold());
        Assert.assertEquals(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.WEEK.name(),
                response.getFrequencyTimeWindow());
        Assert.assertEquals(12, response.getLowFrequencyThreshold());
        Assert.assertFalse(response.isAutoMergeEnabled());

        response = projectService.resetProjectConfig(PROJECT, "query_accelerate_threshold");
        Assert.assertTrue(response.isFavoriteQueryTipsEnabled());
        Assert.assertEquals(20, response.getFavoriteQueryThreshold());

        response = projectService.resetProjectConfig(PROJECT, "garbage_cleanup_config");
        Assert.assertEquals(GarbageCleanUpConfigRequest.FrequencyTimeWindowEnum.MONTH.name(),
                response.getFrequencyTimeWindow());
        Assert.assertEquals(5, response.getLowFrequencyThreshold());

        response = projectService.resetProjectConfig(PROJECT, "segment_config");
        Assert.assertTrue(response.isAutoMergeEnabled());
        Assert.assertEquals(4, response.getAutoMergeTimeRanges().size());

        response = projectService.resetProjectConfig(PROJECT, "storage_quota_config");
        Assert.assertEquals(10995116277760L, response.getStorageQuotaSize());
    }

    @Test
    public void testUpdateYarnQueue() throws Exception {
        final String updateTo = "q.queue";
        Assert.assertEquals("default", projectService.getProjectConfig(PROJECT).getYarnQueue());
        projectService.updateYarnQueue(PROJECT, updateTo);
        Assert.assertEquals(updateTo, projectService.getProjectConfig(PROJECT).getYarnQueue());
        Assert.assertEquals(updateTo, NProjectManager.getInstance(getTestConfig()).getProject(PROJECT).getConfig()
                .getOptional("kylin.engine.spark-conf.spark.yarn.queue", ""));
    }

    @Test
    public void testCreateProjectComputedColumnConfig() throws Exception {
        // auto
        {
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setName("project11");
            projectInstance.setMaintainModelType(MaintainModelType.AUTO_MAINTAIN);
            UnitOfWork.doInTransactionWithRetry(() -> {
                projectService.createProject(projectInstance.getName(), projectInstance);
                return null;
            }, projectInstance.getName());
            ProjectInstance projectInstance2 = projectManager.getProject("project11");
            Assert.assertNotNull(projectInstance2);
            Assert.assertEquals(MaintainModelType.AUTO_MAINTAIN, projectInstance2.getMaintainModelType());
            Assert.assertFalse(projectInstance2.getConfig().exposeComputedColumn());
            projectManager.dropProject("project11");
        }

        // manual
        {
            ProjectInstance projectInstance = new ProjectInstance();
            projectInstance.setName("project11");
            projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
            UnitOfWork.doInTransactionWithRetry(() -> {
                projectService.createProject(projectInstance.getName(), projectInstance);
                return null;
            }, projectInstance.getName());
            ProjectInstance projectInstance2 = projectManager.getProject("project11");
            Assert.assertNotNull(projectInstance2);
            Assert.assertEquals(MaintainModelType.MANUAL_MAINTAIN, projectInstance2.getMaintainModelType());
            Assert.assertTrue(projectInstance2.getConfig().exposeComputedColumn());
            projectManager.dropProject("project11");
        }
    }

    @Test
    public void testUpdateProjectOwner() throws IOException {
        String project = "default";
        String owner = "test";

        // normal case
        Set<String> projectAdminUsers1 = Sets.newHashSet();
        projectAdminUsers1.add("test");
        Mockito.doReturn(projectAdminUsers1).when(accessService).getProjectAdminUsers(project);

        OwnerChangeRequest ownerChangeRequest1 = new OwnerChangeRequest();
        ownerChangeRequest1.setOwner(owner);

        projectService.updateProjectOwner(project, ownerChangeRequest1);
        ProjectInstance projectInstance = projectManager.getProject(project);
        Assert.assertEquals(owner, projectInstance.getOwner());

        // user not exists
        ownerChangeRequest1.setOwner("nonUser");
        thrown.expectMessage("Illegal users!"
                + " Only the system administrator and the project administrator role of this project can be set as the project owner.");
        projectService.updateProjectOwner(project, ownerChangeRequest1);

        // empty admin users, throw exception
        Set<String> projectAdminUsers = Sets.newHashSet();
        Mockito.doReturn(projectAdminUsers).when(accessService).getProjectAdminUsers(project);

        OwnerChangeRequest ownerChangeRequest2 = new OwnerChangeRequest();
        ownerChangeRequest2.setOwner(owner);

        thrown.expectMessage("Illegal users!"
                + " Only the system administrator and the project administrator role of this project can be set as the project owner.");
        projectService.updateProjectOwner(project, ownerChangeRequest2);
    }

    @Test
    public void testGetRulesWithError() {
        // assert get rule error
        try {
            projectService.getFavoriteRules(PROJECT_NEWTEN);
        } catch (Throwable ex) {
            Assert.assertEquals(NotFoundException.class, ex.getClass());
            Assert.assertEquals(
                    String.format(MsgPicker.getMsg().getFAVORITE_RULE_NOT_FOUND(), FavoriteRule.COUNT_RULE_NAME),
                    ex.getMessage());
        }
    }

    public void testGetFavoriteRules() {
        Map<String, Object> favoriteRuleResponse = projectService.getFavoriteRules(PROJECT);
        Assert.assertEquals(true, favoriteRuleResponse.get("count_enable"));
        Assert.assertEquals(10.0f, favoriteRuleResponse.get("count_value"));
        Assert.assertEquals(Lists.newArrayList("userA", "userB", "userC"), favoriteRuleResponse.get("users"));
        Assert.assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRuleResponse.get("user_groups"));
        Assert.assertEquals(5L, favoriteRuleResponse.get("min_duration"));
        Assert.assertEquals(8L, favoriteRuleResponse.get("max_duration"));
        Assert.assertEquals(true, favoriteRuleResponse.get("duration_enable"));
    }

    @Test
    public void testUpdateFavoriteRules() {
        // update with FavoriteRuleUpdateRequest and assert
        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject(PROJECT);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(false);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        projectService.updateRegularRule(PROJECT, request);
        Map<String, Object> favoriteRuleResponse = projectService.getFavoriteRules(PROJECT);
        Assert.assertEquals(false, favoriteRuleResponse.get("duration_enable"));
        Assert.assertEquals(false, favoriteRuleResponse.get("submitter_enable"));
        Assert.assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        Assert.assertEquals(Lists.newArrayList(), favoriteRuleResponse.get("user_groups"));
        Assert.assertEquals(0L, favoriteRuleResponse.get("min_duration"));
        Assert.assertEquals(10L, favoriteRuleResponse.get("max_duration"));
        Assert.assertEquals(true, favoriteRuleResponse.get("recommendation_enable"));
        Assert.assertEquals(30L, favoriteRuleResponse.get("recommendations_value"));

        // check user_groups
        request.setUserGroups(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"));
        projectService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectService.getFavoriteRules(PROJECT);
        Assert.assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        Assert.assertEquals(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"), favoriteRuleResponse.get("user_groups"));

        // assert if favorite rules' values are empty
        request.setFreqEnable(false);
        request.setFreqValue(null);
        request.setDurationEnable(false);
        request.setMinDuration(null);
        request.setMaxDuration(null);
        projectService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectService.getFavoriteRules(PROJECT);
        Assert.assertNull(favoriteRuleResponse.get("freq_value"));
        Assert.assertNull(favoriteRuleResponse.get("min_duration"));
        Assert.assertNull(favoriteRuleResponse.get("max_duration"));
    }

    @Test
    public void testResetFavoriteRules() {
        // reset
        projectService.resetProjectConfig(PROJECT, "favorite_rule_config");
        Map<String, Object> favoriteRules = projectService.getFavoriteRules(PROJECT);

        Assert.assertEquals(false, favoriteRules.get("freq_enable"));
        Assert.assertNull(favoriteRules.get("freq_value"));

        Assert.assertEquals(true, favoriteRules.get("count_enable"));
        Assert.assertEquals(10.0f, favoriteRules.get("count_value"));

        Assert.assertEquals(true, favoriteRules.get("submitter_enable"));
        Assert.assertEquals(Lists.newArrayList("ADMIN"), favoriteRules.get("users"));
        Assert.assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRules.get("user_groups"));

        Assert.assertEquals(false, favoriteRules.get("duration_enable"));
        Assert.assertNull(favoriteRules.get("min_duration"));
        Assert.assertNull(favoriteRules.get("max_duration"));

        Assert.assertEquals(true, favoriteRules.get("recommendation_enable"));
        Assert.assertEquals(20L, favoriteRules.get("recommendations_value"));
    }
}
