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

package io.kyligence.kap.rest.service;

import static io.kyligence.kap.common.constant.Constants.HIDDEN_VALUE;
import static io.kyligence.kap.common.constant.Constants.KYLIN_SOURCE_JDBC_DRIVER_KEY;
import static io.kyligence.kap.common.constant.Constants.KYLIN_SOURCE_JDBC_PASS_KEY;
import static io.kyligence.kap.metadata.model.MaintainModelType.MANUAL_MAINTAIN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRuleUpdateRequest;
import org.apache.kylin.rest.response.UserProjectPermissionResponse;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclPermissionEnum;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JdbcRequest;
import io.kyligence.kap.rest.request.JdbcSourceInfoRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.MultiPartitionConfigRequest;
import io.kyligence.kap.rest.request.OwnerChangeRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.PushDownProjectConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.request.ShardNumConfigRequest;
import io.kyligence.kap.rest.response.ProjectStatisticsResponse;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.rest.service.task.RecommendationTopNUpdateScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.val;
import lombok.var;

public class ProjectServiceTest extends ServiceTestBase {
    private static final String PROJECT = "default";
    private static final String PROJECT_NEWTEN = "newten";
    private static final String PROJECT_JDBC = "jdbc";
    private static final String PROJECT_ID = "a8f4da94-a8a4-464b-ab6f-b3012aba04d5";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private final ProjectSmartService projectSmartService = Mockito.spy(ProjectSmartService.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @InjectMocks
    private final RawRecService rawRecService = Mockito.spy(RawRecService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AsyncTaskService asyncTaskService = Mockito.spy(AsyncTaskService.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private final UserService userService = Mockito.spy(UserService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "asyncTaskService", asyncTaskService);
        ReflectionTestUtils.setField(projectService, "accessService", accessService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        ReflectionTestUtils.setField(projectService, "userService", userService);
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);


        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                new RecommendationTopNUpdateScheduler());
        ReflectionTestUtils.setField(projectSmartService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectSmartService, "projectSmartSupporter", rawRecService);
        ReflectionTestUtils.setField(projectSmartService, "projectModelSupporter", modelService);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testCreateProject_SemiMode() {
        ProjectInstance projectInstance = new ProjectInstance();
        KylinConfig.getInstanceFromEnv().setProperty("kylin.metadata.semi-automatic-mode", "true");
        projectInstance.setName("project11");
        projectInstance.setMaintainModelType(MANUAL_MAINTAIN);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertTrue(projectInstance2 != null);
        Assert.assertEquals("true", projectInstance2.getOverrideKylinProps().get("kylin.metadata.semi-automatic-mode"));
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProject_AutoMaintain_Pass() {

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
        projectInstance.setMaintainModelType(MANUAL_MAINTAIN);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertNotNull(projectInstance2);
        Assert.assertFalse(projectInstance2.isSemiAutoMode());
        Assert.assertEquals(MANUAL_MAINTAIN, projectInstance2.getMaintainModelType());
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectException() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT);
        thrown.expect(KylinException.class);
        thrown.expectMessage("The project name \"default\" already exists. Please rename it.");
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
        Assert.assertEquals(27, projectInstances.size());
    }

    @Test
    public void testGetAdminProjects() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getAdminProjects();
        Assert.assertEquals(27, projectInstances.size());
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
        Assert.assertEquals(27, projectInstances.size());

    }

    @Test
    public void testGetProjectsWrapWIthUserPermission() throws Exception {
        Mockito.doReturn(Mockito.mock(UserDetails.class)).when(userService).loadUserByUsername(Mockito.anyString());
        Mockito.doReturn(true).when(userService).isGlobalAdmin(Mockito.any(UserDetails.class));
        List<UserProjectPermissionResponse> projectInstances = projectService
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission("default", true,
                        AclPermissionEnum.READ);
        Assert.assertEquals(1, projectInstances.size());
        Assert.assertEquals("ADMINISTRATION", projectInstances.get(0).getPermission());
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
    @Ignore
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
    @Ignore
    public void testScheduledGarbageCleanup() {
        prepareLayoutHitCount();
        val aclManager = AclManager.getInstance(getTestConfig());
        val record = new AclRecord();
        record.setUuid(RandomUtil.randomUUIDStr());

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
                continue;
            }

            for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
                if (model.isMultiPartitionModel()) {
                    continue;
                }
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
        Assert.assertEquals(false, response.isAutoMergeEnabled());
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
        Assert.assertFalse(response.isJobErrorNotificationEnabled());
        Assert.assertFalse(response.isDataLoadEmptyNotificationEnabled());

        jobNotificationConfigRequest
                .setJobNotificationEmails(Lists.newArrayList("@kyligence.io", "user2@.io", "user2@kyligence.io"));
        thrown.expect(KylinException.class);
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        thrown = ExpectedException.none();
    }

    @Test
    public void testUpdateSegmentConfigWhenError() {
        val project = PROJECT;

        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setAutoMergeTimeRanges(Arrays.asList(AutoMergeTimeEnum.DAY));

        segmentConfigRequest.getRetentionRange().setRetentionRangeType(null);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains(
                    "No valid value for 'retention_range_type', Please set {'DAY', 'MONTH', 'YEAR'} to specify the period of retention."));
        }
        segmentConfigRequest.getVolatileRange().setVolatileRangeNumber(-1);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage()
                    .contains("No valid value. Please set an integer 'x' to "
                            + "'volatile_range_number'. The 'Auto-Merge' will not merge latest 'x' "
                            + "period(day/week/month/etc..) segments."));
        }
        segmentConfigRequest.getVolatileRange().setVolatileRangeNumber(1);
        segmentConfigRequest.getRetentionRange().setRetentionRangeNumber(-1);
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("No valid value for 'retention_range_number'."
                    + " Please set an integer 'x' to specify the retention threshold. The system will "
                    + "only retain the segments in the retention threshold (x years before the last data time). "));
        }
        segmentConfigRequest.getRetentionRange().setRetentionRangeNumber(1);
        segmentConfigRequest.setAutoMergeTimeRanges(new ArrayList<>());
        try {
            projectService.updateSegmentConfig(project, segmentConfigRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertTrue(e.getMessage().contains("No valid value for 'auto_merge_time_ranges'. Please set "
                    + "{'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'} to specify the period of auto-merge. "));
        }
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
    public void testUpdateProjectConfig_trim() {

        Map<String, String> testOverrideP = Maps.newLinkedHashMap();
        testOverrideP.put(" testk1 ", " testv1 ");
        testOverrideP.put("tes   tk2", "test    v2");
        testOverrideP.put("      tes    tk3 ", "    t     estv3    ");

        projectService.updateProjectConfig(PROJECT, testOverrideP);

        val kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();

        Assert.assertEquals("testv1", kylinConfigExt.get("testk1"));
        Assert.assertEquals("test    v2", kylinConfigExt.get("tes   tk2"));
        Assert.assertEquals("t     estv3", kylinConfigExt.get("tes    tk3"));
    }

    @Test
    public void testDeleteProjectConfig() {
        Map<String, String> testOverrideP = Maps.newLinkedHashMap();
        testOverrideP.put("testk1", "testv1");

        projectService.updateProjectConfig(PROJECT, testOverrideP);

        var kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();
        Assert.assertEquals("testv1", kylinConfigExt.get("testk1"));

        projectService.deleteProjectConfig(PROJECT, "testk1");

        kylinConfigExt = projectManager.getProject(PROJECT).getConfig().getExtendedOverrides();
        Assert.assertNull(kylinConfigExt.get("testk1"));
    }

    @Test
    public void testMultiPartitionConfig() {
        val project = PROJECT;
        val modelId = "b780e4e4-69af-449e-b09f-05c90dfa04b6";
        NDataflowManager dfm = NDataflowManager.getInstance(getTestConfig(), project);

        MultiPartitionConfigRequest request1 = new MultiPartitionConfigRequest(true);
        projectService.updateMultiPartitionConfig(project, request1, modelService);
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dfm.getDataflow(modelId).getStatus());

        MultiPartitionConfigRequest request2 = new MultiPartitionConfigRequest(false);
        projectService.updateMultiPartitionConfig(project, request2, modelService);
        Assert.assertEquals(RealizationStatusEnum.OFFLINE, dfm.getDataflow(modelId).getStatus());
    }

    @Test
    public void testDropProject() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project12";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.dropProject(project);
            return null;
        }, project);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testDropStreamingProject() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.RUNNING);
                Mockito.when(projectService.getStreamingJobManager(Mockito.anyString())).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Arrays.asList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.STARTING);
                Mockito.when(projectService.getStreamingJobManager(Mockito.anyString())).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Arrays.asList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            try {
                val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
                val meta1 = new StreamingJobMeta();
                meta1.setCurrentStatus(JobStatusEnum.STARTING);
                Mockito.when(projectService.getStreamingJobManager(Mockito.anyString())).thenReturn(mgr);
                Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Arrays.asList(meta1));
                projectService.dropProject(project);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals("KE-010037009", ((KylinException) e).getErrorCode().getCodeString());
            }
            return null;
        }, project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val mgr = Mockito.spy(StreamingJobManager.getInstance(getTestConfig(), PROJECT));
            val meta1 = new StreamingJobMeta();
            meta1.setCurrentStatus(JobStatusEnum.STOPPED);
            Mockito.when(projectService.getStreamingJobManager(Mockito.anyString())).thenReturn(mgr);
            Mockito.when(mgr.listAllStreamingJobMeta()).thenReturn(Arrays.asList(meta1));
            projectService.dropProject(project);
            return null;
        }, project);
    }

    @Test
    public void testDropProjectWithAllJobsBeenKilled() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);

        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()));
        Assert.assertTrue(scheduler.hasStarted());
        NExecutableManager jobMgr = NExecutableManager.getInstance(getTestConfig(), project);

        val job1 = new DefaultChainedExecutable();
        job1.setProject(project);
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);

        jobMgr.updateJobOutput(job1.getId(), ExecutableState.DISCARDED, null, null, null);

        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.dropProject(project);
            return null;
        }, project);
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testDropProjectWithoutAllJobsBeenKilled() {
        KylinConfig.getInstanceFromEnv().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,DATABASE_TO_UPPER=FALSE,username=sa,password=");
        val project = "project13";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(project, projectInstance);
            return null;
        }, project);

        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()));
        Assert.assertTrue(scheduler.hasStarted());
        NExecutableManager jobMgr = NExecutableManager.getInstance(getTestConfig(), project);

        val job1 = new DefaultChainedExecutable();
        job1.setProject(project);
        val task1 = new ShellExecutable();
        job1.addTask(task1);
        jobMgr.addJob(job1);

        val job2 = new DefaultChainedExecutable();
        job2.setProject(project);
        val task2 = new ShellExecutable();
        job2.addTask(task2);
        jobMgr.addJob(job2);

        val job3 = new DefaultChainedExecutable();
        job3.setProject(project);
        val task3 = new ShellExecutable();
        job3.addTask(task3);
        jobMgr.addJob(job3);

        jobMgr.updateJobOutput(job2.getId(), ExecutableState.RUNNING, null, null, null);
        jobMgr.updateJobOutput(job3.getId(), ExecutableState.PAUSED, null, null, null);

        Assert.assertThrows(KylinException.class, () -> projectService.dropProject(project));
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNotNull(prjManager.getProject(project));
        Assert.assertNotNull(NDefaultScheduler.getInstanceByProject(project));
    }

    @Test
    public void testClearManagerCache() throws Exception {
        val config = getTestConfig();
        val modelManager = NDataModelManager.getInstance(config, "default");
        ConcurrentHashMap<Class, Object> managersCache = getInstances();
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = getInstanceByProject();

        Assert.assertTrue(managersByPrjCache.containsKey(NDataModelManager.class));
        Assert.assertTrue(managersByPrjCache.get(NDataModelManager.class).containsKey("default"));

        Assert.assertTrue(managersCache.containsKey(NProjectManager.class));
        projectService.clearManagerCache("default");

        managersCache = getInstances();
        managersByPrjCache = getInstanceByProject();
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
        Assert.assertFalse(response.isAutoMergeEnabled());
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
            projectInstance.setMaintainModelType(MANUAL_MAINTAIN);
            UnitOfWork.doInTransactionWithRetry(() -> {
                projectService.createProject(projectInstance.getName(), projectInstance);
                return null;
            }, projectInstance.getName());
            ProjectInstance projectInstance2 = projectManager.getProject("project11");
            Assert.assertNotNull(projectInstance2);
            Assert.assertEquals(MANUAL_MAINTAIN, projectInstance2.getMaintainModelType());
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
        thrown.expectMessage(
                "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.");
        projectService.updateProjectOwner(project, ownerChangeRequest1);

        // empty admin users, throw exception
        Set<String> projectAdminUsers = Sets.newHashSet();
        Mockito.doReturn(projectAdminUsers).when(accessService).getProjectAdminUsers(project);

        OwnerChangeRequest ownerChangeRequest2 = new OwnerChangeRequest();
        ownerChangeRequest2.setOwner(owner);

        thrown.expectMessage(
                "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.");
        projectService.updateProjectOwner(project, ownerChangeRequest2);
    }

    public void testGetFavoriteRules() {
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
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
        RecommendationTopNUpdateScheduler recommendationTopNUpdateScheduler = new RecommendationTopNUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                recommendationTopNUpdateScheduler);
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
        request.setMinHitCount("11");
        request.setEffectiveDays("11");
        request.setUpdateFrequency("3");

        projectSmartService.updateRegularRule(PROJECT, request);
        Map<String, Object> favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertEquals(false, favoriteRuleResponse.get("duration_enable"));
        Assert.assertEquals(false, favoriteRuleResponse.get("submitter_enable"));
        Assert.assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        Assert.assertEquals(Lists.newArrayList(), favoriteRuleResponse.get("user_groups"));
        Assert.assertEquals(0L, favoriteRuleResponse.get("min_duration"));
        Assert.assertEquals(10L, favoriteRuleResponse.get("max_duration"));
        Assert.assertEquals(true, favoriteRuleResponse.get("recommendation_enable"));
        Assert.assertEquals(30L, favoriteRuleResponse.get("recommendations_value"));
        Assert.assertEquals(false, favoriteRuleResponse.get("excluded_tables_enable"));
        Assert.assertEquals("", favoriteRuleResponse.get("excluded_tables"));
        Assert.assertEquals(11, favoriteRuleResponse.get("min_hit_count"));
        Assert.assertEquals(11, favoriteRuleResponse.get("effective_days"));
        Assert.assertEquals(3, favoriteRuleResponse.get("update_frequency"));

        // check excluded_tables
        request.setExcludeTablesEnable(true);
        request.setExcludedTables("a.a,b.b,c.c");
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertEquals(true, favoriteRuleResponse.get("excluded_tables_enable"));
        Assert.assertEquals("a.a,b.b,c.c", favoriteRuleResponse.get("excluded_tables"));
        // check excluded_tables
        request.setExcludeTablesEnable(false);
        request.setExcludedTables(null);
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertEquals(false, favoriteRuleResponse.get("excluded_tables_enable"));
        Assert.assertEquals("", favoriteRuleResponse.get("excluded_tables"));

        // check user_groups
        request.setUserGroups(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"));
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertEquals(Lists.newArrayList("userA", "userB", "userC", "ADMIN"), favoriteRuleResponse.get("users"));
        Assert.assertEquals(Lists.newArrayList("ROLE_ADMIN", "USER_GROUP1"), favoriteRuleResponse.get("user_groups"));

        // assert if favorite rules' values are empty
        request.setFreqEnable(false);
        request.setFreqValue(null);
        request.setDurationEnable(false);
        request.setMinDuration(null);
        request.setMaxDuration(null);
        projectSmartService.updateRegularRule(PROJECT, request);
        favoriteRuleResponse = projectSmartService.getFavoriteRules(PROJECT);
        Assert.assertNull(favoriteRuleResponse.get("freq_value"));
        Assert.assertNull(favoriteRuleResponse.get("min_duration"));
        Assert.assertNull(favoriteRuleResponse.get("max_duration"));
        recommendationTopNUpdateScheduler.close();
    }

    @Test
    public void testResetFavoriteRules() {
        // reset
        projectService.resetProjectConfig(PROJECT, "favorite_rule_config");
        Map<String, Object> favoriteRules = projectSmartService.getFavoriteRules(PROJECT);

        Assert.assertEquals(false, favoriteRules.get("freq_enable"));
        Assert.assertEquals(0.1f, favoriteRules.get("freq_value"));

        Assert.assertEquals(true, favoriteRules.get("count_enable"));
        Assert.assertEquals(10.0f, favoriteRules.get("count_value"));

        Assert.assertEquals(true, favoriteRules.get("submitter_enable"));
        Assert.assertEquals(Lists.newArrayList("ADMIN"), favoriteRules.get("users"));
        Assert.assertEquals(Lists.newArrayList("ROLE_ADMIN"), favoriteRules.get("user_groups"));

        Assert.assertEquals(false, favoriteRules.get("duration_enable"));
        Assert.assertEquals(0L, favoriteRules.get("min_duration"));
        Assert.assertEquals(180L, favoriteRules.get("max_duration"));

        Assert.assertEquals(true, favoriteRules.get("recommendation_enable"));
        Assert.assertEquals(20L, favoriteRules.get("recommendations_value"));

        Assert.assertEquals(false, favoriteRules.get("excluded_tables_enable"));
        Assert.assertEquals("", favoriteRules.get("excluded_tables"));

        Assert.assertEquals(30, favoriteRules.get("min_hit_count"));
        Assert.assertEquals(2, favoriteRules.get("effective_days"));
        Assert.assertEquals(2, favoriteRules.get("update_frequency"));

    }

    @Test
    public void testGetProjectStatistics() {
        RecommendationTopNUpdateScheduler recommendationTopNUpdateScheduler = new RecommendationTopNUpdateScheduler();
        ReflectionTestUtils.setField(projectSmartService, "recommendationTopNUpdateScheduler",
                recommendationTopNUpdateScheduler);
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("gc_test");
        Assert.assertEquals(1, projectStatistics.getDatabaseSize());
        Assert.assertEquals(1, projectStatistics.getTableSize());
        Assert.assertEquals(0, projectStatistics.getLastWeekQueryCount());
        Assert.assertEquals(0, projectStatistics.getUnhandledQueryCount());
        Assert.assertEquals(0, projectStatistics.getAdditionalRecPatternCount());
        Assert.assertEquals(0, projectStatistics.getRemovalRecPatternCount());
        Assert.assertEquals(0, projectStatistics.getRecPatternCount());
        Assert.assertEquals(7, projectStatistics.getEffectiveRuleSize());
        Assert.assertEquals(0, projectStatistics.getApprovedRecCount());
        Assert.assertEquals(0, projectStatistics.getApprovedAdditionalRecCount());
        Assert.assertEquals(0, projectStatistics.getApprovedRemovalRecCount());
        Assert.assertEquals(2, projectStatistics.getModelSize());
        Assert.assertEquals(0, projectStatistics.getAcceptableRecSize());
        Assert.assertFalse(projectStatistics.isRefreshed());
        Assert.assertEquals(20, projectStatistics.getMaxRecShowSize());

        FavoriteRuleUpdateRequest request = new FavoriteRuleUpdateRequest();
        request.setProject("gc_test");
        request.setExcludeTablesEnable(true);
        request.setDurationEnable(false);
        request.setMinDuration("0");
        request.setMaxDuration("10");
        request.setSubmitterEnable(true);
        request.setUsers(Lists.newArrayList("userA", "userB", "userC", "ADMIN"));
        request.setRecommendationEnable(true);
        request.setRecommendationsValue("30");
        request.setUpdateFrequency("1");
        projectSmartService.updateRegularRule("gc_test", request);
        ProjectStatisticsResponse projectStatistics2 = projectSmartService.getProjectStatistics("gc_test");
        Assert.assertEquals(7, projectStatistics2.getEffectiveRuleSize());

        ProjectStatisticsResponse statisticsOfProjectDefault = projectSmartService.getProjectStatistics(PROJECT);
        Assert.assertEquals(3, statisticsOfProjectDefault.getDatabaseSize());
        Assert.assertEquals(20, statisticsOfProjectDefault.getTableSize());
        Assert.assertEquals(0, statisticsOfProjectDefault.getLastWeekQueryCount());
        Assert.assertEquals(0, statisticsOfProjectDefault.getUnhandledQueryCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getAdditionalRecPatternCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getRemovalRecPatternCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getRecPatternCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getEffectiveRuleSize());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getApprovedRecCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getApprovedAdditionalRecCount());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getApprovedRemovalRecCount());
        Assert.assertEquals(8, statisticsOfProjectDefault.getModelSize());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getAcceptableRecSize());
        Assert.assertFalse(statisticsOfProjectDefault.isRefreshed());
        Assert.assertEquals(-1, statisticsOfProjectDefault.getMaxRecShowSize());

        ProjectStatisticsResponse statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        Assert.assertEquals(2, statsOfPrjStreamingTest.getDatabaseSize());
        Assert.assertEquals(11, statsOfPrjStreamingTest.getTableSize());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        statsOfPrjStreamingTest = projectSmartService.getProjectStatistics("streaming_test");
        Assert.assertEquals(1, statsOfPrjStreamingTest.getDatabaseSize());
        Assert.assertEquals(6, statsOfPrjStreamingTest.getTableSize());
        recommendationTopNUpdateScheduler.close();
    }

    @Test
    public void testUpdateJdbcConfig() throws Exception {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT_JDBC);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());

        JdbcRequest jdbcRequest = new JdbcRequest();
        jdbcRequest.setAdaptor("org.apache.kylin.sdk.datasource.adaptor.H2Adaptor");
        jdbcRequest.setDialect("h2");
        jdbcRequest.setDriver("org.h2.Driver");
        jdbcRequest.setPushdownClass("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl");
        jdbcRequest.setSourceConnector("io.kyligence.kap.source.jdbc.DefaultSourceConnector");
        jdbcRequest.setUrl("jdbc:h2:mem:db");
        jdbcRequest.setPass("kylin");
        projectService.updateJdbcConfig(PROJECT_JDBC, jdbcRequest);

        ProjectInstance project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals(ISourceAware.ID_JDBC, project.getSourceType());
        Assert.assertEquals("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl",
                project.getOverrideKylinProps().get("kylin.query.pushdown.runner-class-name"));
        Assert.assertEquals("org.apache.kylin.sdk.datasource.PushDownRunnerSDKImpl",
                project.getOverrideKylinProps().get("kylin.query.pushdown.partition-check.runner-class-name"));
        Assert.assertEquals("io.kyligence.kap.source.jdbc.DefaultSourceConnector",
                project.getOverrideKylinProps().get("kylin.source.jdbc.connector-class-name"));
        Assert.assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')",
                project.getOverrideKylinProps().get("kylin.source.jdbc.pass"));

        Mockito.doReturn(Mockito.mock(UserDetails.class)).when(userService).loadUserByUsername(Mockito.anyString());
        Mockito.doReturn(true).when(userService).isGlobalAdmin(Mockito.any(UserDetails.class));
        List<UserProjectPermissionResponse> projectInstances = projectService
                .getProjectsFilterByExactMatchAndPermissionWrapperUserPermission(PROJECT_JDBC, true,
                        AclPermissionEnum.READ);
        Assert.assertEquals(1, projectInstances.size());
        Assert.assertEquals(HIDDEN_VALUE,
                projectInstances.get(0).getProject().getOverrideKylinProps().get("kylin.source.jdbc.pass"));
    }

    @Test
    public void testGetStreamingProjectStatistics() {
        ProjectStatisticsResponse projectStatistics = projectSmartService.getProjectStatistics("streaming_test");
        Assert.assertEquals(2, projectStatistics.getDatabaseSize());
        Assert.assertEquals(11, projectStatistics.getTableSize());
        Assert.assertEquals(0, projectStatistics.getLastWeekQueryCount());
        Assert.assertEquals(0, projectStatistics.getUnhandledQueryCount());
        Assert.assertEquals(11, projectStatistics.getModelSize());
    }

    @Test
    public void testUpdateJdbcInfo() {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT_JDBC);
        UnitOfWork.doInTransactionWithRetry(() -> {
            projectService.createProject(projectInstance.getName(), projectInstance);
            return null;
        }, projectInstance.getName());

        JdbcSourceInfoRequest jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(true);
        jdbcSourceInfoRequest.setJdbcSourceName("h2");
        jdbcSourceInfoRequest.setJdbcSourceDriver("org.h2.Driver");
        jdbcSourceInfoRequest.setJdbcSourceConnectionUrl("jdbc:h2:mem:db");
        jdbcSourceInfoRequest.setJdbcSourceUser("kylin");
        jdbcSourceInfoRequest.setJdbcSourcePass("kylin");
        projectService.updateJdbcInfo(PROJECT_JDBC, jdbcSourceInfoRequest);

        ProjectInstance project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals("ENC('YeqVr9MakSFbgxEec9sBwg==')",
                project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_PASS_KEY));
        jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(true);
        jdbcSourceInfoRequest.setJdbcSourceName("h2");
        jdbcSourceInfoRequest.setJdbcSourceDriver("com.mysql.jdbc.driver");
        thrown.expect(KylinException.class);
        projectService.updateJdbcInfo(PROJECT_JDBC, jdbcSourceInfoRequest);

        project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals("org.h2.Driver", project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_DRIVER_KEY));
        jdbcSourceInfoRequest = new JdbcSourceInfoRequest();
        jdbcSourceInfoRequest.setJdbcSourceEnable(false);
        jdbcSourceInfoRequest.setJdbcSourcePass("test");
        projectService.updateJdbcInfo(PROJECT_JDBC, jdbcSourceInfoRequest);

        project = NProjectManager.getInstance(getTestConfig()).getProject(PROJECT_JDBC);
        Assert.assertEquals("test", project.getOverrideKylinProps().get(KYLIN_SOURCE_JDBC_PASS_KEY));

    }

    @Test(expected = KylinException.class)
    public void testDropProjectFailed() throws IOException {
        val project = "default";
        MockSecondStorage.mock(project, new ArrayList<>(), this);
        projectService.dropProject(project);
    }

}
