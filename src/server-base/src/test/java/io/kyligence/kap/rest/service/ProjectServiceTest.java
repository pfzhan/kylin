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
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.security.AclManager;
import org.apache.kylin.rest.security.AclRecord;
import org.apache.kylin.rest.security.ObjectIdentityImpl;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.apache.kylin.rest.util.AclEvaluate;
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

import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.metadata.cube.model.FrequencyMap;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.config.initialize.BootstrapCommand;
import io.kyligence.kap.rest.request.GarbageCleanUpConfigRequest;
import io.kyligence.kap.rest.request.JobNotificationConfigRequest;
import io.kyligence.kap.rest.request.ProjectGeneralInfoRequest;
import io.kyligence.kap.rest.request.PushDownConfigRequest;
import io.kyligence.kap.rest.request.SegmentConfigRequest;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import io.kyligence.kap.source.file.S3KeyCredential;
import io.kyligence.kap.source.file.S3KeyCredentialOperator;
import lombok.val;
import lombok.var;

public class ProjectServiceTest extends ServiceTestBase {
    private static final String PROJECT = "default";
    private static final String PROJECT_ID = "a8f4da94-a8a4-464b-ab6f-b3012aba04d5";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AsyncTaskService asyncTaskService = Mockito.spy(AsyncTaskService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "asyncTaskService", asyncTaskService);
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
        projectService.createProject(projectInstance.getName(), projectInstance);
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertTrue(projectInstance2 != null);
        Assert.assertEquals(projectInstance2.getMaintainModelType(), MaintainModelType.AUTO_MAINTAIN);
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProject_ManualMaintain_Pass() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project11");
        projectInstance.setMaintainModelType(MaintainModelType.MANUAL_MAINTAIN);
        projectService.createProject(projectInstance.getName(), projectInstance);
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertTrue(projectInstance2 != null);
        Assert.assertEquals(projectInstance2.getMaintainModelType(), MaintainModelType.MANUAL_MAINTAIN);
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectException() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(PROJECT);
        thrown.expect(BadRequestException.class);
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
        Assert.assertEquals(10, projectInstances.size());

    }

    @Test
    public void testGetReadableProjects_NoPermission() throws Exception {
        Mockito.doReturn(false).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(0, projectInstances.size());

    }

    @Test
    public void testGetReadableProjects_hasNoPermissionProject() throws Exception {
        Mockito.doReturn(true).when(aclEvaluate).hasProjectAdminPermission(Mockito.any(ProjectInstance.class));
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("", false);
        Assert.assertEquals(10, projectInstances.size());

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
        projectService.updateStorageQuotaConfig(PROJECT, 2147483648L);
        Assert.assertEquals(2147483648L,
                NProjectManager.getInstance(getTestConfig()).getProject(PROJECT).getConfig().getStorageQuotaSize());
    }

    @Test
    public void testGetStorageVolumeInfoResponse() {
        prepareLayoutHitCount();
        StorageVolumeInfoResponse storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse(PROJECT);

        Assert.assertEquals(10240L * 1024 * 1024 * 1024, storageVolumeInfoResponse.getStorageQuotaSize());

        // layouts except 1 and 1000001 were all considered as garbage
        Assert.assertEquals(4178944L, storageVolumeInfoResponse.getGarbageStorageSize());
    }

    @Test
    public void testCleanupProjectGarbage() throws IOException {
        prepareLayoutHitCount();
        Mockito.doNothing().when(asyncTaskService).cleanupStorage();
        projectService.cleanupGarbage(PROJECT);
        val indexPlan = NIndexPlanManager.getInstance(getTestConfig(), PROJECT).getIndexPlan(MODEL_ID);
        Assert.assertEquals(2, indexPlan.getAllLayouts().size());
        val allLayoutsIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        Assert.assertTrue(allLayoutsIds.contains(1L));
        Assert.assertTrue(allLayoutsIds.contains(1000001L));
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
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), "broken_test");
        Assert.assertEquals(3, dataflowManager.listUnderliningDataModels(true).size());

        // there are no any favorite queries
        projectService.garbageCleanup();
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), projectInstance.getName());
            if (projectInstance.getName().equals("broken_test")) {
                Assert.assertEquals(0, dataflowManager.listUnderliningDataModels(true).size());
            }

            for (NDataModel model : dataflowManager.listUnderliningDataModels()) {
                val indexPlan = dataflowManager.getDataflow(model.getId()).getIndexPlan();
                if (model.getId().equals(MODEL_ID)) {
                    val layouts = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId)
                            .collect(Collectors.toSet());
                    Assert.assertEquals(2, layouts.size());
                    Assert.assertTrue(layouts.contains(1L));
                    Assert.assertTrue(layouts.contains(1000001L));
                    continue;
                }

                // all auto index layouts are deleted
                Assert.assertEquals(0, indexPlan.getWhitelistLayouts().size());
                for (LayoutEntity layoutEntity : indexPlan.getAllLayouts()) {
                    Assert.assertFalse(layoutEntity.isAuto());
                }
            }
        }

        acl = AclManager.getInstance(getTestConfig()).get(PROJECT_ID);
        Assert.assertNull(acl);
    }

    private void prepareLayoutHitCount() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);

        long currentTime = System.currentTimeMillis();
        long currentDate = currentTime - currentTime % (24 * 60 * 60 * 1000L);
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
    }

    @Test
    public void testUpdateProjectConfig() {
        val project = PROJECT;

        val description = "test description";
        val request = new ProjectGeneralInfoRequest();
        request.setProject(project);
        request.setDescription(description);
        projectService.updateProjectGeneralInfo(project, request);
        var response = projectService.getProjectConfig(project);
        Assert.assertEquals(description, response.getDescription());

        val segmentConfigRequest = new SegmentConfigRequest();
        segmentConfigRequest.setAutoMergeEnabled(false);
        segmentConfigRequest.setProject(project);
        projectService.updateSegmentConfig(project, segmentConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(false, response.isAutoMergeEnabled());

        val jobNotificationConfigRequest = new JobNotificationConfigRequest();
        jobNotificationConfigRequest.setProject(project);
        jobNotificationConfigRequest.setDataLoadEmptyNotificationEnabled(false);
        jobNotificationConfigRequest.setJobErrorNotificationEnabled(false);
        jobNotificationConfigRequest.setJobNotificationEmails(
                Lists.newArrayList("user1@kyligence.io", "user2@kyligence.io", "user2@kyligence.io"));
        projectService.updateJobNotificationConfig(project, jobNotificationConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(2, response.getJobNotificationEmails().size());
        Assert.assertEquals(false, response.isJobErrorNotificationEnabled());
        Assert.assertEquals(false, response.isDataLoadEmptyNotificationEnabled());

        val pushDownConfigRequest = new PushDownConfigRequest();
        pushDownConfigRequest.setProject(project);
        pushDownConfigRequest.setPushDownEnabled(false);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(false, response.isPushDownEnabled());

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name",
                "io.kyligence.kap.smart.query.mockup.MockupPushDownRunner");
        pushDownConfigRequest.setProject(project);
        pushDownConfigRequest.setPushDownEnabled(true);
        projectService.updatePushDownConfig(project, pushDownConfigRequest);
        response = projectService.getProjectConfig(project);
        Assert.assertEquals(true, response.isPushDownEnabled());

        getTestConfig().setProperty("kylin.query.pushdown.runner-class-name", "");
        pushDownConfigRequest.setProject(project);
        pushDownConfigRequest.setPushDownEnabled(true);
        thrown.expectMessage(
                "There is no default PushDownRunner, please check kylin.query.pushdown.runner-class-name in kylin.properties.");
        projectService.updatePushDownConfig(project, pushDownConfigRequest);

    }

    @Test
    public void testDropProject() {
        val project = "project12";
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(project);
        projectService.createProject(project, projectInstance);

        val eventOrchestratorManager = EventOrchestratorManager.getInstance(getTestConfig());
        eventOrchestratorManager.addProject(project);
        Assert.assertNotNull(eventOrchestratorManager.getEventOrchestratorByProject(project));
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(getTestConfig()), new MockJobLock());
        Assert.assertTrue(scheduler.hasStarted());

        NFavoriteScheduler favoriteScheduler = NFavoriteScheduler.getInstance(project);
        favoriteScheduler.init();
        Assert.assertTrue(favoriteScheduler.hasStarted());
        projectService.dropProject(project);
        new BootstrapCommand().run();
        val prjManager = NProjectManager.getInstance(getTestConfig());
        Assert.assertNull(prjManager.getProject(project));
        Assert.assertNull(eventOrchestratorManager.getEventOrchestratorByProject(project));
        Assert.assertNull(NDefaultScheduler.getInstanceByProject(project));
        Assert.assertNull(NFavoriteScheduler.getInstanceByProject(project));
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
        request.setLowFrequencyThreshold(12);
        projectService.updateGarbageCleanupConfig("default", request);
        val prjMgr = NProjectManager.getInstance(getTestConfig());
        val prj = prjMgr.getProject("default");
        Assert.assertEquals(604800000L, prj.getConfig().getFavoriteQueryFrequencyTimeWindow());
        Assert.assertEquals(12, prj.getConfig().getFavoriteQueryLowFrequency());
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

}
