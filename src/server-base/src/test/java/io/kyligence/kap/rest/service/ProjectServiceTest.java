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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.CuboidLayoutQueryTimes;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.rest.response.StorageVolumeInfoResponse;
import lombok.val;

public class ProjectServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() throws IOException {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @AfterClass
    public static void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testCreateProject_AutoMaintain_Pass() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("project11");
        projectService.createProject(projectInstance);
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
        projectService.createProject(projectInstance);
        ProjectInstance projectInstance2 = projectManager.getProject("project11");
        Assert.assertTrue(projectInstance2 != null);
        Assert.assertEquals(projectInstance2.getMaintainModelType(), MaintainModelType.MANUAL_MAINTAIN);
        projectManager.dropProject("project11");
    }

    @Test
    public void testCreateProjectException() throws Exception {

        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName("default");
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("The project named 'default' already exists.");
        projectService.createProject(projectInstance);

    }

    @Test
    public void testGetReadableProjectsByName() throws Exception {

        List<ProjectInstance> projectInstances = projectService.getReadableProjects("default");
        Assert.assertTrue(projectInstances.size() == 1 && projectInstances.get(0).getName().equals("default"));

    }

    @Test
    public void testGetReadableProjects() throws Exception {
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("");
        Assert.assertEquals(9, projectInstances.size());

    }

    @Test
    public void testUpdateThreshold() throws Exception {
        projectService.updateQueryAccelerateThresholdConfig("default", 20, false, true);
        List<ProjectInstance> projectInstances = projectService.getReadableProjects("default");
    }

    @Test
    public void testGetThreshold() throws Exception {
        val response = projectService.getQueryAccelerateThresholdConfig("default");
        Assert.assertEquals(20, response.getThreshold());
        Assert.assertTrue(response.isBatchEnabled());
        Assert.assertFalse(response.isAutoApply());
    }

    @Test
    public void testUpdateProjectMaintainType() throws Exception {
        projectService.updateMantainModelType("default", MaintainModelType.MANUAL_MAINTAIN.name());
        Assert.assertEquals(MaintainModelType.MANUAL_MAINTAIN,
                NProjectManager.getInstance(getTestConfig()).getProject("default").getMaintainModelType());
    }

    @Test
    public void testUpdateStorageQuotaConfig() throws Exception {
        projectService.updateStorageQuotaConfig("default", 2147483648L);
        Assert.assertEquals(2147483648L,
                NProjectManager.getInstance(getTestConfig()).getProject("default").getConfig().getStorageQuotaSize());
    }

    @Test
    public void testGetStorageVolumeInfoResponse() throws Exception {
        mockHotModelLayouts();
        StorageVolumeInfoResponse storageVolumeInfoResponse = projectService.getStorageVolumeInfoResponse("default");

        Assert.assertEquals(1024 * 1024 * 1024L, storageVolumeInfoResponse.getStorageQuotaSize());
        Assert.assertEquals(5633024L, storageVolumeInfoResponse.getGarbageStorageSize());
    }

    @Test
    public void testCleanupProjectGarbageIndex() throws Exception {
        val project = "default";
        mockHotModelLayouts();
        projectService.cleanupProjectGarbageIndex(project);
        val cubePlan = NCubePlanManager.getInstance(getTestConfig(), project).getCubePlan("ncube_basic");
        Assert.assertEquals(1L, cubePlan.getAllCuboidLayouts().size());
    }

    private void mockHotModelLayouts() throws NoSuchFieldException, IllegalAccessException {

        List<CuboidLayoutQueryTimes> hotCuboidLayoutQueryTimesList = Lists.newArrayList();
        KylinConfig config = getTestConfig();
        QueryHistoryDAO.getInstance(config);

        Field field = config.getClass().getDeclaredField("managersCache");
        field.setAccessible(true);

        ConcurrentHashMap<Class, Object> cache = (ConcurrentHashMap<Class, Object>) field.get(config);
        QueryHistoryDAO dao = Mockito.spy(QueryHistoryDAO.getInstance(config));
        cache.put(QueryHistoryDAO.class, dao);
        Mockito.doReturn(hotCuboidLayoutQueryTimesList).when(dao).getCuboidLayoutQueryTimes("default", 5,
                CuboidLayoutQueryTimes.class);

    }

}
