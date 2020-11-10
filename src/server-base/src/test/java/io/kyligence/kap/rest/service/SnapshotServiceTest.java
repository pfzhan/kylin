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

import com.google.common.collect.Sets;
import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkSnapshotJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.SnapshotConfigRequest;
import io.kyligence.kap.rest.response.SnapshotCheckResponse;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class SnapshotServiceTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private SnapshotService snapshotService = Mockito.spy(new SnapshotService());

    @InjectMocks
    private ProjectService projectService = Mockito.spy(new ProjectService());
    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(IUserGroupService.class);

    @Before
    public void setup() {
        System.setProperty("HADOOP_USER_NAME", "root");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        createTestMetadata();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject("default");
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "11");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps,
                MaintainModelType.AUTO_MAINTAIN);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(snapshotService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(snapshotService, "userGroupService", userGroupService);
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);

    }

    @Test
    public void testBuildSnapshotWithoutSnapshotManualEnable() {
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        thrown.expect(KylinException.class);
        thrown.expectMessage("Snapshot management is not enable");
        snapshotService.buildSnapshots(PROJECT, tables, false);
    }

    @Test
    public void testBuildSnapshot() {
        enableSnapshotManualManagement();
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        snapshotService.buildSnapshots(PROJECT, tables, false);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);

        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(2, allExecutables.size());

        final AbstractExecutable job1 = allExecutables.get(0);
        Assert.assertTrue(job1 instanceof NSparkSnapshotJob);
        NSparkSnapshotJob samplingJob1 = (NSparkSnapshotJob) job1;
        Assert.assertEquals("SNAPSHOT_BUILD", samplingJob1.getName());
        Assert.assertEquals(PROJECT, samplingJob1.getProject());
        final String tableNameOfSamplingJob1 = samplingJob1.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob1));
        Assert.assertEquals(PROJECT, samplingJob1.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", samplingJob1.getSubmitter());

        final AbstractExecutable job2 = allExecutables.get(1);
        Assert.assertTrue(job2 instanceof NSparkSnapshotJob);
        NSparkSnapshotJob samplingJob2 = (NSparkSnapshotJob) job2;
        Assert.assertEquals("SNAPSHOT_BUILD", samplingJob2.getName());
        final String tableNameOfSamplingJob2 = samplingJob2.getParam(NBatchConstants.P_TABLE_NAME);
        Assert.assertEquals(PROJECT, samplingJob2.getProject());
        Assert.assertTrue(tables.contains(tableNameOfSamplingJob2));
        Assert.assertEquals(PROJECT, samplingJob2.getParam(NBatchConstants.P_PROJECT_NAME));
        Assert.assertEquals("ADMIN", samplingJob2.getSubmitter());

        Assert.assertEquals(tables, Sets.newHashSet(tableNameOfSamplingJob1, tableNameOfSamplingJob2));
    }

    @Test
    public void testSamplingKillAnExistingNonFinalJob() {
        enableSnapshotManualManagement();
        // initialize a sampling job and assert the status of it
        String table = "DEFAULT.TEST_KYLIN_FACT";
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(table), false);
        NExecutableManager executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        Assert.assertEquals(1, allExecutables.size());

        val initialJob = allExecutables.get(0);
        Assert.assertEquals(ExecutableState.READY, initialJob.getStatus());
        try {
            snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(table), false);
        } catch (TransactionException e) {
            Assert.assertTrue(e.getCause() instanceof JobSubmissionException);
            Assert.assertEquals("The job failed to be submitted. There already exists building job running " +
                    "under the corresponding subject.", (e.getCause()).getMessage());
        }
    }

    @Test
    public void testDeleteSnapshot() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.deleteSnapshots(PROJECT, Sets.newHashSet(tableName));
        Assert.assertNull(getSnapshotPath(tableName));
    }

    @Test
    public void testDeleteSnapshotWithRunningSnapshot() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(tableName), false);
        SnapshotCheckResponse response = snapshotService.deleteSnapshots(PROJECT, Sets.newHashSet(tableName));
        Assert.assertEquals(1, response.getAffectedJobs().size());
        Assert.assertNull(getSnapshotPath(tableName));
    }

    @Test
    public void testCheckBeforeDeleteSnapshotWithRunningSnapshot() {
        enableSnapshotManualManagement();
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        tableManager.getTableDesc(tableName).setLastSnapshotPath("file://a/b");
        Assert.assertNotNull(getSnapshotPath(tableName));
        snapshotService.buildSnapshots(PROJECT, Sets.newHashSet(tableName), false);
        SnapshotCheckResponse response = snapshotService.checkBeforeDeleteSnapshots(PROJECT, Sets.newHashSet(tableName));
        Assert.assertEquals(1, response.getAffectedJobs().size());
        Assert.assertNotNull(getSnapshotPath(tableName));
    }

    private String getSnapshotPath(String tableName) {
        return NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName)
                .getLastSnapshotPath();
    }

    private void enableSnapshotManualManagement() {
        SnapshotConfigRequest request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(PROJECT, request);
    }
}
