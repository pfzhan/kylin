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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

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

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.TransactionException;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.NSparkSnapshotJob;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.model.MaintainModelType;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.request.SnapshotConfigRequest;
import io.kyligence.kap.rest.response.NInitTablesResponse;
import io.kyligence.kap.rest.response.SnapshotCheckResponse;
import io.kyligence.kap.rest.response.SnapshotResponse;
import io.kyligence.kap.rest.response.TableNameResponse;
import lombok.val;

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

    @Mock
    protected TableService tableService = Mockito.spy(TableService.class);

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
        ReflectionTestUtils.setField(snapshotService, "tableService", tableService);
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
    public void testBuildSnapshotOfNoPermissionTables() {
        enableSnapshotManualManagement();
        Set<String> tables = Sets.newHashSet("non-exist");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Cannot find table 'non-exist'.");
        snapshotService.buildSnapshots(PROJECT, tables, false);
    }

    @Test
    public void testRefreshSnapshotFailWithNoSnapshot() {
        enableSnapshotManualManagement();
        Set<String> tables = Sets.newHashSet("DEFAULT.TEST_KYLIN_FACT");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Snapshot(s) 'DEFAULT.TEST_KYLIN_FACT' not found.");
        snapshotService.buildSnapshots(PROJECT, tables, true);
    }

    @Test
    public void testBuildSnapshot() {
        enableSnapshotManualManagement();
        final String table1 = "DEFAULT.TEST_KYLIN_FACT";
        final String table2 = "DEFAULT.TEST_ACCOUNT";
        Set<String> tables = Sets.newHashSet(table1, table2);
        Set<String> databases = Sets.newHashSet();
        snapshotService.buildSnapshots(PROJECT, databases, tables, false);
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

        // refresh failed
        String expected = "The job failed to be submitted. There already exists building job running "
                + "under the corresponding subject.";
        String actual = "";
        try {
            snapshotService.buildSnapshots(PROJECT, databases, tables, true);
        } catch (TransactionException e) {
            actual = e.getCause().getMessage();
        }
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBuildSnapshotOfDatabase() {
        // build snapshots of database "DEFAULT"
        enableSnapshotManualManagement();
        final String database = "DEFAULT";
        Set<String> databases = Sets.newHashSet(database);
        Set<String> tables = Sets.newHashSet();
        snapshotService.buildSnapshots(PROJECT, databases, tables, false);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), PROJECT);
        final List<AbstractExecutable> allExecutables = executableManager.getAllExecutables();
        val tableManager = NTableMetadataManager.getInstance(getTestConfig(), PROJECT);
        int expected_table_size = tableManager.listAllTables().stream()
                .filter(tableDesc -> tableDesc.getDatabase().equals(database)).toArray().length;
        Assert.assertEquals(expected_table_size, allExecutables.size());

        // build snapshots of non-exist database
        databases = Sets.newHashSet("non-exist");
        thrown.expect(KylinException.class);
        thrown.expectMessage("Database:[NON-EXIST] is not exist.");
        snapshotService.buildSnapshots(PROJECT, databases, tables, false);
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
            Assert.assertEquals("The job failed to be submitted. There already exists building job running "
                    + "under the corresponding subject.", (e.getCause()).getMessage());
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
        Assert.assertEquals(-1, getOriginalSize(tableName));
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
        SnapshotCheckResponse response = snapshotService.checkBeforeDeleteSnapshots(PROJECT,
                Sets.newHashSet(tableName));
        Assert.assertEquals(1, response.getAffectedJobs().size());
        Assert.assertNotNull(getSnapshotPath(tableName));
    }

    @Test
    public void testGetProjectSnapshots() {
        enableSnapshotManualManagement();
        String tablePattern = "SSB";
        Set<String> statusFilter = Sets.newHashSet("ONLINE");
        String sortBy = "";
        setSnapshotPath("SSB.LINEORDER", "some_path");
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
        snapshotService.getProjectSnapshots(PROJECT, tablePattern, statusFilter, sortBy, true);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        List<SnapshotResponse> responses = snapshotService.getProjectSnapshots(PROJECT, tablePattern, statusFilter,
                sortBy, true);
        SnapshotResponse response = responses.get(0);
        Assert.assertEquals("SSB", response.getDatabase());
        Assert.assertEquals("LINEORDER", response.getTable());
    }

    @Test
    public void testGetTables() {
        enableSnapshotManualManagement();
        String tablePattern = "SSB.CUSTOM";
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "true");
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("testuser", "testuser", Constant.ROLE_MODELER));
        snapshotService.getTables(PROJECT, tablePattern, 0, 10);

        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        NInitTablesResponse response = snapshotService.getTables(PROJECT, tablePattern, 0, 1);
        NInitTablesResponse.DatabaseTables database = response.getDatabases().get(0);
        Assert.assertEquals("SSB", database.getDbname());
        Assert.assertEquals("CUSTOMER", ((TableNameResponse) database.getTables().get(0)).getTableName());
        Assert.assertEquals(false, ((TableNameResponse) database.getTables().get(0)).isLoaded());
    }

    @Test
    public void TestGetTableNameResponses() {
        enableSnapshotManualManagement();
        String database = "SSB";
        String tablePattern = "ABC.CUSTOM";
        int size = snapshotService.getTableNameResponses(PROJECT, database, tablePattern).size();
        Assert.assertEquals(0, size);

        tablePattern = "SSB.CUSTOM";
        TableNameResponse response = snapshotService.getTableNameResponses(PROJECT, database, tablePattern).get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());

        tablePattern = "";
        List<TableNameResponse> responses = snapshotService.getTableNameResponses(PROJECT, database, tablePattern);
        response = responses.get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());
        Assert.assertEquals(6, responses.size());

        tablePattern = null;
        responses = snapshotService.getTableNameResponses(PROJECT, database, tablePattern);
        response = responses.get(0);
        Assert.assertEquals("CUSTOMER", response.getTableName());
        Assert.assertEquals(false, response.isLoaded());
        Assert.assertEquals(6, responses.size());
    }

    private String getSnapshotPath(String tableName) {
        return NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName)
                .getLastSnapshotPath();
    }

    private void setSnapshotPath(String tableName, String snapshotPath) {
        NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getTableDesc(tableName)
                .setLastSnapshotPath(snapshotPath);
    }

    private long getOriginalSize(String tableName) {
        return NTableMetadataManager.getInstance(getTestConfig(), PROJECT).getOrCreateTableExt(tableName)
                .getOriginalSize();
    }

    private void enableSnapshotManualManagement() {
        SnapshotConfigRequest request = new SnapshotConfigRequest();
        request.setSnapshotManualManagementEnabled(true);
        projectService.updateSnapshotConfig(PROJECT, request);
    }
}
