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
import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.rest.execution.SucceedTestExecutable;
import io.kyligence.kap.rest.request.JobFilter;
import io.kyligence.kap.rest.response.ExecutableResponse;
import io.kyligence.kap.rest.response.ExecutableStepResponse;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.rest.constant.Constant;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.springframework.test.util.ReflectionTestUtils;

public class JobServiceTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private JobService jobService = Mockito.spy(new JobService());

    @Mock
    private TableExtService tableExtService = Mockito.spy(TableExtService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setupResource() throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        staticCreateTestMetadata();

    }

    @Before
    public void setup() throws IOException {
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(jobService, "tableExtService", tableExtService);

    }

    @AfterClass
    public static void tearDown() {
        cleanAfterClass();
    }

    @Test
    public void testListJobs() throws Exception {
        NExecutableManager executableManager = Mockito.mock(NExecutableManager.class);
        Mockito.when(jobService.getExecutableManager("default")).thenReturn(executableManager);
        Mockito.when(executableManager.getAllExecutables(Mockito.anyLong(), Mockito.anyLong())).thenReturn(mockJobs());
        Integer[] statusInt = {};
        String[] subjects = {};
        JobFilter jobFilter = new JobFilter(statusInt, "", 4, subjects, "default", "", true);
        List<ExecutableResponse> jobs = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs.size() == 3);
        jobFilter.setTimeFilter(0);
        jobFilter.setJobName("ob1");
        List<ExecutableResponse> jobs2 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs2.size() == 1);
        String[] subjects2 = {"model1"};
        jobFilter.setSubjects(subjects2);
        jobFilter.setJobName("");
        jobFilter.setTimeFilter(2);
        List<ExecutableResponse> jobs3 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs3.size() == 1);
        Integer[] statusInt2 = {0};
        jobFilter.setSubjects(subjects);
        jobFilter.setStatus(statusInt2);
        jobFilter.setTimeFilter(1);
        List<ExecutableResponse> jobs4 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs4.size() == 3);
        jobFilter.setStatus(statusInt);
        jobFilter.setTimeFilter(3);
        jobFilter.setSortBy("job_name");
        List<ExecutableResponse> jobs5 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs5.size() == 3 && jobs5.get(0).getJobName().equals("sparkjob3"));
        jobFilter.setTimeFilter(4);
        jobFilter.setReverse(false);
        List<ExecutableResponse> jobs6 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs6.size() == 3 && jobs6.get(0).getJobName().equals("sparkjob1"));
        jobFilter.setSortBy("duration");
        jobFilter.setReverse(true);
        List<ExecutableResponse> jobs7 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs7.size() == 3 && jobs7.get(0).getJobName().equals("sparkjob2"));
        jobFilter.setSortBy("exec_start_time");
        jobFilter.setReverse(false);
        List<ExecutableResponse> jobs8 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs8.size() == 3 && jobs8.get(0).getJobName().equals("sparkjob2"));
        jobFilter.setSortBy("target_subject");
        List<ExecutableResponse> jobs9 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs9.size() == 3 && jobs9.get(0).getJobName().equals("sparkjob1"));
        Integer[] statusInt3 = {0, 1, 2, 4, 8, 16, 32};
        jobFilter.setStatus(statusInt3);
        jobFilter.setSortBy("");
        List<ExecutableResponse> jobs10 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs9.size() == 3);
        jobFilter.setSortBy("job_status");
        List<ExecutableResponse> jobs11 = jobService.listJobs(jobFilter);
        Assert.assertTrue(jobs11.size() == 3 && jobs11.get(0).getJobName().equals("sparkjob1"));
    }


    @Test
    public void testBasic() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        NDataflowManager dsMgr = NDataflowManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        manager.addJob(executable);
        jobService.updateJobStatus(executable.getId(), "default", "PAUSE");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.STOPPED));
        jobService.updateJobStatus(executable.getId(), "default", "RESUME");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.READY));
        jobService.updateJobStatus(executable.getId(), "default", "DISCARD");
        Assert.assertTrue(manager.getJob(executable.getId()).getStatus().equals(ExecutableState.DISCARDED));
        Assert.assertTrue(dsMgr.getDataflow("ncube_basic").getSegment(0) == null);
        Mockito.doNothing().when(tableExtService).removeJobIdFromTableExt(executable.getId(), "default");
        jobService.dropJob("default", executable.getId());
        List<AbstractExecutable> executables = manager.getAllExecutables();
        Assert.assertTrue(!executables.contains(executable));
    }

    @Test
    public void testDiscardJobException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setProject("default");
        manager.addJob(executable);
        manager.updateJobOutput(executable.getId(), ExecutableState.RUNNING, null, null);
        manager.updateJobOutput(executable.getId(), ExecutableState.SUCCEED, null, null);
        Assert.assertEquals(ExecutableState.SUCCEED, executable.getStatus());
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("The job " + executable.getId() + " has already been succeed and cannot be discarded.");
        jobService.updateJobStatus(executable.getId(), "default", "DISCARD");
    }


    @Test
    public void testUpdateException() throws IOException {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        manager.addJob(executable);
        thrown.expect(IllegalStateException.class);
        jobService.updateJobStatus(executable.getId(), "default", "ROLLBACK");
    }

    @Test
    public void testGetJobDetail() {
        NExecutableManager manager = NExecutableManager.getInstance(jobService.getConfig(), "default");
        SucceedTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        executable.setProject("default");
        executable.setName("test");
        executable.addTask(new SucceedTestExecutable());
        manager.addJob(executable);
        List<ExecutableStepResponse> result = jobService.getJobDetail("default", executable.getId());
        Assert.assertTrue(result.size() == 1);
    }


    private List<AbstractExecutable> mockJobs() {
        List<AbstractExecutable> jobs = new ArrayList<>();
        SucceedTestExecutable job1 = new SucceedTestExecutable();
        job1.setProject("default");
        job1.initConfig(KylinConfig.getInstanceFromEnv());
        job1.setName("sparkjob1");
        job1.setTargetSubject("model1");
        job1.setStartTime(1506585216000L);
        job1.setEndTime(1506758016000L);
        SucceedTestExecutable job2 = new SucceedTestExecutable();
        job2.setProject("default");
        job2.initConfig(KylinConfig.getInstanceFromEnv());
        job2.setName("sparkjob2");
        job2.setTargetSubject("model2");
        job2.setStartTime(1506585215000L);
        job2.setEndTime(1506758017000L);
        SucceedTestExecutable job3 = new SucceedTestExecutable();
        job3.setProject("default");
        job3.initConfig(KylinConfig.getInstanceFromEnv());
        job3.setName("sparkjob3");
        job3.setTargetSubject("model3");
        job3.setStartTime(1506585217000L);
        job3.setEndTime(1506758018000L);
        jobs.add(job1);
        jobs.add(job2);
        jobs.add(job3);
        return jobs;
    }

}
