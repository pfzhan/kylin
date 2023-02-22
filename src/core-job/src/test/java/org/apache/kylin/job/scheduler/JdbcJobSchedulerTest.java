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
package org.apache.kylin.job.scheduler;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.SucceedChainedTestExecutable;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo(onlyProps = true)
public class JdbcJobSchedulerTest {
    private static final String PROJECT = "default";
    
    private JobInfoDao jobInfoDao;
    private JobContext jobContext;
    
    @BeforeEach
    public void setup() {
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.job.slave-pull-batch-size", "1");
        config.setProperty("kylin.job.slave-lock-renew-sec", "3");
        jobContext = JobContextUtil.getJobContextForTest(config);
        jobInfoDao = JobContextUtil.getJobInfoDao(config);
    }

    @AfterEach
    public void clean() {
        JobContextUtil.cleanUp();
    }
    
    @Test
    void happyPath() {
        String jobId = mockJob();
        Assert.assertEquals(jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus(),
                ExecutableState.READY.name());
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.PENDING.name()));
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.RUNNING.name()));
        await().atMost(2, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.SUCCEED.name()));
        //release lock
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobContext.getJobLockMapper().selectByJobId(jobId) == null);

    }
    
    @Test
    void oneJobCanNotRunOnTwoNodesTest() throws Exception {
        JobContext secondJobContext = mockJobContext("127.0.0.1:7071");
        String jobId = mockJob();
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus()
                .equals(ExecutableState.RUNNING.name()));
        Assert.assertEquals(secondJobContext.getJobScheduler().getRunningJob().size()
                + jobContext.getJobScheduler().getRunningJob().size(), 1);

        secondJobContext.destroy();
    }

    @Test
    void JobsScheduledOnTwoNode() throws Exception {
        JobContext secondJobContext = mockJobContext("127.0.0.1:7071");
        System.setProperty("COST_TIME", "3000");
        for (int i = 0; i < 3; i++) {
            mockJob();
        }
        JobMapperFilter filter = new JobMapperFilter();
        filter.setStatuses(Collections.singletonList(ExecutableState.RUNNING.name()));
        await().atMost(5, TimeUnit.SECONDS).until(() -> jobInfoDao.getJobInfoListByFilter(filter).size() == 3);
        Assert.assertEquals(secondJobContext.getJobScheduler().getRunningJob().size()
                + jobContext.getJobScheduler().getRunningJob().size(), 3);
        Assert.assertTrue(jobContext.getJobScheduler().getRunningJob().size() > 0);
        Assert.assertTrue(secondJobContext.getJobScheduler().getRunningJob().size() > 0);

        secondJobContext.destroy();
        System.clearProperty("COST_TIME");
    }

    @Test
    void testLockExpiredAndJobNotFinal() {
        String jobId = mockJob();
        JobLock lock = new JobLock(jobId);
        lock.setLockNode("mock_node");
        lock.setLockExpireTime(new Date());
        int expect = jobContext.getJobLockMapper().insert(lock);
        Assert.assertEquals(1, expect);
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> jobInfoDao.getExecutablePOByUuid(jobId).getOutput().getStatus().equals(ExecutableState.SUCCEED.name()));

    }

    @Test
    void testJobProducedAndDeleted() {
        // mock job, not persist in metadata
        AbstractExecutable job = mockExecutable();
        // insert job lock, without lock node
        String jobId = job.getJobId();
        JobLock lock = new JobLock(jobId);
        int expect = jobContext.getJobLockMapper().insert(lock);
        Assert.assertEquals(1, expect);
        await().atMost(60, TimeUnit.SECONDS).until(() -> jobContext.getJobLockMapper().selectByJobId(jobId) == null);
    }

    private String mockJob() {
        ExecutableManager manager = ExecutableManager.getInstance(getTestConfig(), PROJECT);
        AbstractExecutable job = mockExecutable();
        manager.addJob(job);
        return job.getJobId();
    }

    private AbstractExecutable mockExecutable() {
        SucceedChainedTestExecutable job = new SucceedChainedTestExecutable();
        job.setProject(PROJECT);
        job.setName("mocked job");
        job.setTargetSubject("12345678");
        job.setJobType(JobTypeEnum.INC_BUILD);
        return job;
    }
    
    private JobContext mockJobContext(String serverNode) {
        JobContext secondJobContext = new JobContext();
        secondJobContext.setKylinConfig(getTestConfig());
        secondJobContext.setJobInfoMapper(jobContext.getJobInfoMapper());
        secondJobContext.setJobLockMapper(jobContext.getJobLockMapper());
        secondJobContext.setTransactionManager(jobContext.getTransactionManager());
        secondJobContext.init();
        ReflectionTestUtils.setField(secondJobContext, "serverNode", serverNode);
        return secondJobContext;
    }
}
