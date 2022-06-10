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

package io.kyligence.kap.job;

import java.util.List;
import java.util.Objects;

import javax.annotation.Resource;

import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.ExecutableState;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.config.FileJobConfig;
import io.kyligence.kap.job.core.lock.JdbcLockClient;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.mapper.JobLockMapper;
import io.kyligence.kap.job.rest.JobMapperFilter;
import io.kyligence.kap.job.scheduler.JdbcJobScheduler;
import io.kyligence.kap.job.scheduler.ParallelLimiter;
import io.kyligence.kap.job.scheduler.ResourceAcquirer;
import io.kyligence.kap.job.scheduler.SharedFileProgressReporter;

@Component
public class JobContext implements InitializingBean, DisposableBean {

    // resource block
    // progress report
    // status control

    private String serverNode;

    private AbstractJobConfig jobConfig;

    @Resource
    private JobInfoMapper jobInfoMapper;

    @Resource
    private JobLockMapper jobLockMapper;

    @Autowired
    private DataSourceTransactionManager transactionManager;

    private ParallelLimiter parallelLimiter;
    private ResourceAcquirer resourceAcquirer;

    private SharedFileProgressReporter progressReporter;

    private JdbcLockClient lockClient;

    private JdbcJobScheduler jobScheduler;

    @Override
    public void destroy() throws Exception {

        if (Objects.nonNull(jobConfig)) {
            jobConfig.destroy();
        }

        if (Objects.nonNull(resourceAcquirer)) {
            resourceAcquirer.destroy();
        }

        if (Objects.nonNull(progressReporter)) {
            progressReporter.destroy();
        }

        if (Objects.nonNull(parallelLimiter)) {
            parallelLimiter.destroy();
        }

        if (Objects.nonNull(lockClient)) {
            lockClient.destroy();
        }

        if (Objects.nonNull(jobScheduler)) {
            jobScheduler.destroy();
        }

    }

    @Override
    public void afterPropertiesSet() throws Exception {

        serverNode = AddressUtil.getLocalInstance();

        jobConfig = new FileJobConfig();

        resourceAcquirer = new ResourceAcquirer(jobConfig);
        resourceAcquirer.start();

        progressReporter = new SharedFileProgressReporter(jobConfig);
        progressReporter.start();

        parallelLimiter = new ParallelLimiter(this);
        parallelLimiter.start();

        lockClient = new JdbcLockClient(this);
        lockClient.start();

        jobScheduler = new JdbcJobScheduler(this);
        jobScheduler.start();

    }

    public String getServerNode() {
        return serverNode;
    }

    public AbstractJobConfig getJobConfig() {
        return jobConfig;
    }

    public DataSourceTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public JobInfoMapper getJobInfoMapper() {
        return jobInfoMapper;
    }

    public JobLockMapper getJobLockMapper() {
        return jobLockMapper;
    }

    public ParallelLimiter getParallelLimiter() {
        return parallelLimiter;
    }

    public ResourceAcquirer getResourceAcquirer() {
        return resourceAcquirer;
    }

    public SharedFileProgressReporter getProgressReporter() {
        return progressReporter;
    }

    public JdbcLockClient getLockClient() {
        return lockClient;
    }

    public JdbcJobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public List<JobInfo> fetchAllRunningJobs(String project, List<String> jobNames, List<String> subjects) {
        JobMapperFilter mapperFilter = JobMapperFilter.builder()
                .jobNames(jobNames)
                .statuses(Lists.newArrayList(ExecutableState.READY.name(),
                        JobStatusEnum.PENDING.name(),
                        JobStatusEnum.RUNNING.name()))
                .subjects(subjects)
                .project(project)
                .offset(0).limit(10000)
                .build();
        return jobInfoMapper.selectByJobFilter(mapperFilter);
    }
}
