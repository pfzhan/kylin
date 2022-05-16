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

package io.kyligence.kap.job.scheduler;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.engine.spark.utils.ThreadUtils;
import io.kyligence.kap.job.DataLoadingManager;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.AbstractJobExecutable;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.domain.JobScheduleLock;
import io.kyligence.kap.job.mapper.JobInfoMapper;
import io.kyligence.kap.job.mapper.JobScheduleLockMapper;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JdbcJobScheduler.class);

    private static final String JOB_SCHEDULER_MASTER = "job_scheduler_master";

    private final AbstractJobConfig jobConfig;

    private final JobInfoMapper jobInfoMapper;

    private final JobScheduleLockMapper scheduleLockMapper;

    private final AtomicBoolean isMaster = new AtomicBoolean(false);

    private final String schedulerInstance = AddressUtil.getLocalInstance();

    private ScheduledExecutorService standbyScheduler;

    private ScheduledExecutorService masterScheduler;

    private ScheduledExecutorService slaveScheduler;

    private AtomicReference<ThreadPoolExecutor> jobExecutorPoolRef;

    private AtomicReference<JobContext> jobContextRef;

    public JdbcJobScheduler() {
        DataLoadingManager dataLoading = DataLoadingManager.getInstance();
        jobConfig = dataLoading.getJobConfig();
        jobInfoMapper = dataLoading.getJobInfoMapper();
        scheduleLockMapper = dataLoading.getScheduleLockMapper();
    }

    public void start() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {

        // acquire JSM
        standbyScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Standby-Singleton");

        // produce job:  READY -> PENDING
        masterScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Master-Singleton");

        // consume job: PENDING -> RUNNING
        slaveScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Slave-Singleton");

        // run job: RUNNING -> FINISHED
        jobExecutorPoolRef = new AtomicReference<>(ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Executor",
                1, jobConfig.getJobSchedulerConsumerMaxThreads(), 5, TimeUnit.MINUTES));

        // resource block
        // progress report
        // status control
        jobContextRef = new AtomicReference<>(new JobContext(jobConfig));

        // acquire JSM
        scheduleStandby();
        // produce job
        scheduleMaster();
        // consume job
        scheduleSlave();
    }

    @Override
    public void destroy() {
        if (Objects.nonNull(standbyScheduler)) {
            standbyScheduler.shutdownNow();
        }

        if (Objects.nonNull(masterScheduler)) {
            masterScheduler.shutdownNow();
        }

        if (Objects.nonNull(slaveScheduler)) {
            slaveScheduler.shutdownNow();
        }

        if (Objects.nonNull(jobExecutorPoolRef)) {
            jobExecutorPoolRef.get().shutdownNow();
        }

        if (Objects.nonNull(jobContextRef)) {
            jobContextRef.get().close();
        }
    }

    @Override
    public String getJobOwner(String jobId) {
        JobScheduleLock jobScheduleLock = scheduleLockMapper.selectByPrimaryKey(jobId);
        if (jobScheduleLock == null) {
            return AddressUtil.getLocalInstance();
        }
        return jobScheduleLock.getLockInstance();
    }

    private Date nextJSMExpireTime() {
        Date current = new Date();
        return DateUtils.addSeconds(current, jobConfig.getJobSchedulerMasterExpireSec());
    }

    private int getJSMRenewalDelaySec() {
        double renewalRatio = jobConfig.getJobSchedulerMasterRenewalRatio();
        return (int) (renewalRatio * jobConfig.getJobSchedulerMasterExpireSec());
    }

    private void scheduleStandby() {
        int renewDelay = getJSMRenewalDelaySec();
        int preemptDelay = jobConfig.getJobSchedulerMasterExpireSec() - renewDelay;
        Preconditions.checkArgument(preemptDelay < renewDelay, "JdbcJobScheduler illegal renewal delay.");
        // preempt
        standbyScheduler.schedule(this::preemptJSM, preemptDelay, TimeUnit.SECONDS);

        // renew
        standbyScheduler.schedule(this::renewJSM, renewDelay, TimeUnit.SECONDS);
    }

    private void preemptJSM() {
        int r = scheduleLockMapper.insertLock(JOB_SCHEDULER_MASTER, schedulerInstance, nextJobExpireTime());
        if (r > 0) {
            isMaster.set(true);
            logger.info("JdbcJobScheduler preempt master success.");
        }
    }

    private void renewJSM() {
        int r = scheduleLockMapper.updateLock(JOB_SCHEDULER_MASTER, schedulerInstance, nextJSMExpireTime());
        if (r > 0) {
            if (isMaster.get()) {
                logger.info("JdbcJobScheduler renew master success.");
            } else {
                isMaster.set(true);
                logger.info("JdbcJobScheduler acquire master success, scheduler become master and produce job.");
            }
        } else if (isMaster.get()) {
            isMaster.set(false);
            logger.info("JdbcJobScheduler demise master, scheduler fallback to standby and consume job.");
        }

        int delay = getJSMRenewalDelaySec();
        standbyScheduler.schedule(this::renewJSM, delay, TimeUnit.SECONDS);
    }

    private void scheduleMaster() {
        int delay = jobConfig.getJobSchedulerMasterPollIntervalSec();
        if (isMaster.get()) {
            // 1. only single master exists at concurrent time.
            // 2. master's duty: publish job.
            masterScheduler.schedule(this::produceJob, delay, TimeUnit.SECONDS);
        } else {
            masterScheduler.schedule(this::scheduleMaster, delay, TimeUnit.SECONDS);
        }

    }

    private void produceJob() {
        // TODO enum job status
        final String readyStatus = "READY";
        int batchSize = jobConfig.getJobSchedulerMasterPollBatchSize();
        List<String> readyJobIdList = jobInfoMapper.selectJobIdListByStatusBatch(readyStatus, batchSize);
        if (readyJobIdList.isEmpty()) {
            scheduleMaster();
            return;
        }

        String polledJobIdInfo = readyJobIdList.stream().collect(Collectors.joining(",", "[", "]"));
        logger.info("JdbcJobScheduler polled {} jobs: {}", readyJobIdList.size(), polledJobIdInfo);

        for (String jobId : readyJobIdList) {
            int r = scheduleLockMapper.insertLock(jobId, null, null);
            if (r > 0) {
                // TODO enum job status
                jobInfoMapper.updateJobStatus(jobId, "PENDING");
            }
        }

        // maybe un-scheduled job exists, schedule job immediately
        produceJob();
    }

    private void consumeJob() {

        // TODO
        // project level: concurrent job count threshold.

        int batchSize = jobConfig.getJobSchedulerProducerPollBatchSize();
        List<String> jobIdList = scheduleLockMapper.selectNonLockedIdList(batchSize).stream()
                .filter(id -> !JOB_SCHEDULER_MASTER.equals(id)).collect(Collectors.toList());

        // TODO
        // shuffle jobs avoiding jobLock conflict
        // Collections.shuffle(jobIdList);

        final ThreadPoolExecutor jobExecutorPool = jobExecutorPoolRef.get();
        final JobContext jobContext = jobContextRef.get();

        for (String jobId : jobIdList) {
            JobInfo jobInfo = jobInfoMapper.selectByPrimaryKey(jobId);
            if (Objects.isNull(jobInfo)) {
                logger.warn("[LESS_LIKELY_THINGS_HAPPENED] JdbcJobScheduler null job {}", jobId);
            } else {
                final AbstractJobExecutable jobExecutable = getJobExecutable(jobInfo);
                jobContext.add(jobExecutable);
                prepareJob(jobExecutorPool, jobContext, jobExecutable);
            }
        }

        int delay = jobConfig.getJobSchedulerProducerPollIntervalSec();
        slaveScheduler.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
    }

    private void scheduleSlave() {
        int delay = jobConfig.getJobSchedulerProducerPollIntervalSec();
        slaveScheduler.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
    }

    private AbstractJobExecutable getJobExecutable(JobInfo jobInfo) {
        // TODO
        return null;
    }

    private Date nextJobExpireTime() {
        Date current = new Date();
        return DateUtils.addSeconds(current, jobConfig.getJobSchedulerConsumerExpireSec());
    }

    private void prepareJob(ThreadPoolExecutor jobExecutorPool, JobContext jobContext,
            AbstractJobExecutable jobExecutable) {
        if (jobContext.isResourceBlocked(jobExecutable)) {
            return;
        }
        int r = scheduleLockMapper.updateLock(jobExecutable.getJobId(), schedulerInstance, nextJobExpireTime());
        if (r > 0) {
            jobExecutorPool.execute(() -> executeJob(jobContext, jobExecutable));
            logger.info("JdbcJobScheduler submit job: {} {}", jobExecutable.getProject(), jobExecutable.getJobId());
        }
    }

    private void executeJob(JobContext jobContext, AbstractJobExecutable jobExecutable) {
        try (JobExecutor jobExecutor = new JobExecutor(jobContext, jobExecutable)) {
            jobExecutor.execute();
        } catch (Exception e) {
            logger.error("Job execute failed", e);
        }
    }

}
