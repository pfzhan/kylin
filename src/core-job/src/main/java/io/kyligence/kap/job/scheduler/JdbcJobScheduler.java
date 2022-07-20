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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.ThreadUtils;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.core.AbstractJobExecutable;
import io.kyligence.kap.job.core.lock.JdbcJobLock;
import io.kyligence.kap.job.core.lock.LockAcquireListener;
import io.kyligence.kap.job.core.lock.LockException;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.JobInfoUtil;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JdbcJobScheduler.class);

    private final JobContext jobContext;

    private final AtomicBoolean isMaster;

    private final Map<String, JobExecutor> runningJobMap;

    private JdbcJobLock masterLock;

    private ScheduledExecutorService master;

    private ScheduledExecutorService slave;

    private ThreadPoolExecutor executorPool;

    public JdbcJobScheduler(JobContext jobContext) {
        this.jobContext = jobContext;
        this.isMaster = new AtomicBoolean(false);
        this.runningJobMap = Maps.newConcurrentMap();
    }

    @Override
    public void publishJob() {
        // master lock
        masterLock = new JdbcJobLock(JobScheduler.MASTER_SCHEDULER, jobContext.getServerNode(),
                jobContext.getJobConfig().getJobSchedulerMasterRenewalSec(),
                jobContext.getJobConfig().getJobSchedulerMasterRenewalRatio(), jobContext.getLockClient(),
                new MasterAcquireListener());
        // standby: acquire master lock
        master.schedule(this::standby, 0, TimeUnit.SECONDS);

        // master: publish job
        master.schedule(this::produceJob, 0, TimeUnit.SECONDS);
    }

    @Override
    public void subscribeJob() {
        // slave: subscribe job
        slave.schedule(this::consumeJob, 0, TimeUnit.SECONDS);
    }

    private boolean hasRunningJob() {
        return !runningJobMap.isEmpty();
    }

    public Map<String, JobExecutor> getRunningJob() {
        return runningJobMap;
    }

    @Override
    public String getJobNode(String jobId) {
        String jobNode = jobContext.getJobLockMapper().findNodeByLockId(jobId);
        if (Objects.isNull(jobNode)) {
            return jobContext.getServerNode();
        }
        return jobNode;
    }

    public void start() {
        // standby: acquire JSM
        // publish job:  READY -> PENDING
        master = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Master");

        // subscribe job: PENDING -> RUNNING
        slave = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Slave");

        // execute job: RUNNING -> FINISHED
        executorPool = ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Executor", 1,
                jobContext.getJobConfig().getJobSchedulerConsumerMaxThreads(), 5, TimeUnit.MINUTES);

        publishJob();
        subscribeJob();
    }

    public void destroy() {

        if (Objects.nonNull(masterLock)) {
            try {
                masterLock.tryRelease();
            } catch (LockException e) {
                logger.error("Something's wrong when removing master lock", e);
            }
        }

        if (Objects.nonNull(master)) {
            master.shutdownNow();
        }

        if (Objects.nonNull(slave)) {
            slave.shutdownNow();
        }

        if (Objects.nonNull(executorPool)) {
            executorPool.shutdownNow();
        }
    }

    private void standby() {
        try {
            masterLock.tryAcquire();
        } catch (LockException e) {
            logger.error("Something's wrong when acquiring master lock.", e);
        }
    }

    private void produceJob() {
        long delaySec = jobContext.getJobConfig().getJobSchedulerMasterPollIntervalSec();
        try {
            // only master can publish job
            if (!isMaster.get()) {
                return;
            }

            // parallel job count threshold
            if (!jobContext.getParallelLimiter().tryRelease()) {
                return;
            }

            // TODO enum job status
            final String readyStatus = "READY";
            int batchSize = jobContext.getJobConfig().getJobSchedulerMasterPollBatchSize();

            List<String> readyJobIdList = jobContext.getJobInfoMapper().findJobIdListByStatusBatch(readyStatus,
                    batchSize);
            if (readyJobIdList.isEmpty()) {
                return;
            }

            String polledJobIdInfo = readyJobIdList.stream().collect(Collectors.joining(",", "[", "]"));
            logger.info("Scheduler polled jobs: {} {}", readyJobIdList.size(), polledJobIdInfo);

            for (String jobId : readyJobIdList) {
                if (!jobContext.getParallelLimiter().tryAcquire()) {
                    return;
                }

                JdbcUtil.withTransaction(jobContext.getTransactionManager(), () -> {
                    int r = jobContext.getJobLockMapper().upsertLock(jobId, null, 0L);
                    if (r > 0) {
                        JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
                        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                                .publishJob(jobId, (AbstractExecutable) getJobExecutable(jobInfo));
                    }
                    return null;
                });
            }

            // maybe more jobs exist, publish job immediately
            delaySec = 0;
        } catch (Exception e) {
            logger.error("Something's wrong when publishing job", e);
        } finally {
            master.schedule(this::produceJob, delaySec, TimeUnit.SECONDS);
        }
    }

    private void consumeJob() {

        long delay = jobContext.getJobConfig().getJobSchedulerSlavePollIntervalSec();
        try {

            int batchSize = jobContext.getJobConfig().getJobSchedulerSlavePollBatchSize();
            List<String> jobIdList = jobContext.getJobLockMapper().findNonLockIdList(batchSize);
            if (CollectionUtils.isEmpty(jobIdList)) {
                return;
            }

            // TODO
            // Shuffle jobs avoiding jobLock conflict.
            // At the same time, we should ensure the overall order.
            if (hasRunningJob()) {
                Collections.shuffle(jobIdList);
            }

            CountDownLatch countDownLatch = new CountDownLatch(jobIdList.size());
            // submit job
            jobIdList.forEach(jobId -> {
                if (JobScheduler.MASTER_SCHEDULER.equals(jobId)) {
                    countDownLatch.countDown();
                    return;
                }
                JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
                if (Objects.isNull(jobInfo)) {
                    logger.warn("Job not found: {}", jobId);
                    countDownLatch.countDown();
                    return;
                }
                final AbstractJobExecutable jobExecutable = getJobExecutable(jobInfo);
                if (!jobContext.getResourceAcquirer().tryAcquire(jobExecutable)) {
                    countDownLatch.countDown();
                    return;
                }
                executorPool.execute(() -> executeJob(jobExecutable, jobInfo, countDownLatch));
            });
            countDownLatch.await(jobContext.getJobConfig().getJobSchedulerSlaveLockBatchJobsWaitSec(), TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Something's wrong when consuming job", e);
        } finally {
            slave.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
        }
    }

    private void executeJob(AbstractJobExecutable jobExecutable, JobInfo jobInfo, CountDownLatch countDownLatch) {
        try (JobExecutor jobExecutor = new JobExecutor(jobContext, jobExecutable)) {
            JdbcJobLock jobLock = new JdbcJobLock(jobExecutable.getJobId(), jobContext.getServerNode(),
                    jobContext.getJobConfig().getJobSchedulerJobRenewalSec(),
                    jobContext.getJobConfig().getJobSchedulerJobRenewalRatio(), jobContext.getLockClient(),
                    new JobAcquireListener(jobExecutable));
            if (!tryJobLock(jobLock, countDownLatch)) {
                return;
            }

            if (jobContext.isProjectReachQuotaLimit(jobExecutable.getProject())
                    && JobCheckUtil.stopJobIfStorageQuotaLimitReached(jobContext, jobInfo, jobExecutable)) {
                jobLock.tryRelease();
                return;
            }
            
            AbstractExecutable executable = (AbstractExecutable) jobExecutable;
            if (executable.getStatus().equals(ExecutableState.RUNNING)) {
                // resume job status from running to ready
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), executable.getProject())
                        .resumeJob(jobExecutable.getJobId(), executable, true);
                jobLock.tryRelease();
                return;
            }

            jobExecutor.setStartTime(System.currentTimeMillis());
            runningJobMap.put(jobExecutable.getJobId(), jobExecutor);

            try {
                // heavy action
                jobExecutor.execute();
            } finally {
                jobLock.tryRelease();
            }
        } catch (Exception e) {
            logger.error("Execute job failed", e);
        } finally {
            runningJobMap.remove(jobExecutable.getJobId());
        }
    }

    private boolean tryJobLock(JdbcJobLock jobLock, CountDownLatch countDownLatch) throws LockException {
        try {
            if (!jobLock.tryAcquire()) {
                logger.info("Acquire job lock failed.");
                return false;
            }
            return true;
        } finally {
            countDownLatch.countDown();
        }

    }

    private AbstractJobExecutable getJobExecutable(JobInfo jobInfo) {
        ExecutablePO executablePO = JobInfoUtil.deserializeExecutablePO(jobInfo);
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                .fromPO(executablePO);
    }

    private class MasterAcquireListener implements LockAcquireListener {

        @Override
        public void onSucceed() {
            if (isMaster.compareAndSet(false, true)) {
                logger.info("Job scheduler become master.");
            }
        }

        @Override
        public void onFailed() {
            if (isMaster.compareAndSet(true, false)) {
                logger.info("Job scheduler fallback standby.");
            }
            // standby
            master.schedule(JdbcJobScheduler.this::standby, masterLock.getRenewalSec(), TimeUnit.SECONDS);
        }
    }

    private class JobAcquireListener implements LockAcquireListener {

        private final AbstractJobExecutable jobExecutable;

        JobAcquireListener(AbstractJobExecutable jobExecutable) {
            this.jobExecutable = jobExecutable;
        }

        @Override
        public void onSucceed() {
            // do nothing
        }

        @Override
        public void onFailed() {
            jobExecutable.cancelJob();
        }
    }

}
