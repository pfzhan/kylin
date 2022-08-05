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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.constant.JobStatusEnum;
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
import lombok.val;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JdbcJobScheduler.class);

    private final JobContext jobContext;

    private final AtomicBoolean isMaster;

    // job id -> (executable, job scheduled time)
    private final Map<String, Pair<AbstractJobExecutable, Long>> runningJobMap;

    private JdbcJobLock masterLock;

    private ScheduledExecutorService master;

    private ScheduledExecutorService slave;

    private ThreadPoolExecutor executorPool;
    
    private int consumerMaxThreads;

    public JdbcJobScheduler(JobContext jobContext) {
        this.jobContext = jobContext;
        this.isMaster = new AtomicBoolean(false);
        this.runningJobMap = Maps.newConcurrentMap();
        this.consumerMaxThreads = jobContext.getJobConfig().getJobSchedulerConsumerMaxThreads();
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

    public Map<String, Pair<AbstractJobExecutable, Long>> getRunningJob() {
        return Collections.unmodifiableMap(runningJobMap);
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
        executorPool = ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Executor", 1, this.consumerMaxThreads,
                5, TimeUnit.MINUTES);

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
            
            int batchSize = jobContext.getJobConfig().getJobSchedulerMasterPollBatchSize();
            List<String> readyJobIdList = jobContext.getJobInfoMapper()
                    .findJobIdListByStatusBatch(JobStatusEnum.READY.name(), batchSize);
            if (readyJobIdList.isEmpty()) {
                return;
            }

            String polledJobIdInfo = readyJobIdList.stream().collect(Collectors.joining(",", "[", "]"));
            logger.info("Scheduler polled jobs: {} {}", readyJobIdList.size(), polledJobIdInfo);

            for (String jobId : readyJobIdList) {
                if (!jobContext.getParallelLimiter().tryAcquire()) {
                    return;
                }

                if (JobCheckUtil.markSuicideJob(jobId, jobContext)){
                    logger.info("suicide job = {} on produce", jobId);
                    continue;
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
            // The number of tasks to be obtained cannot exceed the free slots of the 'executorPool'
            int exeFreeSlots = this.consumerMaxThreads - this.runningJobMap.size();
            if (exeFreeSlots == 0) {
                logger.info("No free slots to execute job");
                return;
            }
            int batchSize = jobContext.getJobConfig().getJobSchedulerSlavePollBatchSize();
            if (exeFreeSlots < batchSize) {
                batchSize = exeFreeSlots;
            }
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

            // submit job
            jobIdList.forEach(jobId -> {
                // check 'runningJobMap', avoid processing submitted tasks
                if (runningJobMap.containsKey(jobId)) {
                    logger.warn("Job {} has already been submitted", jobId);
                    return;
                }
                Pair<JobInfo, AbstractJobExecutable> job = fetchJob(jobId);
                if (null == job) {
                    return;
                }
                final JobInfo jobInfo = job.getFirst();
                final AbstractJobExecutable jobExecutable = job.getSecond();
                if (!canSubmitJob(jobId, jobInfo, jobExecutable)) {
                    return;
                }
                // record running job id, and job scheduled time
                runningJobMap.put(jobId, new Pair<>(jobExecutable, System.currentTimeMillis()));
                executorPool.execute(() -> executeJob(jobExecutable, jobInfo));
            });
        } catch (Exception e) {
            logger.error("Something's wrong when consuming job", e);
        } finally {
            logger.info("{} running jobs in current scheduler", getRunningJob().size());
            slave.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
        }
    }

    private Pair<JobInfo, AbstractJobExecutable> fetchJob(String jobId) {
        try {
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            if (jobInfo == null){
                logger.warn("can not find job info {}", jobId);
                jobContext.getJobLockMapper().removeLock(jobId, null);
                return null;
            }
            AbstractJobExecutable jobExecutable = getJobExecutable(jobInfo);
            return new Pair<>(jobInfo, jobExecutable);
        } catch (Throwable throwable) {
            logger.error("Fetch job failed, job id: " + jobId, throwable);
            return null;
        }
    }

    private boolean canSubmitJob(String jobId, JobInfo jobInfo, AbstractJobExecutable jobExecutable) {
        try {
            if (JobScheduler.MASTER_SCHEDULER.equals(jobId)) {
                return false;
            }
            if (Objects.isNull(jobInfo)) {
                logger.warn("Job not found: {}", jobId);
                return false;
            }
            if (!jobContext.getResourceAcquirer().tryAcquire(jobExecutable)) {
                return false;
            }
        } catch (Throwable throwable) {
            logger.error("Error when preparing to submit job: " + jobId, throwable);
            return false;
        }
        return true;
    }

    private void executeJob(AbstractJobExecutable jobExecutable, JobInfo jobInfo) {
        try (JobExecutor jobExecutor = new JobExecutor(jobContext, jobExecutable)) {
            JdbcJobLock jobLock = tryJobLock(jobExecutable);
            if (null == jobLock) {
                return;
            }

            if (jobContext.isProjectReachQuotaLimit(jobExecutable.getProject())
                    && JobCheckUtil.stopJobIfStorageQuotaLimitReached(jobContext, jobInfo, jobExecutable)) {
                jobLock.tryRelease();
                return;
            }

            if (!checkJobStatusBeforeExecute(jobExecutable, jobLock)) {
                return;
            }

            try {
                // heavy action
                jobExecutor.execute();
            } finally {
                releaseJobLockAfterExecute(jobLock);
            }
        } catch (Throwable t) {
            logger.error("Execute job failed " + jobExecutable.getJobId(), t);
        } finally {
            runningJobMap.remove(jobExecutable.getJobId());
        }
    }

    private JdbcJobLock tryJobLock(AbstractJobExecutable jobExecutable) throws LockException {
        JdbcJobLock jobLock = new JdbcJobLock(jobExecutable.getJobId(), jobContext.getServerNode(),
                jobContext.getJobConfig().getJobSchedulerJobRenewalSec(),
                jobContext.getJobConfig().getJobSchedulerJobRenewalRatio(), jobContext.getLockClient(),
                new JobAcquireListener(jobExecutable));
        if (!jobLock.tryAcquire()) {
            logger.info("Acquire job lock failed.");
            return null;
        }
        return jobLock;
    }

    private boolean checkJobStatusBeforeExecute(AbstractJobExecutable jobExecutable, JdbcJobLock jobLock) throws LockException {
        AbstractExecutable executable = (AbstractExecutable) jobExecutable;
        ExecutableState jobStatus = executable.getStatus();
        if (ExecutableState.PENDING == jobStatus) {
            return true;
        }
        logger.warn("Unexpected status for {} <{}>, should not execute job", jobExecutable.getJobId(), jobStatus);
        if (ExecutableState.RUNNING == jobStatus) {
            // there should be other nodes crashed during job execution, resume job status from running to ready
            logger.warn("Resume <RUNNING> job {}", jobExecutable.getJobId());
            ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), executable.getProject())
                    .resumeJob(jobExecutable.getJobId(), executable, true);
        }
        // for other status, it should be caused by the failure of release locks, so just release lock agagin
        logger.warn("Release lock for {} <{}>", jobExecutable.getJobId(), jobStatus);
        jobLock.tryRelease();
        return false;
    }

    private void releaseJobLockAfterExecute(JdbcJobLock jobLock) throws LockException {
        try {
            String jobId = jobLock.getLockId();
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            AbstractExecutable jobExecutable = (AbstractExecutable) getJobExecutable(jobInfo);
            if (jobExecutable.getStatusInMem().isProgressing()) {
                logger.error("Unexpected status for {} <{}>, mark job error", jobId, jobExecutable.getStatusInMem());
                markErrorJob(jobId, jobExecutable.getProject());
            }
        } catch (Throwable t) {
            logger.error("Fail to check status before release job lock {}, stop renew job lock", jobLock.getLockId());
            jobLock.stopRenew();
            throw t;
        }
        jobLock.tryRelease();
    }

    private void markErrorJob(String jobId, String project) {
        try {
            val manager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.errorJob(jobId);
        } catch (Throwable t) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} should be error but mark failed", project,
                    jobId, t);
            throw t;
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
