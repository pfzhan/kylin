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
package org.apache.kylin.job.runners;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;

public class FetcherRunner extends AbstractDefaultSchedulerRunner {

    private static final Logger logger = LoggerFactory.getLogger(FetcherRunner.class);

    private final ExecutorService jobPool;

    private final ScheduledExecutorService fetcherPool;

    public FetcherRunner(NDefaultScheduler nDefaultScheduler, ExecutorService jobPool,
            ScheduledExecutorService fetcherPool) {
        super(nDefaultScheduler);
        this.jobPool = jobPool;
        this.fetcherPool = fetcherPool;
    }

    private boolean checkSuicide(String jobId) {
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (executableManager.getJob(jobId).getStatus().isFinalState()) {
            return false;
        }
        return executableManager.getJob(jobId).checkSuicide();
    }

    private boolean markDiscardJob(String jobId) {
        try {
            if (checkSuicide(jobId)) {
                return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    if (checkSuicide(jobId)) {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).cancelJob(jobId);
                        return true;
                    }
                    return false;
                }, context.getEpochId(), project);
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} should be suicidal but discard failed", project,
                    jobId, e);
        }
        return false;
    }

    private boolean markErrorJob(String jobId) {
        try {
            return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                val manager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                manager.updateJobOutput(jobId, ExecutableState.ERROR);
                return true;
            }, context.getEpochId(), project);
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} should be error but mark failed", project,
                    jobId, e);
        }
        return false;
    }

    @Override
    public void doRun() {
        try {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            Map<String, Executable> runningJobs = context.getRunningJobs();

            int nRunning = 0;
            int nReady = 0;
            int nStopped = 0;
            int nOthers = 0;
            int nError = 0;
            int nDiscarded = 0;
            int nSucceed = 0;
            int nSuicidal = 0;
            for (final String id : executableManager.getJobs()) {
                if (markDiscardJob(id)) {
                    nDiscarded++;
                    continue;
                }

                if (runningJobs.containsKey(id)) {

                    // this is very important to prevent from same job being scheduled at same time.
                    // e.g. when a job is restarted, the old job may still be running (even if we tried to interrupt it)
                    // until the old job is finished, the new job should not start
                    nRunning++;
                    continue;
                }

                final Output output = executableManager.getOutput(id);
                switch (output.getState()) {
                case READY:
                    nReady++;
                    if (isJobPoolFull() || context.isReachQuotaLimit()) {
                        break;
                    }

                    logger.info("fetcher schedule {} ", id);
                    scheduleJob(id);
                    break;
                case DISCARDED:
                    nDiscarded++;
                    break;
                case ERROR:
                    nError++;
                    break;
                case SUCCEED:
                    nSucceed++;
                    break;
                case PAUSED:
                    nStopped++;
                    break;
                case SUICIDAL:
                    nSuicidal++;
                    break;
                default:
                    if (allSubTasksSuccess(id)) {
                        logger.info("All sub tasks are successful, reschedule job {}", id);
                        scheduleJob(id);
                        break;
                    }
                    logger.warn("Unexpected status for {} <{}>", id, output.getState());
                    if (markErrorJob(id)) {
                        nError++;
                    } else {
                        nOthers++;
                    }
                    break;
                }
            }

            logger.info(
                    "Job Status in project {}: {} should running, {} actual running, {} stopped, {} ready, {} already succeed, {} error, {} discarded, {} suicidal,  {} others",
                    project, nRunning, runningJobs.size(), nStopped, nReady, nSucceed, nError, nDiscarded, nSuicidal,
                    nOthers);
        } catch (Exception e) {
            logger.warn("Job Fetcher caught a exception ", e);
        }
    }

    private boolean allSubTasksSuccess(String id) {
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        // check special case, all sub task success, show make current job to success
        AbstractExecutable job = executableManager.getJob(id);
        if (job instanceof DefaultChainedExecutable) {
            return ((DefaultChainedExecutable) job).getTasks().stream().allMatch(
                    abstractExecutable -> abstractExecutable.getStatus() == ExecutableState.SUCCEED);
        }

        return false;
    }

    private void scheduleJob(String id) {
        AbstractExecutable executable = null;
        String jobDesc = null;

        boolean memoryLock = false;
        int useMemoryCapacity = 0;
        try {
            val config = KylinConfig.getInstanceFromEnv();
            val executableManager = NExecutableManager.getInstance(config, project);
            executable = executableManager.getJob(id);
            useMemoryCapacity = executable.computeStepDriverMemory();

            memoryLock = NDefaultScheduler.getMemoryRemaining().tryAcquire(useMemoryCapacity);
            if (memoryLock) {
                jobDesc = executable.toString();
                logger.info("{} prepare to schedule", jobDesc);
                context.addRunningJob(executable);
                jobPool.execute(new JobRunner(nDefaultScheduler, executable, this));
                logger.info("{} scheduled", jobDesc);
            } else {
                logger.info("memory is not enough, remaining: {} MB",
                        NDefaultScheduler.getMemoryRemaining().availablePermits());
            }
        } catch (Exception ex) {
            if (executable != null) {
                context.removeRunningJob(executable);
                if (memoryLock) {
                    // may release twice when exception raise after jobPool execute executable
                    NDefaultScheduler.getMemoryRemaining().release(useMemoryCapacity);
                }
            }
            logger.warn("{} fail to schedule", jobDesc, ex);
        }
    }

    private boolean isJobPoolFull() {
        if (context.getRunningJobs().size() >= nDefaultScheduler.getJobEngineConfig().getMaxConcurrentJobLimit()) {
            logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time.");
            return true;
        }
        return false;
    }

    void scheduleNext() {
        fetcherPool.schedule(this, 0, TimeUnit.SECONDS);
    }
}
