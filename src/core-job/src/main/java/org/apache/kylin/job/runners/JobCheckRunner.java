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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.scheduler.JdbcJobScheduler;
import io.kyligence.kap.job.scheduler.JobExecutor;
import lombok.val;

public class JobCheckRunner implements Runnable {

    private JobContext jobContext;

    private static final Logger logger = LoggerFactory.getLogger(JobCheckRunner.class);

    public JobCheckRunner(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    private boolean discardTimeoutJob(String jobId, String project, Long startTime) {
        Integer timeOutMinute = KylinConfig.getInstanceFromEnv().getSchedulerJobTimeOutMinute();
        if (timeOutMinute == 0) {
            return false;
        }
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AbstractExecutable jobExecutable = executableManager.getJob(jobId);
        try {
            if (checkTimeoutIfNeeded(jobExecutable, startTime, timeOutMinute)) {
                logger.error("project {} job {} running timeout.", project, jobId);
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).errorJob(jobId);
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project " + project + " job " + jobId
                    + " should be timeout but discard failed", e);
        }
        return false;
    }

    private boolean checkTimeoutIfNeeded(AbstractExecutable jobExecutable, Long startTime, Integer timeOutMinute) {
        if (jobExecutable.getStatusInMem().isFinalState()) {
            return false;
        }
        long duration = System.currentTimeMillis() - startTime;
        long durationMins = Math.toIntExact(duration / (60 * 1000));
        return durationMins >= timeOutMinute;
    }

    @Override
    public void run() {
        logger.info("Start check job pool.");
        JdbcJobScheduler jdbcJobScheduler = jobContext.getJobScheduler();
        Map<String, JobExecutor> runningJobs = jdbcJobScheduler.getRunningJob();
        for (Map.Entry<String, JobExecutor> entry : runningJobs.entrySet()) {
            String jobId = entry.getKey();
            JobExecutor jobExecutor = entry.getValue();
            String project = jobExecutor.getJobExecutable().getProject();
            discardTimeoutJob(jobId, project, jobExecutor.getStartTime());
            stopJobIfStorageQuotaLimitReached(jobContext, jobId, project);
        }
    }

    private void stopJobIfStorageQuotaLimitReached(JobContext jobContext, String jobId, String project) {
        if (!KylinConfig.getInstanceFromEnv().isCheckQuotaStorageEnabled()) {
            return;
        }
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        ExecutablePO executablePO = executableManager.getExecutablePO(jobId);
        AbstractExecutable jobExecutable = executableManager.fromPO(executablePO);
        JobCheckUtil.stopJobIfStorageQuotaLimitReached(jobContext, executablePO, jobExecutable);
    }
}