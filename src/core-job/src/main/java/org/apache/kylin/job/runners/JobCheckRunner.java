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
package org.apache.kylin.job.runners;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.scheduler.JdbcJobScheduler;
import org.apache.kylin.job.util.JobContextUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                return JobContextUtil.withTxAndRetry(() -> {
                    ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).errorJob(jobId);
                    return true;
                });
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
        Map<String, Pair<AbstractJobExecutable, Long>> runningJobs = jdbcJobScheduler.getRunningJob();
        for (Map.Entry<String, Pair<AbstractJobExecutable, Long>> entry : runningJobs.entrySet()) {
            String jobId = entry.getKey();
            AbstractJobExecutable jobExecutable = entry.getValue().getFirst();
            long startTime = entry.getValue().getSecond();
            String project = jobExecutable.getProject();
            if (JobCheckUtil.markSuicideJob(jobId, jobContext)) {
                logger.info("suicide job = {} on checker runner", jobId);
                continue;
            }
            if (discardTimeoutJob(jobId, project, startTime)) {
                logger.info("discardTimeoutJob job = {} on checker runner", jobId);
                continue;
            }
            if (stopJobIfStorageQuotaLimitReached(jobContext, jobId, project)) {
                logger.info("stopJobIfStorageQuotaLimitReached job = {} on checker runner", jobId);
                continue;
            }
        }
    }

    private boolean stopJobIfStorageQuotaLimitReached(JobContext jobContext, String jobId, String project) {
        if (!KylinConfig.getInstanceFromEnv().isStorageQuotaEnabled()) {
            return false;
        }
        val executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        ExecutablePO executablePO = executableManager.getExecutablePO(jobId);
        AbstractExecutable jobExecutable = executableManager.fromPO(executablePO);
        return JobCheckUtil.stopJobIfStorageQuotaLimitReached(jobContext, executablePO, jobExecutable);
    }
}
