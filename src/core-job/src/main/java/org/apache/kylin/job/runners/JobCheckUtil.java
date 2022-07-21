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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import io.kyligence.kap.common.util.ThreadUtils;
import io.kyligence.kap.job.JobContext;
import io.kyligence.kap.job.core.AbstractJobExecutable;
import io.kyligence.kap.job.domain.JobInfo;
import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.job.util.JobInfoUtil;

public class JobCheckUtil {

    private static final Logger logger = LoggerFactory.getLogger(JobCheckUtil.class);

    private static ScheduledExecutorService jobCheckThreadPool;

    synchronized private static ScheduledExecutorService getJobCheckThreadPool() {
        if (null == jobCheckThreadPool) {
            jobCheckThreadPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JobCheckThreadPool");
        }
        return jobCheckThreadPool;
    }

    public static void startQuotaStorageCheckRunner(QuotaStorageCheckRunner quotaStorageCheckRunner) {
        if (!KylinConfig.getInstanceFromEnv().isCheckQuotaStorageEnabled()) {
            return;
        }
        int pollSecond = KylinConfig.getInstanceFromEnv().getSchedulerPollIntervalSecond();
        getJobCheckThreadPool().scheduleWithFixedDelay(quotaStorageCheckRunner, RandomUtils.nextInt(0, pollSecond),
                pollSecond, TimeUnit.SECONDS);
    }

    public static void startJobCheckRunner(JobCheckRunner jobCheckRunner) {
        int pollSecond = KylinConfig.getInstanceFromEnv().getSchedulerPollIntervalSecond();
        getJobCheckThreadPool().scheduleWithFixedDelay(jobCheckRunner, RandomUtils.nextInt(0, pollSecond),
                pollSecond, TimeUnit.SECONDS);
    }

    public static boolean stopJobIfStorageQuotaLimitReached(JobContext jobContext, JobInfo jobInfo,
            AbstractJobExecutable jobExecutable) {
        return stopJobIfStorageQuotaLimitReached(jobContext, JobInfoUtil.deserializeExecutablePO(jobInfo),
                jobExecutable);
    }

    public static boolean stopJobIfStorageQuotaLimitReached(JobContext jobContext, ExecutablePO executablePO,
            AbstractJobExecutable jobExecutable) {
        if (!KylinConfig.getInstanceFromEnv().isCheckQuotaStorageEnabled()) {
            return false;
        }
        String jobId = executablePO.getId();
        String project = jobExecutable.getProject();
        try {
            if (jobContext.isProjectReachQuotaLimit(project)) {
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(jobId, executablePO,
                        (AbstractExecutable) jobExecutable);
                logger.info("Job {} paused due to no available storage quota.", jobId);
                logger.info("Please clean up low-efficient storage in time, "
                        + "increase the low-efficient storage threshold, "
                        + "or notify the administrator to increase the storage quota for this project.");
                return true;
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} failed to pause", project, jobId, e);
        }
        return false;
    }

    public static boolean markSuicideJob(String jobId, JobContext jobContext) {
        try {
            if (checkSuicide(jobId, jobContext)) {
                return JdbcUtil.withTransaction(jobContext.getTransactionManager(), () -> {
                    if (checkSuicide(jobId, jobContext)) {
                        JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
                        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                                .suicideJob(jobId);
                        return true;
                    }
                    return false;
                });
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED]  job {} should be suicidal but discard failed", jobId, e);
        }
        return false;
    }

    private static boolean checkSuicide(String jobId, JobContext jobContext) {
        JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
        AbstractExecutable job = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                .getJob(jobId);
        if (job.getStatus().isFinalState()) {
            return false;
        }
        return job.checkSuicide();
    }

}
