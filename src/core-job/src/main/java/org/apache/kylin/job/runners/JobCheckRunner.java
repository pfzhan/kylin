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
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;

public class JobCheckRunner extends AbstractDefaultSchedulerRunner {

    private static final Logger logger = LoggerFactory.getLogger(JobCheckRunner.class);

    public JobCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);
    }

    private boolean checkTimeoutIfNeeded(String jobId, Long startTime) {
        Integer timeOutMinute = KylinConfig.getInstanceFromEnv().getSchedulerJobTimeOutMinute();
        if (timeOutMinute == 0) {
            return false;
        }
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (executableManager.getJob(jobId).getStatus().isFinalState()) {
            return false;
        }
        long duration = System.currentTimeMillis() - startTime;
        long durationMins = Math.toIntExact(duration / (60 * 1000));
        return durationMins >= timeOutMinute;
    }

    private boolean discardTimeoutJob(String jobId, Long startTime) {
        try {
            if (checkTimeoutIfNeeded(jobId, startTime)) {
                return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    if (checkTimeoutIfNeeded(jobId, startTime)) {
                        logger.error("project {} job {} running timeout.", project, jobId);
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).errorJob(jobId);
                        return true;
                    }
                    return false;
                }, project, UnitOfWork.DEFAULT_MAX_RETRY, context.getEpochId(), jobId);
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project " + project + " job " + jobId
                    + " should be timeout but discard failed", e);
        }
        return false;
    }

    @Override
    protected void doRun() {

        logger.info("start check project {} job pool.", project);

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<String, Executable> runningJobs = context.getRunningJobs();
        Map<String, Long> runningJobInfos = context.getRunningJobInfos();
        for (final String id : executableManager.getJobs()) {
            if (runningJobs.containsKey(id)) {
                discardTimeoutJob(id, runningJobInfos.get(id));
                stopJobIfSQLReached(id);
            }
        }

    }
}
