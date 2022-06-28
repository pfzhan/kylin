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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.logging.SetLogCategory;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.SneakyThrows;

public abstract class AbstractDefaultSchedulerRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDefaultSchedulerRunner.class);
    protected final NDefaultScheduler nDefaultScheduler;

    protected final ExecutableContext context;

    protected final String project;

    public AbstractDefaultSchedulerRunner(final NDefaultScheduler nDefaultScheduler) {
        this.nDefaultScheduler = nDefaultScheduler;
        this.context = nDefaultScheduler.getContext();
        this.project = nDefaultScheduler.getProject();
    }

    @SneakyThrows
    private boolean checkEpochIdFailed() {
        //check failed if isInterrupted
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        if (!KylinConfig.getInstanceFromEnv().isUTEnv()
                && !EpochManager.getInstance().checkEpochId(context.getEpochId(), project)) {
            nDefaultScheduler.forceShutdown();
            return true;
        }
        return false;
    }

    // stop job if Storage Quota Limit reached
    protected void stopJobIfSQLReached(String jobId) {
        if (context.isReachQuotaLimit()) {
            try {
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    if (context.isReachQuotaLimit()) {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(jobId);
                        logger.info("Job {} paused due to no available storage quota.", jobId);
                        logger.info("Please clean up low-efficient storage in time, "
                                + "increase the low-efficient storage threshold, "
                                + "or notify the administrator to increase the storage quota for this project.");
                    }
                    return null;
                }, project, UnitOfWork.DEFAULT_MAX_RETRY, context.getEpochId(), jobId);
            } catch (Exception e) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} failed to pause", project, jobId, e);
            }
        }
    }

    @Override
    public void run() {
        try (SetLogCategory ignored = new SetLogCategory("schedule")) {
            if (checkEpochIdFailed()) {
                return;
            }
            doRun();
        }
    }

    protected abstract void doRun();
}
