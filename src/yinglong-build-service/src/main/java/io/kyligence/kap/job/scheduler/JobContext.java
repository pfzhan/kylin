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

import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.spark.utils.ThreadUtils;
import io.kyligence.kap.job.core.AbstractJobConfig;
import io.kyligence.kap.job.core.AbstractJobExecutable;

public class JobContext {

    private static final Logger logger = LoggerFactory.getLogger(JobContext.class);

    // thread safe

    private final AbstractJobConfig jobConfig;

    private final ResourceBlocker resourceBlocker;
    private final RestfulProgressReporter progressReporter;

    private ScheduledExecutorService progressReportScheduler;

    public void close() {
        progressReporter.close();
    }

    public JobContext(AbstractJobConfig jobConfig) {
        this.jobConfig = jobConfig;

        resourceBlocker = new ResourceBlocker(jobConfig);
        // TODO
        // progressReporter = jobConfig.getJobProgressReporter();
        progressReporter = new RestfulProgressReporter();

        int corePoolSize = jobConfig.getJobProgressReporterMaxThreads();
        progressReportScheduler = ThreadUtils.newDaemonThreadScheduledExecutor(corePoolSize, "JobProgressReporter");
    }

    public boolean isResourceBlocked(AbstractJobExecutable jobExecutable) {
        return resourceBlocker.isBlocked(jobExecutable);
    }

    public void add(AbstractJobExecutable jobExecutable) {
        resourceBlocker.onJobRegistered(jobExecutable);
    }

    public void remove(AbstractJobExecutable jobExecutable) {
        resourceBlocker.onJobUnregistered(jobExecutable);
    }

    public void onJobStarted(JobExecutor jobExecutor) {

    }

    public void onJobStopped(JobExecutor jobExecutor) {

    }

    public void onJobFailed(JobExecutor jobExecutor) {

    }

}
