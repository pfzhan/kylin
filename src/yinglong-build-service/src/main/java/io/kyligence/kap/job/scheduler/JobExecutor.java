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

import java.util.Locale;

import io.kyligence.kap.job.core.AbstractJobExecutable;

public class JobExecutor implements AutoCloseable {

    private final JobContext jobContext;
    private final AbstractJobExecutable jobExecutable;

    private final String originThreadName;

    public JobExecutor(JobContext jobContext, AbstractJobExecutable jobExecutable) {
        this.jobContext = jobContext;
        this.jobExecutable = jobExecutable;

        this.originThreadName = Thread.currentThread().getName();
        setThreadName();

    }

    public void execute() throws Exception {

    }

    private void setThreadName() {
        String project = jobExecutable.getProject();
        String jobFlag = jobExecutable.getJobId().substring(0, 8);
        Thread.currentThread().setName(String.format(Locale.ROOT, "JobExecutor(project:%s,job:%s)", project, jobFlag));
    }

    private void setbackThreadName() {
        Thread.currentThread().setName(originThreadName);
    }

    @Override
    public void close() throws Exception {
        jobContext.remove(jobExecutable);

        // setback thread name
        setbackThreadName();
    }
}
