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

package io.kyligence.kap.rest.scheduler;

import com.google.common.eventbus.Subscribe;
import io.kyligence.kap.common.scheduler.JobCreatedNotifier;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;

public class JobSchedulerListener {
    // only for test usage
    @Getter
    @Setter
    private boolean jobCreatedNotified = false;
    @Getter
    @Setter
    private boolean jobFinishedNotified = false;

    @Subscribe
    public void onJobCreated(JobCreatedNotifier notifier) {
        jobCreatedNotified = true;
        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }

    @Subscribe
    public void onJobFinished(JobFinishedNotifier notifier) {
        jobFinishedNotified = true;
        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
        EventOrchestratorManager.getInstance(KylinConfig.getInstanceFromEnv()).fetchEventsImmediately(notifier.getProject());
    }
}
