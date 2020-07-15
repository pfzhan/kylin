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
import io.kyligence.kap.common.scheduler.CubingJobFinishedNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.scheduler.JobFinishedNotifier;
import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.manager.SegmentAutoMergeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobSchedulerListener {
    // only for test usage
    @Getter
    @Setter
    private boolean jobReadyNotified = false;
    @Getter
    @Setter
    private boolean jobFinishedNotified = false;

    private static Logger logger = LoggerFactory.getLogger(JobSchedulerListener.class);

    @Subscribe
    public void onJobIsReady(JobReadyNotifier notifier) {
        jobReadyNotified = true;
        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }

    @Subscribe
    public void onJobFinished(JobFinishedNotifier notifier) {
        jobFinishedNotified = true;
        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }

    @Subscribe
    public void onCubingJobFinished(CubingJobFinishedNotifier notifier) {
        try {
            SegmentAutoMergeUtil.autoMergeSegments(notifier.getProject(), notifier.getModelId(), notifier.getOwner());
        } catch (Throwable e) {
            logger.error("Auto merge failed on project {} model {}", notifier.getProject(), notifier.getModelId(), e);
        }
    }

}
