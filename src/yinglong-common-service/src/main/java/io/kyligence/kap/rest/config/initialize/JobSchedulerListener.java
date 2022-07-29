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

package io.kyligence.kap.rest.config.initialize;

import java.util.Map;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.scheduler.JobAddedNotifier;
import io.kyligence.kap.common.scheduler.JobDiscardNotifier;
import io.kyligence.kap.common.scheduler.JobReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobSchedulerListener {
    
    @Subscribe
    public void onJobIsReady(JobReadyNotifier notifier) {
        //TODO schedule job immediately
        //        NDefaultScheduler.getInstance(notifier.getProject()).fetchJobsImmediately();
    }
    
    @Subscribe
    public void onJobAdded(JobAddedNotifier notifier) {
        String project = notifier.getProject();
        MetricsGroup.hostTagCounterInc(MetricsName.JOB, MetricsCategory.PROJECT, project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), notifier.getJobType());
        MetricsGroup.counterInc(MetricsName.JOB_COUNT, MetricsCategory.PROJECT, project, tags);
    }

    @Subscribe
    public void onJobDiscard(JobDiscardNotifier notifier) {
        String project = notifier.getProject();
        MetricsGroup.hostTagCounterInc(MetricsName.JOB_DISCARDED, MetricsCategory.PROJECT, project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.HOST.getVal(), AddressUtil.getZkLocalInstance());
        tags.put(MetricsTag.JOB_TYPE.getVal(), notifier.getJobType());
        MetricsGroup.counterInc(MetricsName.TERMINATED_JOB_COUNT, MetricsCategory.PROJECT, project, tags);
    }

}
