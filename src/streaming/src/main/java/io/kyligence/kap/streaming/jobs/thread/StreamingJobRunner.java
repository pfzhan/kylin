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

package io.kyligence.kap.streaming.jobs.thread;

import org.apache.kylin.job.execution.JobTypeEnum;

import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.jobs.impl.StreamingJobLauncher;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobRunner implements Runnable {

    private String project;
    private String modelId;
    private JobTypeEnum jobType;

    public StreamingJobRunner(String project, String modelId, JobTypeEnum jobType) {
        this.project = project;
        this.modelId = modelId;
        this.jobType = jobType;
        jobLauncher = new StreamingJobLauncher();
    }

    private StreamingJobLauncher jobLauncher;

    @Override
    public void run() {
        launchJob();
    }

    public void stop() {
        stopJob();
    }

    public void init() {
        jobLauncher.init(project, modelId, jobType);
    }

    private void launchJob() {
        jobLauncher.init(project, modelId, jobType);
        jobLauncher.launch();
    }

    private void stopJob() {
        if (!jobLauncher.isInitialized()) {
            log.warn("Streaming job launcher {} not exists...", StreamingUtils.getJobId(modelId, jobType.name()));
            return;
        }
        jobLauncher.stop();
    }
}
