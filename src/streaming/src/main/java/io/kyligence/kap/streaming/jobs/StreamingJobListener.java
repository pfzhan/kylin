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

package io.kyligence.kap.streaming.jobs;

import com.google.common.collect.Sets;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.spark.launcher.SparkAppHandle;

import java.util.Locale;

class StreamingJobListener implements SparkAppHandle.Listener {

    private String project;
    private String jobId;
    private String runnnable;

    public StreamingJobListener(String project, String jobId) {
        this.project = project;
        this.jobId = jobId;
    }

    @Override
    public void stateChanged(SparkAppHandle handler) {
        if (handler.getState().isFinal()) {
            runnnable = null;
            if(isFailed(handler.getState())) {
                MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.ERROR,
                        "The streaming job {} has terminated unexpectedly…");
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                HDFSUtils.touchzMarkFile(config.getStreamingBaseJobsLocation()
                        + String.format(Locale.ROOT, StreamingConstants.JOB_SHUTDOWN_FILE_PATH, project, jobId));
                handler.kill();
            } else if(isFinished(handler.getState())) {
                MetaInfoUpdater.updateJobState(project, jobId, Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED),
                        JobStatusEnum.STOPPED, null);
            }
        } else if (runnnable == null && SparkAppHandle.State.RUNNING == handler.getState()) {
            runnnable = "true";
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.RUNNING);
        }
    }

    private boolean isFailed(SparkAppHandle.State state) {
        if (SparkAppHandle.State.FAILED == state || SparkAppHandle.State.KILLED == state
                || SparkAppHandle.State.LOST == state) {
            return true;
        }
        return false;
    }

    private boolean isFinished(SparkAppHandle.State state) {
        if (SparkAppHandle.State.FINISHED == state) {
            return true;
        }
        return false;
    }

    @Override
    public void infoChanged(SparkAppHandle handler) {

    }
}