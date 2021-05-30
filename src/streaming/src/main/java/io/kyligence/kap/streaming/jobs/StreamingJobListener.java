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
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.event.StreamingJobDropEvent;
import io.kyligence.kap.streaming.event.StreamingJobKillEvent;
import io.kyligence.kap.streaming.jobs.scheduler.StreamingScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Locale;

public class StreamingJobListener implements SparkAppHandle.Listener {
    private static final Logger logger = LoggerFactory.getLogger(StreamingJobListener.class);

    private String project;
    private String jobId;
    private String runnable;

    public StreamingJobListener() {

    }

    public StreamingJobListener(String project, String jobId) {
        this.project = project;
        this.jobId = jobId;
    }

    @Override
    public void stateChanged(SparkAppHandle handler) {
        if (handler.getState().isFinal()) {
            runnable = null;
            if(isFailed(handler.getState())) {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                val mgr = StreamingJobManager.getInstance(config, project);
                val jobMeta = mgr.getStreamingJobByUuid(jobId);
                if (!skipListener(jobMeta)) {
                    logger.warn("The streaming job {} has terminated unexpectedly…", jobId);
                    MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.ERROR);
                    HDFSUtils.touchzMarkFile(config.getStreamingBaseJobsLocation()
                            + String.format(Locale.ROOT, StreamingConstants.JOB_SHUTDOWN_FILE_PATH, project, jobId));
                    handler.kill();
                }
            } else if(isFinished(handler.getState())) {
                MetaInfoUpdater.updateJobState(project, jobId, Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED),
                        JobStatusEnum.STOPPED);
            }
        } else if (runnable == null && SparkAppHandle.State.RUNNING == handler.getState()) {
            runnable = "true";
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.RUNNING);
        }
    }

    private boolean skipListener(StreamingJobMeta jobMeta) {
        SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        String lastUpdateTime = jobMeta.getLastUpdateTime();
        try {
            if (jobMeta.isSkipListener() && lastUpdateTime != null) {
                val lastDateTime = simpleFormat.parse(lastUpdateTime);
                val diff = (System.currentTimeMillis() - lastDateTime.getTime()) / (60 * 1000);
                if (diff <= 2) {
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;
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

    @Subscribe
    public void onStreamingJobKill(StreamingJobKillEvent streamingJobKillEvent) {
        val project = streamingJobKillEvent.getProject();
        val modelId = streamingJobKillEvent.getModelId();
        StreamingScheduler scheduler = StreamingScheduler.getInstance(project);
        scheduler.killJob(modelId, JobTypeEnum.STREAMING_MERGE, JobStatusEnum.STOPPED);
        scheduler.killJob(modelId, JobTypeEnum.STREAMING_BUILD, JobStatusEnum.STOPPED);
    }

    @Subscribe
    public void onStreamingJobDrop(StreamingJobDropEvent streamingJobDropEvent) {
        val project = streamingJobDropEvent.getProject();
        val modelId = streamingJobDropEvent.getModelId();
        val config = KylinConfig.getInstanceFromEnv();
        val mgr = StreamingJobManager.getInstance(config, project);
        val buildJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.toString());
        val mergeJobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.toString());
        mgr.deleteStreamingJob(buildJobId);
        mgr.deleteStreamingJob(mergeJobId);
    }
}