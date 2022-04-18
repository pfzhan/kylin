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

import java.io.IOException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.event.StreamingJobDropEvent;
import io.kyligence.kap.streaming.event.StreamingJobKillEvent;
import io.kyligence.kap.streaming.event.StreamingJobMetaCleanEvent;
import io.kyligence.kap.streaming.jobs.scheduler.StreamingScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.util.JobKiller;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;

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
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            val jobMeta = mgr.getStreamingJobByUuid(jobId);
            if (isFailed(handler.getState()) && !jobMeta.isSkipListener()) {
                logger.warn("The streaming job {} has terminated unexpectedly…", jobId);
                handler.kill();
                JobKiller.killProcess(jobMeta);
                JobKiller.killApplication(jobId);
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED), JobStatusEnum.ERROR);
            } else if (isFinished(handler.getState())) {
                handler.stop();
                JobKiller.killProcess(jobMeta);
                JobKiller.killApplication(jobId);
                MetaInfoUpdater.updateJobState(project, jobId,
                        Sets.newHashSet(JobStatusEnum.ERROR, JobStatusEnum.STOPPED), JobStatusEnum.STOPPED);
            }
        } else if (runnable == null && SparkAppHandle.State.RUNNING == handler.getState()) {
            runnable = "true";
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

    @Subscribe
    public void onStreamingJobMetaCleanEvent(StreamingJobMetaCleanEvent streamingJobMetaCleanEvent) {
        val deletedPath = streamingJobMetaCleanEvent.getDeletedMetaPath();
        if (CollectionUtils.isEmpty(deletedPath)) {
            logger.debug("path list is empty, skip to delete.");
            return;
        }

        logger.info("begin to delete streaming meta path size:{}", deletedPath.size());
        deletedPath.forEach(path -> {
            try {
                val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
                logger.debug("delete streaming meta {} path:{}", deleteSuccess, path);
            } catch (IOException e) {
                logger.warn("delete streaming meta path:{} error", path, e);
            }
        });
    }
}