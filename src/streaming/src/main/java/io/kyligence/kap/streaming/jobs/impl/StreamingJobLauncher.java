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

package io.kyligence.kap.streaming.jobs.impl;

import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.AbstractSparkJobLauncher;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;
import lombok.var;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NetworkUtils;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

public class StreamingJobLauncher extends AbstractSparkJobLauncher {

    private static final Logger logger = LoggerFactory.getLogger(StreamingJobLauncher.class);
    private Map<String, String> jobParams;
    private String mainClazz;
    private String[] appArgs;

    @Override
    public void init(String project, String modelId, JobTypeEnum jobType) {
        super.init(project, modelId, jobType);
        jobParams = strmJob.getParams();
        if (JobTypeEnum.STREAMING_BUILD == jobType) {
            this.mainClazz = StreamingConstants.SPARK_STREAMING_ENTRY;
            var maxRatePerPartition = jobParams.getOrDefault(StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION,
                    KylinConfig.getInstanceFromEnv().getKafkaRatePerPartition());
            this.appArgs = new String[] { project, modelId,
                    jobParams.getOrDefault(StreamingConstants.STREAMING_DURATION,
                            StreamingConstants.STREAMING_DURATION_DEFAULT),
                    jobParams.getOrDefault(StreamingConstants.STREAMING_WATERMARK,
                            StreamingConstants.STREAMING_WATERMARK_DEFAULT),
                    maxRatePerPartition };
        } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
            this.mainClazz = StreamingConstants.SPARK_STREAMING_MERGE_ENTRY;
            this.appArgs = new String[] { project, modelId,
                    jobParams.getOrDefault(StreamingConstants.STREAMING_SEGMENT_MAX_SIZE,
                            StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT),
                    jobParams.getOrDefault(StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD,
                            StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT) };
        } else {
            throw new IllegalArgumentException("The streaming job Type " + jobType.name() + " is not supported...");
        }
    }

    @Override
    public void launch() {
        try {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (config.isUTEnv()) {
                if (JobTypeEnum.STREAMING_BUILD == jobType) {
                    logger.info(project + "--" + modelId + " build job begins to launch");
                } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                    logger.info(project + "--" + modelId + " merge job begins to launch");
                }
            } else if (StreamingUtils.isLocalMode()) {
                if (JobTypeEnum.STREAMING_BUILD == jobType) {
                    StreamingEntry.main(appArgs);
                } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                    StreamingMergeEntry.main(appArgs);
                }
            } else {
                String driverExtraConf = "-Dfile.encoding=UTF-8 -Dhdp.version=current -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${KYLIN_HOME}/logs -D"
                        + StreamingConstants.REST_SERVER_IP + "=" + NetworkUtils.getLocalIp();
                String executorExtraConf = "-Dfile.encoding=UTF-8 -Dhdp.version=current -Dlog4j.configuration=spark-executor-log4j.properties -Dlog4j.debug -Dkylin.hdfs.working.dir=${kylin.env.hdfs-working-dir} -Dkap.metadata.identifier=${kylin.metadata.url.identifier} -Dkap.spark.category=job -Dkap.spark.project=${job.project} -Dkap.spark.identifier=${job.id} -Dkap.spark.jobName=${job.stepId} -Duser.timezone=${user.timezone} -Dkap.spark.mountDir=${job.mountDir}";
                Map<String, String> sparkConf = config.getStreamingSparkConfigOverride();
                sparkConf.entrySet().forEach(entry -> launcher.setConf(entry.getKey(), entry.getValue()));
                val iter = jobParams.entrySet().iterator();
                val prefix = "kylin.streaming.spark-conf.";
                while (iter.hasNext()) {
                    val entry = iter.next();
                    if (entry.getKey().startsWith(prefix) && !StringUtils.isEmpty(entry.getValue())) {
                        launcher.setConf(entry.getKey().substring(prefix.length()), entry.getValue());
                    }
                }
                val numberOfExecutor = jobParams.getOrDefault(StreamingConstants.SPARK_EXECUTOR_INSTANCES,
                        StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT);
                val numberOfCore = jobParams.getOrDefault(StreamingConstants.SPARK_EXECUTOR_CORES,
                        StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT);
                handler = launcher.setAppName(jobId).setSparkHome(KylinConfig.getSparkHome())
                        .setMaster(jobParams.getOrDefault(StreamingConstants.SPARK_MASTER,
                                StreamingConstants.SPARK_MASTER_DEFAULT))
                        .setConf(StreamingConstants.SPARK_DRIVER_MEM,
                                jobParams.getOrDefault(StreamingConstants.SPARK_DRIVER_MEM,
                                        StreamingConstants.SPARK_DRIVER_MEM_DEFAULT))
                        .setConf(StreamingConstants.SPARK_EXECUTOR_INSTANCES, numberOfExecutor)
                        .setConf(StreamingConstants.SPARK_EXECUTOR_CORES, numberOfCore)
                        .setConf(StreamingConstants.SPARK_CORES_MAX, calcMaxCores(numberOfExecutor, numberOfCore))
                        .setConf(StreamingConstants.SPARK_EXECUTOR_MEM,
                                jobParams.getOrDefault(StreamingConstants.SPARK_EXECUTOR_MEM,
                                        StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT))
                        .setConf(StreamingConstants.SPARK_SHUFFLE_PARTITIONS,
                                jobParams.getOrDefault(StreamingConstants.SPARK_SHUFFLE_PARTITIONS,
                                        StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT))
                        .setConf(StreamingConstants.SPARK_YARN_DIST_JARS, kylinJobJar)
                        .setConf(StreamingConstants.SPARK_YARN_TIMELINE_SERVICE, "false")
                        .setConf(StreamingConstants.SPARK_DRIVER_OPTS, driverExtraConf)
                        .setConf(StreamingConstants.SPARK_EXECUTOR_OPTS, executorExtraConf).setAppResource(kylinJobJar)
                        .setMainClass(mainClazz).addAppArgs(appArgs).startApplication(listener);
            }
        } catch (Exception e) {
            logger.error("launch streaming application failed: " + e.getMessage(), e);
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.ERROR);
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, e.getMessage());
        }
        logger.info("Streaming job create success on model {}", modelId);
    }

    @Override
    public void stop() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataModelDesc(modelId).getAlias();

        if (JobTypeEnum.STREAMING_BUILD == jobType) {
            val buildMarkFile = config.getStreamingBaseJobsLocation()
                    + String.format(Locale.ROOT, StreamingConstants.JOB_SHUTDOWN_FILE_PATH, project,
                            StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()));
            if (!HDFSUtils.touchzMarkFile(buildMarkFile)) {
                throw new KylinException(ServerErrorCode.JOB_STOP_FAILURE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getJOB_STOP_FAILURE(), model));
            }
        } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
            val mergeMarkFile = config.getStreamingBaseJobsLocation()
                    + String.format(Locale.ROOT, StreamingConstants.JOB_SHUTDOWN_FILE_PATH, project,
                            StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()));
            if (!HDFSUtils.touchzMarkFile(mergeMarkFile)) {
                throw new KylinException(ServerErrorCode.JOB_STOP_FAILURE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getJOB_STOP_FAILURE(), model));
            }
        }

    }

    private String calcMaxCores(String executors, String cores) {
        return String.valueOf(Integer.parseInt(executors) * Integer.parseInt(cores));
    }
}