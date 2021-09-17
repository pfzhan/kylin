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

import static io.kyligence.kap.streaming.constants.StreamingConstants.REST_SERVER_IP;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_CORES_MAX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_DRIVER_MEM;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_DRIVER_MEM_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_DRIVER_OPTS;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_CORES;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_CORES_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_INSTANCES;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_INSTANCES_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_MEM;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_MEM_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_EXECUTOR_OPTS;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_MASTER;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_MASTER_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_SHUFFLE_PARTITIONS;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_SHUFFLE_PARTITIONS_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_STREAMING_ENTRY;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_STREAMING_MERGE_ENTRY;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_YARN_DIST_JARS;
import static io.kyligence.kap.streaming.constants.StreamingConstants.SPARK_YARN_TIMELINE_SERVICE;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_DURATION;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_DURATION_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_WATERMARK;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_WATERMARK_DEFAULT;

import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.jobs.AbstractSparkJobLauncher;
import io.kyligence.kap.streaming.jobs.StreamingJobUtils;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobLauncher extends AbstractSparkJobLauncher {

    private Map<String, String> jobParams;
    private String mainClazz;
    private String[] appArgs;
    private Long currentTimestamp;

    // TODO: support yarn cluster
    private boolean isYarnCluster = false;
    @Getter
    private boolean initialized = false;

    @Override
    public void init(String project, String modelId, JobTypeEnum jobType) {
        super.init(project, modelId, jobType);
        jobParams = strmJob.getParams();
        currentTimestamp = System.currentTimeMillis();

        //reload configuration from job params
        this.config = StreamingJobUtils.getStreamingKylinConfig(this.config, jobParams, modelId, project);

        switch (jobType) {
            case STREAMING_BUILD: {
                this.mainClazz = SPARK_STREAMING_ENTRY;
                this.appArgs = new String[]{project, modelId,
                        jobParams.getOrDefault(STREAMING_DURATION,
                                STREAMING_DURATION_DEFAULT),
                        jobParams.getOrDefault(STREAMING_WATERMARK,
                                STREAMING_WATERMARK_DEFAULT) };
                break;
            }
            case STREAMING_MERGE: {
                this.mainClazz = SPARK_STREAMING_MERGE_ENTRY;
                this.appArgs = new String[]{project, modelId,
                        jobParams.getOrDefault(STREAMING_SEGMENT_MAX_SIZE,
                                STREAMING_SEGMENT_MAX_SIZE_DEFAULT),
                        jobParams.getOrDefault(STREAMING_SEGMENT_MERGE_THRESHOLD,
                                STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT)};
                break;
            }
            default:
                throw new IllegalArgumentException("The streaming job Type " + jobType.name() + " is not supported...");

        }
        initialized = true;
    }

    private String getDriverHDFSLogPath() {
        return String.format(Locale.ROOT, "%s/%s/%s/%s/driver.%s.log", config.getStreamingBaseJobsLocation(), project, jobId,
                currentTimestamp, currentTimestamp);
    }

    private String wrapDriverJavaOptions(Map<String, String> sparkConf) {
        val driverJavaOptsConfigStr = sparkConf.get(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);

        Preconditions.checkNotNull(driverJavaOptsConfigStr, SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS + " is empty");

        StringBuilder driverJavaOptionsSB = new StringBuilder(driverJavaOptsConfigStr);
        driverJavaOptionsSB.append(javaPropertyFormatter(REST_SERVER_IP, AddressUtil.getLocalHostExactAddress()));
        driverJavaOptionsSB.append(javaPropertyFormatter("kylin.hdfs.working.dir", config.getHdfsWorkingDirectory()));
        driverJavaOptionsSB.append(javaPropertyFormatter("spark.driver.log4j.appender.hdfs.File", getDriverHDFSLogPath()));
        driverJavaOptionsSB.append(javaPropertyFormatter("user.timezone", config.getTimeZone()));

        final String driverLog4jXmlFile = config.getLogSparkStreamingDriverPropertiesFile();
        generateLog4jConfiguration(false, driverJavaOptionsSB, driverLog4jXmlFile);

        return driverJavaOptionsSB.toString();
    }

    private void generateLog4jConfiguration(boolean isExecutor, StringBuilder log4jJavaOptionsSB, String log4jXmlFile) {
        String log4jConfigStr = "file:" + log4jXmlFile;

        if (isExecutor || isYarnCluster || config.getSparkMaster().startsWith("k8s")) {
            // Direct file name.
            log4jConfigStr = Paths.get(log4jXmlFile).getFileName().toString();
        }

        log4jJavaOptionsSB.append(javaPropertyFormatter("log4j.configurationFile", log4jConfigStr));
    }

    private String wrapExecutorJavaOptions(Map<String, String> sparkConf) {
        val executorJavaOptsConfigStr = sparkConf.get(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS);

        Preconditions.checkNotNull(executorJavaOptsConfigStr, SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS + " is empty");

        StringBuilder executorJavaOptionsSB = new StringBuilder(executorJavaOptsConfigStr);

        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.identifier", jobId));
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.jobTimeStamp", currentTimestamp.toString()));
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.project", project));
        executorJavaOptionsSB.append(javaPropertyFormatter("user.timezone", config.getTimeZone()));
        if (StringUtils.isNotBlank(config.getMountSparkLogDir())) {
            executorJavaOptionsSB.append(javaPropertyFormatter("job.mountDir", config.getMountSparkLogDir()));
        }

        final String executorLog4jXmlFile = config.getLogSparkStreamingExecutorPropertiesFile();
        generateLog4jConfiguration(true, executorJavaOptionsSB, executorLog4jXmlFile);

        return executorJavaOptionsSB.toString();
    }

    public void startYarnJob() throws Exception {
        Map<String, String> sparkConf = getStreamingSparkConfig(config);
        sparkConf.forEach((key, value) -> launcher.setConf(key, value));

        val numberOfExecutor = sparkConf.getOrDefault(SPARK_EXECUTOR_INSTANCES, SPARK_EXECUTOR_INSTANCES_DEFAULT);
        val numberOfCore = sparkConf.getOrDefault(SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_CORES_DEFAULT);
        handler = launcher.setAppName(jobId).setSparkHome(KylinConfig.getSparkHome())
                .setMaster(sparkConf.getOrDefault(SPARK_MASTER, SPARK_MASTER_DEFAULT))
                .setConf(SPARK_DRIVER_MEM, sparkConf.getOrDefault(SPARK_DRIVER_MEM, SPARK_DRIVER_MEM_DEFAULT))
                .setConf(SPARK_EXECUTOR_INSTANCES, numberOfExecutor).setConf(SPARK_EXECUTOR_CORES, numberOfCore)
                .setConf(SPARK_CORES_MAX, calcMaxCores(numberOfExecutor, numberOfCore))
                .setConf(SPARK_EXECUTOR_MEM, sparkConf.getOrDefault(SPARK_EXECUTOR_MEM, SPARK_EXECUTOR_MEM_DEFAULT))
                .setConf(SPARK_SHUFFLE_PARTITIONS,
                        sparkConf.getOrDefault(SPARK_SHUFFLE_PARTITIONS, SPARK_SHUFFLE_PARTITIONS_DEFAULT))
                .setConf(SPARK_YARN_DIST_JARS, kylinJobJar).setConf(SPARK_YARN_TIMELINE_SERVICE, "false")
                .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, kylinJobJar)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, Paths.get(kylinJobJar).getFileName().toString())
                .setConf(SPARK_DRIVER_OPTS, wrapDriverJavaOptions(sparkConf))
                .setConf(SPARK_EXECUTOR_OPTS, wrapExecutorJavaOptions(sparkConf))
                .addFile(config.getLogSparkStreamingExecutorPropertiesFile()).setAppResource(kylinJobJar)
                .setMainClass(mainClazz).addAppArgs(appArgs).startApplication(listener);
    }

    @Override
    public void launch() {
        try {
            if (config.isUTEnv()) {
                log.info("{} -- {} {} streaming job starts to launch", project, modelId, jobType.name());
            } else if (StreamingUtils.isLocalMode()) {
                if (JobTypeEnum.STREAMING_BUILD == jobType) {
                    log.info("Starting streaming build job in local mode...");
                    StreamingEntry.main(appArgs);
                } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                    log.info("Starting streaming merge job in local mode...");
                    StreamingMergeEntry.main(appArgs);
                }
            } else {
                startYarnJob();
            }
        } catch (Exception e) {
            log.error("launch streaming application failed: " + e.getMessage(), e);
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.LAUNCHING_ERROR);
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, e.getMessage());
        }
        log.info("Streaming job create success on model {}", modelId);
    }

    /**
     * Stop streaming job gracefully
     */
    @Override
    public void stop() {
        MetaInfoUpdater.markGracefulShutdown(project, StreamingUtils.getJobId(modelId, jobType.name()));
    }

    private String calcMaxCores(String executors, String cores) {
        return String.valueOf(Integer.parseInt(executors) * Integer.parseInt(cores));
    }
}