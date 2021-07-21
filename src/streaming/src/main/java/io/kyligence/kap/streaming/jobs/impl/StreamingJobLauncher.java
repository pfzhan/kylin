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

import static io.kyligence.kap.streaming.constants.StreamingConstants.JOB_SHUTDOWN_FILE_PATH;
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
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_CONFIG_PREFIX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_DURATION;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_DURATION_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_MAX_RATE_PER_PARTITION;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MAX_SIZE_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_SEGMENT_MERGE_THRESHOLD_DEFAULT;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_WATERMARK;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_WATERMARK_DEFAULT;

import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.engine.spark.utils.HDFSUtils;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.streaming.app.StreamingEntry;
import io.kyligence.kap.streaming.app.StreamingMergeEntry;
import io.kyligence.kap.streaming.jobs.AbstractSparkJobLauncher;
import io.kyligence.kap.streaming.util.MetaInfoUpdater;
import lombok.val;
import lombok.var;

public class StreamingJobLauncher extends AbstractSparkJobLauncher {

    private static final Logger logger = LoggerFactory.getLogger(StreamingJobLauncher.class);
    private Map<String, String> jobParams;
    private String mainClazz;
    private String[] appArgs;

    // TODO: support yarn cluster
    private boolean isYarnCluster = false;

    @Override
    public void init(String project, String modelId, JobTypeEnum jobType) {
        super.init(project, modelId, jobType);
        jobParams = strmJob.getParams();

        //reload configuration from job params
        this.config = getJobKylinConfig(this.config);

        switch (jobType) {
            case STREAMING_BUILD: {
                this.mainClazz = SPARK_STREAMING_ENTRY;
                var maxRatePerPartition = jobParams.getOrDefault(STREAMING_MAX_RATE_PER_PARTITION,
                        config.getKafkaRatePerPartition());
                this.appArgs = new String[]{project, modelId,
                        jobParams.getOrDefault(STREAMING_DURATION,
                                STREAMING_DURATION_DEFAULT),
                        jobParams.getOrDefault(STREAMING_WATERMARK,
                                STREAMING_WATERMARK_DEFAULT),
                        maxRatePerPartition};
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
    }

    /**
     * kylin.properties config -> model config -> job config
     *
     * @return
     */
    private KylinConfig getJobKylinConfig(final KylinConfig originalConfig) {
        KylinConfigExt kylinConfigExt;
        val dataflowId = modelId;
        if (StringUtils.isNotBlank(dataflowId)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflowId).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        Map<String, String> streamingJobOverrides = Maps.newHashMap();

        if (MapUtils.isNotEmpty(kylinConfigExt.getExtendedOverrides())) {
            streamingJobOverrides.putAll(kylinConfigExt.getExtendedOverrides());
        }

        //load kylin.streaming.spark-conf.*
        jobParams.entrySet().stream().filter(entry ->
                entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)
        ).forEach(entry -> {
            streamingJobOverrides.put(entry.getKey(), entry.getValue());
        });

        //load spark.*
        jobParams.entrySet().stream().filter(entry ->
                !entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)
        ).forEach(entry -> {
            streamingJobOverrides.put(STREAMING_CONFIG_PREFIX + entry.getKey(), entry.getValue());
        });

        return KylinConfigExt.createInstance(kylinConfigExt, streamingJobOverrides);
    }

    private String getDriverHDFSLogPath() {
        return String.format(Locale.ROOT, "%s/%s/%s/driver.%s.log", config.getStreamingBaseJobsLocation(), project, jobId,
                System.currentTimeMillis());
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
        executorJavaOptionsSB.append(javaPropertyFormatter("kap.spark.project", project));
        executorJavaOptionsSB.append(javaPropertyFormatter("user.timezone", config.getTimeZone()));
        if (StringUtils.isNotBlank(config.getMountSparkLogDir())) {
            executorJavaOptionsSB.append(javaPropertyFormatter("job.mountDir", config.getMountSparkLogDir()));
        }

        final String executorLog4jXmlFile = config.getLogSparkStreamingExecutorPropertiesFile();
        generateLog4jConfiguration(true, executorJavaOptionsSB, executorLog4jXmlFile);

        return executorJavaOptionsSB.toString();
    }

    @Override
    public void launch() {
        try {

            if (config.isUTEnv()) {
                logger.info("{} -- {} {} job begins to launch", project, modelId, jobType.name());
                return;
            }

            if (StreamingUtils.isLocalMode()) {
                if (JobTypeEnum.STREAMING_BUILD == jobType) {
                    StreamingEntry.main(appArgs);
                } else if (JobTypeEnum.STREAMING_MERGE == jobType) {
                    StreamingMergeEntry.main(appArgs);
                }
                return;
            }

            Map<String, String> sparkConf = getStreamingSparkConfig(config);
            sparkConf.forEach((key, value) -> launcher.setConf(key, value));

            val numberOfExecutor = sparkConf.getOrDefault(SPARK_EXECUTOR_INSTANCES,
                    SPARK_EXECUTOR_INSTANCES_DEFAULT);
            val numberOfCore = sparkConf.getOrDefault(SPARK_EXECUTOR_CORES,
                    SPARK_EXECUTOR_CORES_DEFAULT);
            handler = launcher.setAppName(jobId).setSparkHome(KylinConfig.getSparkHome())
                    .setMaster(sparkConf.getOrDefault(SPARK_MASTER,
                            SPARK_MASTER_DEFAULT))
                    .setConf(SPARK_DRIVER_MEM,
                            sparkConf.getOrDefault(SPARK_DRIVER_MEM,
                                    SPARK_DRIVER_MEM_DEFAULT))
                    .setConf(SPARK_EXECUTOR_INSTANCES, numberOfExecutor)
                    .setConf(SPARK_EXECUTOR_CORES, numberOfCore)
                    .setConf(SPARK_CORES_MAX, calcMaxCores(numberOfExecutor, numberOfCore))
                    .setConf(SPARK_EXECUTOR_MEM,
                            sparkConf.getOrDefault(SPARK_EXECUTOR_MEM,
                                    SPARK_EXECUTOR_MEM_DEFAULT))
                    .setConf(SPARK_SHUFFLE_PARTITIONS,
                            sparkConf.getOrDefault(SPARK_SHUFFLE_PARTITIONS,
                                    SPARK_SHUFFLE_PARTITIONS_DEFAULT))
                    .setConf(SPARK_YARN_DIST_JARS, kylinJobJar)
                    .setConf(SPARK_YARN_TIMELINE_SERVICE, "false")
                    .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, kylinJobJar)
                    .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, Paths.get(kylinJobJar).getFileName().toString())
                    .setConf(SPARK_DRIVER_OPTS, wrapDriverJavaOptions(sparkConf))
                    .setConf(SPARK_EXECUTOR_OPTS, wrapExecutorJavaOptions(sparkConf))
                    .addFile(config.getLogSparkStreamingExecutorPropertiesFile()).setAppResource(kylinJobJar)
                    .setMainClass(mainClazz).addAppArgs(appArgs).startApplication(listener);

        } catch (Exception e) {
            logger.error("launch streaming application failed: " + e.getMessage(), e);
            MetaInfoUpdater.updateJobState(project, jobId, JobStatusEnum.LAUNCHING_ERROR);
            throw new KylinException(ServerErrorCode.JOB_START_FAILURE, e.getMessage());
        }
        logger.info("Streaming job create success on model {}", modelId);
    }


    @Override
    public void stop() {
        String model = NDataModelManager.getInstance(config, project)
                .getDataModelDesc(modelId).getAlias();

        switch (jobType) {
            case STREAMING_BUILD: {
                val buildMarkFile = config.getStreamingBaseJobsLocation()
                        + String.format(Locale.ROOT, JOB_SHUTDOWN_FILE_PATH, project,
                        StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name()));
                if (!HDFSUtils.touchzMarkFile(buildMarkFile)) {
                    throw new KylinException(ServerErrorCode.JOB_STOP_FAILURE,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getJOB_STOP_FAILURE(), model));
                }
                break;
            }
            case STREAMING_MERGE: {
                val mergeMarkFile = config.getStreamingBaseJobsLocation()
                        + String.format(Locale.ROOT, JOB_SHUTDOWN_FILE_PATH, project,
                        StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_MERGE.name()));
                if (!HDFSUtils.touchzMarkFile(mergeMarkFile)) {
                    throw new KylinException(ServerErrorCode.JOB_STOP_FAILURE,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getJOB_STOP_FAILURE(), model));
                }
                break;
            }
            default:
                throw new IllegalArgumentException("The streaming job Type " + jobType.name() + " is not supported...");
        }

    }

    private String calcMaxCores(String executors, String cores) {
        return String.valueOf(Integer.parseInt(executors) * Integer.parseInt(cores));
    }
}