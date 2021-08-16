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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.NExecutableManager;
import org.joda.time.DateTime;

import io.kyligence.kap.common.util.OptionBuilder;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingSparkLogTool extends ExecutableApplication {

    private static final Option OPTION_STREAMING_DIR = OptionBuilder.getInstance().hasArg()
            .withArgName("DESTINATION_DIR").withDescription("Specify the file to save yarn application id")
            .isRequired(true).create("dir");

    private static final Option OPTION_STREAMING_JOB = OptionBuilder.getInstance().hasArg().withArgName("JOB_ID")
            .withDescription("Specify the job").isRequired(false).create("job");

    private static final Option OPTION_STREAMING_PROJECT = OptionBuilder.getInstance().hasArg()
            .withArgName("OPTION_PROJECT").withDescription("Specify project").isRequired(false).create("project");

    private static final Option OPTION_STREAMING_START_TIME = OptionBuilder.getInstance().withArgName("startTime")
            .hasArg().isRequired(false).withDescription("specify the start of time range to extract logs. ")
            .create("startTime");

    private static final Option OPTION_STREAMING_END_TIME = OptionBuilder.getInstance().withArgName("endTime").hasArg()
            .isRequired(false).withDescription("specify the end of time range to extract logs. ").create("endTime");

    private static final long DAY = 24 * 3600 * 1000L;
    private static final long MAX_DAY = 30L;
    private static final String STREAMING_LOG_ROOT_DIR = "streaming_spark_logs";
    private static final String STREAMING_SPARK_DRIVER_DIR = "spark_driver";
    private static final String STREAMING_SPARK_EXECUTOR_DIR = "spark_executor";
    private static final String STREAMING_SPARK_CHECKPOINT_DIR = "spark_checkpoint";

    private final Options options;

    private final KylinConfig kylinConfig;

    StreamingSparkLogTool() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public StreamingSparkLogTool(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.options = new Options();
        initOptions();
    }

    private void initOptions() {
        options.addOption(OPTION_STREAMING_JOB);
        options.addOption(OPTION_STREAMING_PROJECT);
        options.addOption(OPTION_STREAMING_DIR);

        options.addOption(OPTION_STREAMING_END_TIME);
        options.addOption(OPTION_STREAMING_START_TIME);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        final String dir = optionsHelper.getOptionValue(OPTION_STREAMING_DIR);
        String jobId = optionsHelper.getOptionValue(OPTION_STREAMING_JOB);
        String project = optionsHelper.getOptionValue(OPTION_STREAMING_PROJECT);

        String startTimeStr = optionsHelper.getOptionValue(OPTION_STREAMING_START_TIME);
        String endTimeStr = optionsHelper.getOptionValue(OPTION_STREAMING_END_TIME);

        // streaming job
        if (StringUtils.isNotEmpty(project) && StringUtils.isNotEmpty(jobId)) {
            log.info("start dump streaming spark driver/executor/checkpoint job log, project: {}, jobId: {}", project,
                    jobId);
            dumpJobDriverLog(project, jobId, dir, null, null);
            dumpExecutorLog(project, jobId, dir, null, null);
            dumpCheckPoint(project, StringUtils.split(jobId, "_")[0], dir);
            return;
        }

        // full
        if (StringUtils.isNotEmpty(startTimeStr) && StringUtils.isNotEmpty(endTimeStr)) {
            Long startTime = Long.parseLong(startTimeStr);
            Long endTime = Long.parseLong(endTimeStr);

            long days = (endTime - startTime) / DAY;
            if (days > MAX_DAY) {
                log.error("time range is too large, startTime: {}, endTime: {}, days: {}", startTime, endTime, days);
                return;
            }

            log.info("start dump streaming spark driver/executor/checkpoint full log, startTime: {}, endTime: {}",
                    startTimeStr, endTimeStr);
            dumpAllDriverLog(dir, startTimeStr, endTimeStr);
            dumpAllExecutorLog(dir, startTimeStr, endTimeStr);
            dumpAllCheckPoint(dir);
        }

    }

    /**
     * dump spark driver logs for a single job
     * single job -> latest log
     * full       -> all log with time filter
     */
    private void dumpJobDriverLog(String project, String jobId, String exportDir, String startTime, String endTime) {
        // streaming job driver log in hdfs directory
        String outputStoreDirPath = kylinConfig.getStreamingJobTmpOutputStorePath(project, jobId);
        NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(outputStoreDirPath)) {
            log.warn("The job driver log file on HDFS has not been generated yet, jobId: {}, filePath: {}", jobId,
                    outputStoreDirPath);
            return;
        }
        List<String> logFilePathList = executableManager.getFilePathsFromHDFSDir(outputStoreDirPath, false);
        if (CollectionUtils.isEmpty(logFilePathList)) {
            log.warn("There is no file in the current job HDFS directory: {}", outputStoreDirPath);
            return;
        }

        File driverDir = new File(exportDir, String.format(Locale.ROOT, "/%s/%s/%s/%s", STREAMING_LOG_ROOT_DIR,
                STREAMING_SPARK_DRIVER_DIR, project, jobId));
        List<Path> needCopyPathList = new ArrayList<>();
        boolean isJob = StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime);

        // copy hdfs file to local
        logFilePathList.stream().filter(filePath -> {
            if (isJob) {
                return true;
            }
            // Time Filter
            Long logTimeStamp = Long.parseLong(StringUtils.split(new Path(filePath).getName(), "\\.")[1]);
            return logTimeStamp.compareTo(Long.parseLong(startTime)) >= 0
                    && logTimeStamp.compareTo(Long.parseLong(endTime)) <= 0;
        }).map(Path::new).forEach(needCopyPathList::add);
        Collections.sort(needCopyPathList);

        try {
            FileUtils.forceMkdir(driverDir);
            if (isJob) {
                // log get latest
                Path path = needCopyPathList.get(needCopyPathList.size() - 1);
                fs.copyToLocalFile(path, new Path(driverDir.getAbsolutePath()));
                return;
            }
            // full
            for (Path path : needCopyPathList) {
                fs.copyToLocalFile(path, new Path(driverDir.getAbsolutePath()));
            }
        } catch (IOException e) {
            log.error("dump streaming driver log failed. ", e);
        }
    }

    /**
     * dump spark driver All logs
     */
    private void dumpAllDriverLog(String exportDir, String startTime, String endTime) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);

        projectManager.listAllProjects().forEach(projectInstance -> {
            String project = projectInstance.getName();
            StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(kylinConfig, project);
            streamingJobManager.listAllStreamingJobMeta().stream().map(StreamingJobMeta::getId).forEach(jobId -> {
                dumpJobDriverLog(project, jobId, exportDir, startTime, endTime);
            });
        });
    }

    /**
     * dump spark All checkpoint
     */
    private void dumpAllCheckPoint(String exportDir) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);

        projectManager.listAllProjects().forEach(projectInstance -> {
            String project = projectInstance.getName();
            StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(kylinConfig, project);
            streamingJobManager.listAllStreamingJobMeta().stream().map(StreamingJobMeta::getModelId).forEach(uuid -> {
                dumpCheckPoint(project, uuid, exportDir);
            });
        });
    }

    /**
     * dump spark checkpoint for a single job
     */
    private void dumpCheckPoint(String project, String modelId, String exportDir) {
        String hdfsStreamLogRootPath = kylinConfig.getHdfsWorkingDirectoryWithoutScheme();
        String hdfsStreamJobCheckPointPath = String.format(Locale.ROOT, "%s%s%s", hdfsStreamLogRootPath,
                "streaming/checkpoint/", modelId);
        NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(hdfsStreamJobCheckPointPath)) {
            log.warn("The job checkpoint file on HDFS has not been generated yet, jobId: {}, filePath: {}", modelId,
                    hdfsStreamJobCheckPointPath);
            return;
        }
        List<String> executorLogPath = executableManager.getFilePathsFromHDFSDir(hdfsStreamJobCheckPointPath, true);
        if (CollectionUtils.isEmpty(executorLogPath)) {
            log.warn("There is no file in the current job HDFS directory: {}", hdfsStreamJobCheckPointPath);
            return;
        }

        File checkpointDir = new File(exportDir,
                String.format(Locale.ROOT, "/%s/%s", STREAMING_LOG_ROOT_DIR, STREAMING_SPARK_CHECKPOINT_DIR));
        try {
            FileUtils.forceMkdir(checkpointDir);
            fs.copyToLocalFile(new Path(hdfsStreamJobCheckPointPath), new Path(checkpointDir.getAbsolutePath()));
        } catch (IOException e) {
            log.error("dump streaming checkpoint failed. ", e);
        }
    }

    /**
     * dump spark executor All logs
     */
    private void dumpAllExecutorLog(String exportDir, String startTime, String endTime) {
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);

        projectManager.listAllProjects().forEach(projectInstance -> {
            String project = projectInstance.getName();
            dumpExecutorLog(project, null, exportDir, startTime, endTime);
        });

    }

    /**
     * dump spark executor log for a single job
     */
    private void dumpExecutorLog(String project, String jobId, String exportDir, String startTime, String endTime) {
        String hdfsStreamLogRootPath = kylinConfig.getHdfsWorkingDirectoryWithoutScheme();
        String hdfsStreamLogProjectPath = String.format(Locale.ROOT, "%s%s%s", hdfsStreamLogRootPath,
                "streaming/spark_logs/", project);
        NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();

        if (!executableManager.isHdfsPathExists(hdfsStreamLogProjectPath)) {
            log.warn("The job executor log file on HDFS has not been generated yet, jobId: {}, filePath: {}", jobId,
                    hdfsStreamLogProjectPath);
            return;
        }
        List<String> executorLogPath = executableManager.getFilePathsFromHDFSDir(hdfsStreamLogProjectPath, true);
        if (CollectionUtils.isEmpty(executorLogPath)) {
            log.warn("There is no file in the current job HDFS directory: {}", hdfsStreamLogProjectPath);
            return;
        }

        executorLogPath.stream().filter(
                path -> StringUtils.isEmpty(jobId) || StringUtils.equals(new Path(path).getParent().getName(), jobId))
                .filter(path -> {
                    if (StringUtils.isEmpty(startTime) && StringUtils.isEmpty(endTime)) {
                        return true;
                    }

                    // Time Filter
                    String rollingDate = new Path(path).getParent().getParent().getName();
                    DateTime rollingDateTime = new DateTime(rollingDate);
                    DateTime startDateTime = new DateTime(Long.parseLong(startTime)).withTimeAtStartOfDay();
                    DateTime endDateTime = new DateTime(Long.parseLong(endTime));

                    return rollingDateTime.compareTo(startDateTime) >= 0 && rollingDateTime.compareTo(endDateTime) <= 0;
                }).map(Path::new).forEach(logPath -> {
                    String logJobId = logPath.getParent().getName();
                    String rollingDate = logPath.getParent().getParent().getName();
                    File executorDir = new File(exportDir, String.format(Locale.ROOT, "/%s/%s/%s/%s/%s",
                            STREAMING_LOG_ROOT_DIR, STREAMING_SPARK_EXECUTOR_DIR, project, logJobId, rollingDate));
                    try {
                        FileUtils.forceMkdir(executorDir);
                        fs.copyToLocalFile(logPath, new Path(executorDir.getAbsolutePath()));
                    } catch (IOException e) {
                        log.error("dump streaming executor log failed. ", e);
                    }
                });
    }

}
