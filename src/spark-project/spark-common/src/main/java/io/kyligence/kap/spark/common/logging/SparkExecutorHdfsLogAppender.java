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
package io.kyligence.kap.spark.common.logging;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class SparkExecutorHdfsLogAppender extends AbstractHdfsLogAppender {

    private static final long A_DAY_MILLIS = 24 * 60 * 60 * 1000L;
    private static final long A_HOUR_MILLIS = 60 * 60 * 1000L;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat hourFormat = new SimpleDateFormat("HH");

    @VisibleForTesting
    String outPutPath;
    @VisibleForTesting
    String executorId;

    @VisibleForTesting
    long startTime = 0;
    @VisibleForTesting
    boolean rollingByHour = false;
    @VisibleForTesting
    int rollingPeriod = 5;

    //log appender configurable
    @Getter
    @Setter
    private String metadataIdentifier;
    @Getter
    @Setter
    private String category;
    @Getter
    @Setter
    private String identifier;

    // only cubing job
    @Getter
    @Setter
    private String jobName;
    @Getter
    @Setter
    private String project;

    @Override
    void init() {
        if (StringUtils.isBlank(this.identifier)) {
            this.identifier = YarnSparkHadoopUtil.getContainerId().getApplicationAttemptId().getApplicationId()
                    .toString();
        }

        LogLog.warn("metadataIdentifier -> " + getMetadataIdentifier());
        LogLog.warn("category -> " + getCategory());
        LogLog.warn("identifier -> " + getIdentifier());

        if (null != getProject()) {
            LogLog.warn("project -> " + getProject());
        }

        if (null != getJobName()) {
            LogLog.warn("jobName -> " + getJobName());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkExecutorHdfsLogAppender";
    }

    @Override
    boolean isSkipCheckAndFlushLog() {
        if (SparkEnv.get() == null && StringUtils.isBlank(executorId)) {
            LogLog.warn("Waiting for spark executor to start");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LogLog.error("Waiting for spark executor starting is interrupted!", e);
                Thread.currentThread().interrupt();
            }
            return true;
        }
        return false;
    }

    @Override
    void doWriteLog(int size, List<LoggingEvent> transaction) throws IOException, InterruptedException {
        while (size > 0) {
            final LoggingEvent loggingEvent = getLogBufferQue().take();
            if (isTimeChanged(loggingEvent)) {
                updateOutPutDir(loggingEvent);

                final Path file = new Path(outPutPath);

                String sparkuser = System.getenv("SPARK_USER");
                String user = System.getenv("USER");
                LogLog.warn("login user is " + UserGroupInformation.getLoginUser() + " SPARK_USER is " + sparkuser
                        + " USER is " + user);
                UserGroupInformation ugi = SparkEnv.getUGI();
                // Add tokens to new user so that it may execute its task correctly.
                LogLog.warn("Login user hashcode is " + ugi.hashCode());
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    if (!initHdfsWriter(file, new Configuration())) {
                        LogLog.error("Failed to init the hdfs writer!");
                    }
                    doRollingClean(loggingEvent);
                    return null;
                });
            }

            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            size--;
        }
    }

    @VisibleForTesting
    void updateOutPutDir(LoggingEvent event) {
        if (rollingByHour) {
            String rollingDir = dateFormat.format(new Date(event.getTimeStamp())) + "/"
                    + hourFormat.format(new Date(event.getTimeStamp()));
            outPutPath = getOutPutDir(rollingDir);
        } else {
            String rollingDir = dateFormat.format(new Date(event.getTimeStamp()));
            outPutPath = getOutPutDir(rollingDir);
        }
    }

    private String getOutPutDir(String rollingDir) {
        if (StringUtils.isBlank(executorId)) {
            executorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : UUID.randomUUID().toString();
            LogLog.warn("executorId set to " + executorId);
        }

        if ("job".equals(getCategory())) {
            return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + getJobName() + "/" + "executor-"
                    + executorId + ".log";
        }
        return getRootPathName() + "/" + rollingDir + "/" + getIdentifier() + "/" + "executor-" + executorId + ".log";
    }

    @VisibleForTesting
    void doRollingClean(LoggingEvent event) throws IOException {
        FileSystem fileSystem = getFileSystem();

        String rootPathName = getRootPathName();
        Path rootPath = new Path(rootPathName);

        if (!fileSystem.exists(rootPath))
            return;

        FileStatus[] logFolders = fileSystem.listStatus(rootPath);

        if (logFolders == null)
            return;

        String thresholdDay = dateFormat.format(new Date(event.getTimeStamp() - A_DAY_MILLIS * rollingPeriod));

        for (FileStatus fs : logFolders) {
            String fileName = fs.getPath().getName();
            if (fileName.compareTo(thresholdDay) < 0) {
                Path fullPath = new Path(rootPathName + File.separator + fileName);
                if (!fileSystem.exists(fullPath))
                    continue;
                fileSystem.delete(fullPath, true);
            }
        }
    }

    @VisibleForTesting
    String getRootPathName() {
        if ("job".equals(getCategory())) {
            return parseHdfsWordingDir() + "/" + getProject() + "/spark_logs";
        } else if ("sparder".equals(getCategory())) {
            return parseHdfsWordingDir() + "/_sparder_logs";
        } else {
            throw new IllegalArgumentException("illegal category: " + getCategory());
        }
    }

    @VisibleForTesting
    boolean isTimeChanged(LoggingEvent event) {
        if (rollingByHour) {
            return isNeedRolling(event, A_HOUR_MILLIS);
        } else {
            return isNeedRolling(event, A_DAY_MILLIS);
        }
    }

    private boolean isNeedRolling(LoggingEvent event, Long timeInterval) {
        if (0 == startTime || ((event.getTimeStamp() / timeInterval) - (startTime / timeInterval)) > 0) {
            startTime = event.getTimeStamp();
            return true;
        }
        return false;
    }

    private String parseHdfsWordingDir() {
        return StringUtils.appendIfMissing(getHdfsWorkingDir(), "/")
                + StringUtils.replace(getMetadataIdentifier(), "/", "-");
    }
}
