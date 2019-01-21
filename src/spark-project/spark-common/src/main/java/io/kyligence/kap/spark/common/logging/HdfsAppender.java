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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.apache.spark.deploy.yarn.YarnSparkHadoopUtil;

public class HdfsAppender extends AppenderSkeleton {

    private static long A_DAY_MILLIS = 24 * 60 * 60 * 1000L;
    private static long A_HOUR_MILLIS = 60 * 60 * 1000L;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
    FSDataOutputStream outStream = null;
    BufferedWriter bufferedWriter = null;
    FileSystem fileSystem = null;
    private String outPutPath;
    private String executorId;
    private BlockingDeque<LoggingEvent> logBufferQue = null;
    private ExecutorService appendHdfsService = null;
    private HdfsFlushService flushService;
    private long startTime = 0;

    //configurable
    private int logQueueCapacity = 5000;
    private int flushInterval = 5000;
    private boolean rollingByHour = false;
    private int rollingPeriod = 5;
    private String hdfsWorkingDir;
    private String metadataUrl;
    private String category;
    private String identifier;
    // only cubing job
    private String jobName;
    private String project;

    @Override
    public void activateOptions() {
        init();
    }

    private void init() {
        if (StringUtils.isBlank(this.identifier)) {
            this.identifier = YarnSparkHadoopUtil.getContainerId().getApplicationAttemptId().getApplicationId()
                    .toString();
        }

        LogLog.warn("HdfsAppender start ...");
        LogLog.warn("hdfsWorkingDir -> " + hdfsWorkingDir);
        LogLog.warn("metadataUrl -> " + metadataUrl);
        LogLog.warn("category -> " + category);
        LogLog.warn("identifier -> " + identifier);
        LogLog.warn("project -> " + identifier);

        logBufferQue = new LinkedBlockingDeque<>(logQueueCapacity);

        appendHdfsService = Executors.newSingleThreadExecutor();
        flushService = new HdfsFlushService();
        appendHdfsService.execute(flushService);

        Runtime.getRuntime().addShutdownHook(new Thread((this::close)));
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            logBufferQue.put(loggingEvent);
        } catch (InterruptedException e) {
            LogLog.warn("Append logging event interrupted!", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        LogLog.warn("Close HdfsAppender ...");
        try {
            flushService.flushLog(logBufferQue.size());
            closeWriter();
            if (appendHdfsService != null && !appendHdfsService.isShutdown()) {
                appendHdfsService.shutdownNow();
            }
        } catch (Exception e) {
            LogLog.error("close HdfsAppender failed", e);
        }
        this.closed = true;
    }

    private void closeWriter() {
        IOUtils.closeQuietly(bufferedWriter);
        IOUtils.closeQuietly(outStream);

    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    public String getMetadataUrl() {
        return metadataUrl;
    }

    public void setMetadataUrl(String metadataUrl) {
        this.metadataUrl = metadataUrl;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public boolean isRollingByHour() {
        return rollingByHour;
    }

    public void setRollingByHour(boolean rollingByHour) {
        this.rollingByHour = rollingByHour;
    }

    public void setHdfsWorkingDir(String hdfsWorkingDir) {
        this.hdfsWorkingDir = hdfsWorkingDir;
    }

    public String getHdfsWorkingDir() {
        return this.hdfsWorkingDir;
    }

    public void setLogQueueCapacity(int logQueueCapacity) {
        this.logQueueCapacity = logQueueCapacity;
    }

    public int getLogQueueCapacity() {
        return this.logQueueCapacity;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public int getFlushInterval() {
        return this.flushInterval;
    }

    public int getRollingPeriod() {
        return this.rollingPeriod;
    }

    public void setRollingPeriod(int rollingPeriod) {
        this.rollingPeriod = rollingPeriod;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    private class HdfsFlushService implements Runnable {

        @Override
        public void run() {
            setName("SparkExecutorLogAppender");
            long start = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (SparkEnv.get() == null && StringUtils.isBlank(executorId)) {
                        LogLog.warn("Waiting for spark executor to start");
                        Thread.sleep(1000);
                        continue;
                    }
                    //Small chunk will be flushed each 5 seconds
                    int curSize = logBufferQue.size();
                    if (curSize < logQueueCapacity * 0.2) {
                        Thread.sleep(flushInterval / 100);
                        long end = System.currentTimeMillis();
                        if (end - start > flushInterval) {
                            flushLog(curSize);
                            start = System.currentTimeMillis();
                        }
                    } else {
                        //Big chunk will be flushed immediately
                        flushLog(curSize);

                    }
                } catch (IOException eio) {
                    LogLog.error("IOException when flushing, retry after sleep 10s", eio);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        LogLog.warn("Interrupted!", e);
                        // Restore interrupted state...
                        Thread.currentThread().interrupt();
                    }
                } catch (InterruptedException e) {
                    LogLog.warn("Flush log interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LogLog.error("Unknown exception, ignore", e);
                }
            }
        }

        private void flushLog(int size) throws IOException, InterruptedException {
            if (size == 0)
                return;

            while (size > 0) {
                final LoggingEvent loggingEvent = logBufferQue.take();
                if (isTimeChanged(loggingEvent)) {
                    updateOutPutDir(loggingEvent);

                    final Path file = new Path(outPutPath);

                    // Security framework already loaded the tokens into current ugi
                    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
                    LogLog.warn("Executing with tokens:");
                    for (Token<?> token : credentials.getAllTokens()) {
                        LogLog.warn(token.toString());
                    }

                    String sparkuser = System.getenv("SPARK_USER");
                    String user = System.getenv("USER");
                    LogLog.warn("login user is " + UserGroupInformation.getLoginUser() + " SPARK_USER is " + sparkuser
                            + " USER is " + user);
                    UserGroupInformation childUGI = UserGroupInformation.createRemoteUser(user);
                    // Add tokens to new user so that it may execute its task correctly.
                    childUGI.addCredentials(credentials);

                    childUGI.doAs(new PrivilegedExceptionAction<Void>() {
                        public Void run() throws Exception {
                            initWriter(file);
                            doRollingClean(loggingEvent);
                            return null;
                        }
                    });
                }
                write(layout.format(loggingEvent));
                size--;
            }
            flush();
        }

        private void initWriter(Path outPath) throws IOException {
            closeWriter();
            Configuration conf = new Configuration();
            String workingdir = System.getProperty("kap.hdfs.working.dir");
            fileSystem = HadoopUtil.getFileSystem(new Path(workingdir), conf);

            int retry = 10;
            for (int i = 0; i < retry; i++) {
                try {
                    outStream = fileSystem.create(outPath, true);
                    break;
                } catch (Exception e) {
                    LogLog.error("fail to create stream for path: " + outPath);
                    LogLog.error("", e);
                }

                try {
                    Thread.sleep(1000);//waiting for acl to turn to current user
                } catch (InterruptedException e) {
                    LogLog.warn("Init writer interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream));
        }

        private void updateOutPutDir(LoggingEvent event) {
            if (rollingByHour) {
                String rollingDir = dateFormat.format(new Date(event.getTimeStamp())) + "/"
                        + hourFormat.format(new Date(event.getTimeStamp()));
                outPutPath = getOutPutDir(rollingDir);
            } else {
                String rollingDir = dateFormat.format(new Date(event.getTimeStamp()));
                outPutPath = getOutPutDir(rollingDir);
            }
        }

        private void write(String buf) throws IOException {
            bufferedWriter.write(buf);
        }

        private void flush() throws IOException {
            bufferedWriter.flush();
            outStream.hsync();
        }

        private String getOutPutDir(String rollingDir) {
            if (StringUtils.isBlank(executorId)) {
                executorId = SparkEnv.get() != null ? SparkEnv.get().executorId() : UUID.randomUUID().toString();
                LogLog.warn("executorId set to " + executorId);
            }

            if ("job".equals(category)) {
                return getRootPathName() + "/" + rollingDir + "/" + identifier + "/" + jobName + "/" + "executor-"
                        + executorId + ".log";
            }
            return getRootPathName() + "/" + rollingDir + "/" + identifier + "/" + "executor-" + executorId + ".log";
        }

        private void doRollingClean(LoggingEvent event) throws IOException {

            if (fileSystem == null) {
                fileSystem = HadoopUtil.getWorkingFileSystem();
            }

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

        private String getRootPathName() {
            if ("job".equals(category)) {
                return parseHdfsWordingDir() + "/" + project + "/spark_logs";
            } else if ("sparder".equals(category)) {
                return parseHdfsWordingDir() + "/sparder_logs";
            } else {
                throw new IllegalArgumentException("illegal category: " + category);
            }
        }

        private boolean isTimeChanged(LoggingEvent event) {
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
            if (metadataUrl.contains("@")) {
                return StringUtils.appendIfMissing(hdfsWorkingDir, "/")
                        + metadataUrl.substring(0, metadataUrl.indexOf("@"));
            }

            return StringUtils.appendIfMissing(hdfsWorkingDir, "/") + StringUtils.replace(metadataUrl, "/", "-");
        }
    }

}
