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
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.Setter;

public abstract class AbstractHdfsLogAppender extends AppenderSkeleton {

    private final Object flushLogLock = new Object();
    private final Object initWriterLock = new Object();
    private final Object closeLock = new Object();
    private final Object fileSystemLock = new Object();

    private FSDataOutputStream outStream = null;
    private BufferedWriter bufferedWriter = null;

    private FileSystem fileSystem = null;

    private ExecutorService appendHdfsService = null;

    @Getter
    private BlockingDeque<LoggingEvent> logBufferQue = null;
    private static final double QUEUE_FLUSH_THRESHOLD = 0.2;

    //configurable
    @Getter
    @Setter
    private int logQueueCapacity = 8192;
    @Getter
    @Setter
    private int flushInterval = 5000;

    @Getter
    @Setter
    private String hdfsWorkingDir;

    public FileSystem getFileSystem() {
        if (null == fileSystem) {
            return getFileSystem(new Configuration());
        }
        return fileSystem;
    }

    private FileSystem getFileSystem(Configuration conf) {
        synchronized (fileSystemLock) {
            if (null == fileSystem) {
                try {
                    fileSystem = new Path(hdfsWorkingDir).getFileSystem(conf);
                } catch (IOException e) {
                    LogLog.error("Failed to create the file system, ", e);
                    throw new RuntimeException(e);
                }
            }
        }
        return fileSystem;
    }

    public boolean isWriterInited() {
        synchronized (initWriterLock) {
            return null != bufferedWriter;
        }
    }

    abstract void init();

    abstract String getAppenderName();

    /**
     * init the load resource.
     */
    @Override
    public void activateOptions() {
        LogLog.warn(String.format("%s starting ...", getAppenderName()));
        LogLog.warn("hdfsWorkingDir -> " + getHdfsWorkingDir());

        init();

        logBufferQue = new LinkedBlockingDeque<>(getLogQueueCapacity());
        appendHdfsService = Executors.newSingleThreadExecutor();
        appendHdfsService.execute(this::checkAndFlushLog);
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));

        LogLog.warn(String.format("%s started ...", getAppenderName()));
    }

    @Override
    public void append(LoggingEvent loggingEvent) {
        try {
            boolean offered = logBufferQue.offer(loggingEvent, 10, TimeUnit.SECONDS);
            if (!offered) {
                LogLog.error("LogEvent cannot put into the logBufferQue, log event content:");
                printLoggingEvent(loggingEvent);
            }
        } catch (InterruptedException e) {
            LogLog.warn("Append logging event interrupted!", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (!this.closed) {
                this.closed = true;

                List<LoggingEvent> transaction = Lists.newArrayList();
                try {
                    flushLog(getLogBufferQue().size(), transaction);

                    closeWriter();
                    if (appendHdfsService != null && !appendHdfsService.isShutdown()) {
                        appendHdfsService.shutdownNow();
                    }
                } catch (Exception e) {
                    transaction.forEach(this::printLoggingEvent);
                    try {
                        while (!getLogBufferQue().isEmpty()) {
                            printLoggingEvent(getLogBufferQue().take());
                        }
                    } catch (Exception ie) {
                        LogLog.error("clear the logging buffer queue failed!", ie);
                    }
                    LogLog.error(String.format("close %s failed!", getAppenderName()), e);
                }
                LogLog.warn(String.format("%s closed ...", getAppenderName()));
            }
        }
    }

    private void closeWriter() {
        IOUtils.closeQuietly(bufferedWriter);
        IOUtils.closeQuietly(outStream);
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    /**
     * some times need to wait the component init ok.
     *
     * @return
     */
    abstract boolean isSkipCheckAndFlushLog();

    /**
     * clear the log buffer queue when it was full.
     */
    private void clearLogBufferQueueWhenBlocked() {
        if (logBufferQue.size() >= getLogQueueCapacity()) {
            int removeNum = getLogQueueCapacity() / 5;
            while (removeNum > 0) {
                try {
                    LoggingEvent loggingEvent = logBufferQue.take();
                    printLoggingEvent(loggingEvent);
                } catch (Exception ex) {
                    LogLog.error("Take event interrupted!", ex);
                }
                removeNum--;
            }
        }
    }

    /**
     * print the logging event to stderr
     * @param loggingEvent
     */
    private void printLoggingEvent(LoggingEvent loggingEvent) {
        try {
            String log = getLayout().format(loggingEvent);
            LogLog.error(log.endsWith("\n") ? log.substring(0, log.length() - 1) : log);
            if (null != loggingEvent.getThrowableStrRep()) {
                for (String stack : loggingEvent.getThrowableStrRep()) {
                    LogLog.error(stack);
                }
            }
        } catch (Exception e) {
            LogLog.error("print logging event failed!", e);
        }
    }

    /**
     * flush the log to hdfs when conditions are satisfied.
     */
    protected void checkAndFlushLog() {
        long start = System.currentTimeMillis();
        do {
            List<LoggingEvent> transaction = Lists.newArrayList();
            try {
                if (isSkipCheckAndFlushLog()) {
                    continue;
                }

                int eventSize = getLogBufferQue().size();
                if (eventSize > getLogQueueCapacity() * QUEUE_FLUSH_THRESHOLD
                        || System.currentTimeMillis() - start > getFlushInterval()) {
                    // update start time before doing flushLog to avoid exception when flushLog
                    start = System.currentTimeMillis();
                    flushLog(eventSize, transaction);
                } else {
                    Thread.sleep(getFlushInterval() / 100);
                }
            } catch (Exception e) {
                transaction.forEach(this::printLoggingEvent);
                clearLogBufferQueueWhenBlocked();
                LogLog.error("Error occurred when consume event", e);
            }
        } while (!closed);
    }

    /**
     * init the hdfs writer and create the hdfs file with outPath.
     * need kerberos authentic, so fileSystem init here.
     *
     * @param outPath
     */
    protected boolean initHdfsWriter(Path outPath, Configuration conf) {
        synchronized (initWriterLock) {
            closeWriter();
            bufferedWriter = null;
            outStream = null;

            int retry = 10;
            while (retry-- > 0) {
                try {
                    fileSystem = getFileSystem(conf);
                    outStream = fileSystem.create(outPath, true);
                    break;
                } catch (Exception e) {
                    LogLog.error("fail to create stream for path: " + outPath, e);
                }

                try {
                    initWriterLock.wait(1000);//waiting for acl to turn to current user
                } catch (InterruptedException e) {
                    LogLog.warn("Init writer interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
            if (null != outStream) {
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream));
                return true;
            }
        }

        return false;
    }

    /**
     * write the data into the buffer.
     *
     * @param message
     * @throws IOException
     */
    protected void write(String message) throws IOException {
        bufferedWriter.write(message);
    }

    /**
     * write the error stack info into buffer
     *
     * @param loggingEvent
     * @throws IOException
     */
    protected void writeLogEvent(LoggingEvent loggingEvent) throws IOException {
        if (null != loggingEvent) {
            write(getLayout().format(loggingEvent));

            if (null != loggingEvent.getThrowableStrRep()) {
                for (String message : loggingEvent.getThrowableStrRep()) {
                    write(message);
                    write("\n");
                }
            }
        }
    }

    /**
     * do write log to the buffer.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    abstract void doWriteLog(int eventSize, List<LoggingEvent> transaction) throws IOException, InterruptedException;

    /**
     * flush the buffer data to HDFS.
     *
     * @throws IOException
     */
    private void flush() throws IOException {
        bufferedWriter.flush();
        outStream.hsync();
    }

    /**
     * take the all events from queue and write into the HDFS immediately.
     *
     * @param eventSize
     * @throws IOException
     * @throws InterruptedException
     */
    protected void flushLog(int eventSize, List<LoggingEvent> transaction) throws IOException, InterruptedException {
        if (eventSize <= 0) {
            return;
        }

        synchronized (flushLogLock) {
            if (eventSize > getLogBufferQue().size()) {
                eventSize = getLogBufferQue().size();
            }

            doWriteLog(eventSize, transaction);

            flush();
        }
    }
}
