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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class HdfsFileAppender extends AppenderSkeleton {

    FSDataOutputStream outStream = null;
    BufferedWriter bufferedWriter = null;

    @VisibleForTesting
    FileSystem fileSystem = null;

    @VisibleForTesting
    BlockingDeque<LoggingEvent> logBufferQue = null;
    private ExecutorService appendHdfsService = null;
    private HdfsFileAppender.HdfsFlushService flushService;

    //configurable
    @Getter
    @Setter
    private int logQueueCapacity = 5000;
    @Getter
    @Setter
    private int flushInterval = 5000;

    @Getter
    @Setter
    private String hdfsWorkingDir;

    @Getter
    @Setter
    private String logPath;

    // kerberos
    @Getter
    @Setter
    private boolean kerberosEnable = false;
    @Getter
    @Setter
    private String kerberosPrincipal;
    @Getter
    @Setter
    private String kerberosKeytab;

    /**
     * init the load resource.
     */
    @Override
    public void activateOptions() {
        init();
    }

    /**
     * init the hdfs append service.
     */
    private void init() {
        LogLog.warn("HdfsFileAppender starting ...");
        LogLog.warn("spark.driver.log4j.appender.hdfs.File -> " + getLogPath());
        LogLog.warn("kerberosEnable -> " + isKerberosEnable());

        logBufferQue = new LinkedBlockingDeque<>(logQueueCapacity);

        appendHdfsService = Executors.newSingleThreadExecutor();
        flushService = initService();
        appendHdfsService.execute(flushService);

        Runtime.getRuntime().addShutdownHook(new Thread((this::close)));
        LogLog.warn("HdfsFileAppender Started ...");
    }

    protected HdfsFileAppender.HdfsFlushService initService() {
        return new HdfsFileAppender.HdfsFlushService();
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

    /**
     * close the hdfs writer.
     * shutdown the roll polling thread.
     */
    @Override
    public void close() {
        this.closed = true;

        try {
            flushService.flushLog(logBufferQue.size());
            closeWriter();
            if (appendHdfsService != null && !appendHdfsService.isShutdown()) {
                appendHdfsService.shutdownNow();
            }
        } catch (Exception e) {
            LogLog.error("close HdfsAppender failed", e);
        }

        LogLog.warn("HdfsFileAppender Closed ...");
    }

    private void closeWriter() {
        IOUtils.closeQuietly(bufferedWriter);
        IOUtils.closeQuietly(outStream);
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @VisibleForTesting
    class HdfsFlushService implements Runnable {

        /**
         * roll polling，to check the size of queue and write the event to HDFS.
         * write to HDFS condition: event size > logQueueCapacity*0.2 and flushInterval > 5000ms
         */
        @Override
        public void run() {
            setName("SparkDriverLogAppender");
            long start = System.currentTimeMillis();
            do {
                try {
                    int queSize = logBufferQue.size();
                    if (queSize > logQueueCapacity * 0.2
                            || System.currentTimeMillis() - start > flushInterval) {
                        flushLog(queSize);
                        start = System.currentTimeMillis();
                    } else {
                        Thread.sleep(flushInterval / 100);
                    }
                } catch (Exception e) {
                    if (logBufferQue.size() >= logQueueCapacity) {
                        int removeNum = 1000;
                        while (removeNum > 0) {
                            try {
                                logBufferQue.take();
                            } catch (Exception ex) {
                                LogLog.error("Take event interrupted!", ex);
                            }
                            removeNum--;
                        }
                    }
                    LogLog.error("Error occurred when consume event", e);
                }
            } while (!closed);
        }

        /**
         * take the all event from queue and write into the HDFS immediately.
         *
         * @param size
         * @throws IOException
         * @throws InterruptedException
         */
        private synchronized void flushLog(int size) throws IOException, InterruptedException {
            if (null == outStream && null == bufferedWriter) {
                try {
                    initWriter(new Path(getLogPath()));
                } catch (IOException e) {
                    LogLog.error("init the Hdfs writer failed!", e);
                    throw e;
                }
            }

            if (0 == size || logBufferQue.isEmpty())
                return;

            while (size > 0) {
                final LoggingEvent loggingEvent = logBufferQue.take();
                write(layout.format(loggingEvent));
                if (null != loggingEvent.getThrowableStrRep()) {
                    for (String stackMsg : loggingEvent.getThrowableStrRep()) {
                        write(stackMsg);
                        write("\n");
                    }
                }
                size--;
            }
            flush();
        }

        /**
         * init the hdfs writer;
         * sometimes, the HDFS is not healthy, will init failed;
         *
         * @param outPath
         * @throws IOException
         */
        private void initWriter(Path outPath) throws IOException {
            closeWriter();

            Configuration conf = new Configuration();
            if (isKerberosEnable()) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(getKerberosPrincipal(), getKerberosKeytab());
            }

            fileSystem = HadoopUtil.getFileSystem(outPath, conf);
            int retry = 10;
            for (int i = 0; i < retry; i++) {
                try {
                    outStream = fileSystem.create(outPath, true);
                    break;
                } catch (Exception e) {
                    LogLog.error("fail to create stream for path: " + outPath, e);
                }

                try {
                    // wait for HDFS ok.
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LogLog.warn("Init writer interrupted!", e);
                    // Restore interrupted state...
                    Thread.currentThread().interrupt();
                }
            }
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream));
        }

        /**
         * write the data into the buffer.
         *
         * @param buf
         * @throws IOException
         */
        private void write(String buf) throws IOException {
            bufferedWriter.write(buf);
        }

        /**
         * flush the buffer data to HDFS.
         *
         * @throws IOException
         */
        protected void flush() throws IOException {
            bufferedWriter.flush();
            outStream.hsync();
        }
    }
}
