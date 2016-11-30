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

package io.kyligence.kap.storage.parquet.log;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class HdfsAppender extends AppenderSkeleton {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private StringBuilder buf = new StringBuilder();
    FSDataOutputStream outStream = null;
    BufferedWriter bufferedWriter = null;
    FileSystem fileSystem = null;
    private int logCount = 0;
    private String outPutPath;
    private String executorId;
    private Queue<LoggingEvent> logBuffer = null;
    private ExecutorService appendHdfsService = null;
    //configurable
    private String applicationId;
    private int doFlushCount = 10;
    private boolean enableDailyRolling = true;
    private String hdfsWorkingDir;

    @Override
    public void activateOptions() {
        init();
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        logBuffer.offer(loggingEvent);
    }

    @Override
    public void close() {
        if (appendHdfsService != null)
            appendHdfsService.shutdownNow();
        try {
            closeWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closed = true;
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    private void write(StringBuilder buf) throws IOException {
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outStream));
        bw.write(buf.toString());
        bw.flush();
        outStream.hflush();
    }

    private void init() {
        this.executorId = UUID.randomUUID().toString();
        if (null == this.applicationId || this.applicationId.trim().isEmpty())
            this.applicationId = "default";
        System.out.println("HdfsAppender Start with App ID: " + applicationId);

        logBuffer = new ConcurrentLinkedDeque<>();

        appendHdfsService = Executors.newSingleThreadExecutor();
        appendHdfsService.execute(new Thread() {
            @Override
            public void run() {
                setName("SparkExecutorLogAppender");
                while (true) {
                    try {
                        if (logBuffer.isEmpty())
                            Thread.sleep(1000L);
                        else
                            flushLog();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            private void flushLog() throws IOException {
                while (!logBuffer.isEmpty()) {
                    LoggingEvent loggingEvent = logBuffer.poll();
                    if (updateOutPutDir(loggingEvent)) {
                        Path file = new Path(outPutPath);
                        initWriter(file);
                    }
                    buf.append(layout.format(loggingEvent));
                    logCount++;
                    if (logCount >= doFlushCount) {

                        write(buf);
                        buf = new StringBuilder();
                        logCount = 0;
                    }
                }
            }
        });
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationId() {
        return this.applicationId;
    }

    public void setHdfsWorkingDir(String hdfsWorkingDir) {
        this.hdfsWorkingDir = hdfsWorkingDir;
    }

    public String getHdfsWorkingDir() {
        return this.hdfsWorkingDir;
    }

    public void setEnableDailyRolling(boolean enableDailyRolling) {
        this.enableDailyRolling = enableDailyRolling;
    }

    public void setDoFlushCount(int doFlushCount) {
        this.doFlushCount = doFlushCount;
    }

    public int getDoFlushCount() {
        return this.doFlushCount;
    }

    private void initWriter(Path outPath) throws IOException {
        closeWriter();
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath))
            fileSystem.delete(outPath, true);
        outStream = fileSystem.create(outPath);
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream));
    }

    private void closeWriter() throws IOException {
        if (null == outStream || null == fileSystem || null == bufferedWriter)
            return;

        if (buf.length() > 0) {
            write(buf);
        }
        bufferedWriter.close();
        outStream.close();
        fileSystem.close();
    }

    private boolean updateOutPutDir(LoggingEvent event) {
        String tempOutput = parseHdfsWordingDir() + "/" + "spark_logs" + "/" + (enableDailyRolling ? dateFormat.format(new Date(event.getTimeStamp())) : "") + "/" + "application-" + getApplicationId() + "/" + "executor-" + this.executorId + ".log";

        if (outPutPath != null && outPutPath.equals(tempOutput))
            return false;

        outPutPath = tempOutput;
        return true;
    }

    private String parseHdfsWordingDir() {
        if (this.hdfsWorkingDir.contains("@"))
            return this.hdfsWorkingDir.substring(0, this.hdfsWorkingDir.indexOf("@"));
        return hdfsWorkingDir;
    }
}
