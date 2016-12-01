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
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class HdfsAppender extends AppenderSkeleton {

    private static long A_DAY_MILLIS = 24 * 60 * 60 * 1000;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    FSDataOutputStream outStream = null;
    BufferedWriter bufferedWriter = null;
    FileSystem fileSystem = null;
    private String outPutPath;
    private String executorId;
    private BlockingDeque<LoggingEvent> logBufferQue = null;
    private ExecutorService appendHdfsService = null;
    private long startTime = 0;
    //configurable
    private String applicationId;
    private int doFlushCount = 1000;
    private int flushPeriod = 5;
    private boolean enableDailyRolling = true;
    private String hdfsWorkingDir;

    @Override
    public void activateOptions() {
        init();
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            logBufferQue.put(loggingEvent);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    private void write(String buf) throws IOException {
        bufferedWriter.write(buf.toString());
    }

    private void flush() throws IOException {
        bufferedWriter.flush();
        outStream.hsync();
    }

    private void init() {
        this.executorId = UUID.randomUUID().toString();
        if (null == this.applicationId || this.applicationId.trim().isEmpty())
            this.applicationId = "default";
        System.out.println("HdfsAppender Start with App ID: " + applicationId);

        logBufferQue = new LinkedBlockingDeque<>(5000);

        appendHdfsService = Executors.newSingleThreadExecutor();
        appendHdfsService.execute(new Thread() {
            @Override
            public void run() {
                setName("SparkExecutorLogAppender");
                long start = System.currentTimeMillis();
                try {
                    while (true) {
                        //Small chunk will be flushed each 5 seconds
                        int curSize = logBufferQue.size();
                        if (curSize < doFlushCount) {
                            Thread.sleep(50L);
                            long end = System.currentTimeMillis();
                            if (end - start > flushPeriod * 1000L) {
                                flushLog(curSize);
                                start = System.currentTimeMillis();
                            }
                        } else {
                            //Big chunk will be flushed immediately
                            flushLog(curSize);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            private void flushLog(int size) throws IOException, InterruptedException {
                if (size == 0)
                    return;

                while (size > 0) {
                    LoggingEvent loggingEvent = logBufferQue.take();
                    if (isDayChanged(loggingEvent)) {
                        updateOutPutDir(loggingEvent);
                        Path file = new Path(outPutPath);
                        initWriter(file);
                    }
                    write(layout.format(loggingEvent));
                    size--;
                }
                flush();
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

    public void setFlushPeriod(int flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    public int getFlushPeriod() {
        return this.flushPeriod;
    }

    private void initWriter(Path outPath) throws IOException {
        closeWriter();
        Configuration conf = new Configuration();
        fileSystem = FileSystem.get(conf);
        outStream = fileSystem.create(outPath, true);
        bufferedWriter = new BufferedWriter(new OutputStreamWriter(outStream));
    }

    private void closeWriter() throws IOException {
        if (null == outStream || null == fileSystem || null == bufferedWriter)
            return;
        bufferedWriter.close();
        outStream.close();
        fileSystem.close();
    }

    private void updateOutPutDir(LoggingEvent event) {
        outPutPath = parseHdfsWordingDir() + "/" + "spark_logs" + "/" + dateFormat.format(new Date(event.getTimeStamp())) + "/" + "application-" + getApplicationId() + "/" + "executor-" + this.executorId + ".log";
    }

    private boolean isDayChanged(LoggingEvent event) {
        if (0 == startTime || ((event.getTimeStamp() / A_DAY_MILLIS) - (startTime / A_DAY_MILLIS)) > 0) {
            startTime = event.getTimeStamp();
            return true;
        }
        return false;
    }

    private String parseHdfsWordingDir() {
        if (this.hdfsWorkingDir.contains("@"))
            return this.hdfsWorkingDir.substring(0, this.hdfsWorkingDir.indexOf("@"));
        return hdfsWorkingDir;
    }
}
