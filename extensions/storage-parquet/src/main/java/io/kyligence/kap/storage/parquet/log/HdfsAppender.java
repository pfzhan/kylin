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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

public class HdfsAppender extends AppenderSkeleton {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private StringBuffer buf = new StringBuffer();
    FSDataOutputStream outStream = null;
    private int logCount = 0;
    private String outPutPath = "";

    //configurable
    private String applicationId = "0";
    private String executorId = "0";
    private int doFlushCount = 10;
    private boolean enableDailyRolling = true;
    private String hdfsWorkingDir;
    private String metadataUrl;

    @Override
    public void activateOptions() {
        init();
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if (updateOutPutDir(loggingEvent)) {
            Path file = new Path(outPutPath);
            try {
                initWriter(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        buf.append(loggingEvent.getMessage());
        buf.append("\n");
        logCount++;

        if (logCount >= getDoFlushCount()) {

            try {
                write(buf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buf.delete(0, buf.length());
            logCount = 0;
        }
    }

    @Override
    public void close() {
        if (buf.length() > 0) {
            try {
                write(buf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            outStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    private void initWriter(Path outPath) throws IOException {

        if (null != outStream) {
            outStream.close();
            outStream = null;
        }
        Configuration conf = new Configuration();

        //conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath))
            fs.delete(outPath, true);
        outStream = fs.create(outPath);
    }

    private long write(StringBuffer buf) throws IOException {
        long size = buf.toString().length();
        outStream.writeChars(buf.toString());
        outStream.hflush();
        return size;
    }

    private void init() {
        if (this.executorId.equals("-1"))
            this.executorId = UUID.randomUUID().toString();

        if (null == this.applicationId || this.applicationId.trim().isEmpty())
            this.applicationId = "default";

        System.out.println("HdfsAppender Start with App ID: " + applicationId);
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getApplicationId() {
        return this.applicationId;
    }

    public void setExecutorId(String executorId) {
        this.executorId = executorId;
    }

    public String getExecutorId() {
        return this.executorId;
    }

    public void setHdfsWorkingDir(String hdfsWorkingDir) {
        this.hdfsWorkingDir = hdfsWorkingDir;
    }

    public String getHdfsWorkingDir() {
        return this.hdfsWorkingDir;
    }

    public void setMetadataUrl(String metadataUrl) {
        this.metadataUrl = metadataUrl;
    }

    public String getMetadataUrl() {
        return this.metadataUrl;
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

    private boolean updateOutPutDir(LoggingEvent event) {
        String tempOutput = getHdfsWorkingDir() + "/" + parseMetadataUrl() + "/" + "spark_logs" + "/" + (enableDailyRolling ? dateFormat.format(new Date(event.getTimeStamp())) : "") + "/" + "application-" + getApplicationId() + "/" + "executor-" + getExecutorId() + ".log";

        if (outPutPath.equals(tempOutput))
            return false;

        outPutPath = tempOutput;
        return true;
    }

    private String parseMetadataUrl() {
        return this.metadataUrl.substring(0, this.metadataUrl.indexOf("@"));
    }
}
