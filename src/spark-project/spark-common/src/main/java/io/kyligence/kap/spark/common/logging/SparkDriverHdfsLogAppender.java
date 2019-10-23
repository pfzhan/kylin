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

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.util.List;

public class SparkDriverHdfsLogAppender extends AbstractHdfsLogAppender {

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

    @Override
    public void init() {
        LogLog.warn("spark.driver.log4j.appender.hdfs.File -> " + getLogPath());
        LogLog.warn("kerberosEnable -> " + isKerberosEnable());
        if (isKerberosEnable()) {
            LogLog.warn("kerberosPrincipal -> " + getKerberosPrincipal());
            LogLog.warn("kerberosKeytab -> " + getKerberosKeytab());
        }
    }

    @Override
    String getAppenderName() {
        return "SparkDriverHdfsLogAppender";
    }

    @Override
    public boolean isSkipCheckAndFlushLog() {
        return false;
    }

    @Override
    public void doWriteLog(int eventSize, List<LoggingEvent> transaction)
            throws IOException, InterruptedException {
        if (!isWriterInited()) {
            Configuration conf = new Configuration();
            if (isKerberosEnable()) {
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(getKerberosPrincipal(), getKerberosKeytab());
            }
            if (!initHdfsWriter(new Path(getLogPath()), conf)) {
                LogLog.error("init the hdfs writer failed!");
            }
        }

        while (eventSize > 0) {
            LoggingEvent loggingEvent = getLogBufferQue().take();
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            eventSize--;
        }
    }
}
