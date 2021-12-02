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

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.status.StatusLogger;

import lombok.Getter;
import lombok.Setter;

@Plugin(name = "DriverHdfsRollingAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class SparkDriverHdfsRollingLogAppender extends AbstractHdfsLogAppender {

    @Setter
    @Getter
    private long rollingByteSize;

    @Getter
    @Setter
    private String logPath;

    @Getter
    @Setter
    private boolean kerberosEnabled = false;

    @Getter
    @Setter
    private String kerberosPrincipal;

    @Getter
    @Setter
    private String kerberosKeytab;

    private static SparkDriverHdfsRollingLogAppender appender;

    protected SparkDriverHdfsRollingLogAppender(String name, Layout<? extends Serializable> layout, Filter filter,
            boolean ignoreExceptions, boolean immediateFlush, Property[] properties, HdfsManager manager) {
        super(name, layout, filter, ignoreExceptions, immediateFlush, properties, manager);
    }

    @Override
    String getAppenderName() {
        return "SparkDriverHdfsRollingLogAppender";
    }

    @Override
    public void init() {
        StatusLogger.getLogger().warn("spark.driver.log4j.appender.hdfs.File -> {}", getLogPath());
        StatusLogger.getLogger().warn("kerberosEnable -> {}", isKerberosEnabled());
        if (isKerberosEnabled()) {
            StatusLogger.getLogger().warn("kerberosPrincipal -> {}", getKerberosPrincipal());
            StatusLogger.getLogger().warn("kerberosKeytab -> {}", getKerberosKeytab());
        }
    }

    @Override
    public boolean isSkipCheckAndFlushLog() {
        return false;
    }

    @Override
    public void doWriteLog(int eventSize, List<LogEvent> transaction) throws IOException, InterruptedException {
        if (needRollingFile(getLogPath(), getRollingByteSize())) {
            StatusLogger.getLogger().debug("current log file size > {}, need to rolling", getRollingByteSize());
            setLogPath(updateOutPutPath(getLogPath()));
        }
        if (!isWriterInited() && !initHdfsWriter(new Path(getLogPath()), new Configuration())) {
            StatusLogger.getLogger().error("init the hdfs writer failed!");
        }

        while (eventSize > 0) {
            LogEvent loggingEvent = getLogBufferQue().take();
            transaction.add(loggingEvent);
            writeLogEvent(loggingEvent);
            eventSize--;
        }
    }

    @Override
    String getLogPathAfterRolling(String logPath) {
        Path pathProcess = new Path(logPath);
        return String.format(Locale.ROOT, "%s/driver.%s.log_processing", pathProcess.getParent().toString(),
                System.currentTimeMillis());
    }

    @Override
    String getLogPathRollingDone(String logPath) {
        return StringUtils.replace(logPath, "_processing", "");
    }

    @PluginFactory
    public synchronized static SparkDriverHdfsRollingLogAppender createAppender(@PluginAttribute("name") String name,
            @PluginAttribute("kerberosEnabled") boolean kerberosEnabled,
            @PluginAttribute("kerberosPrincipal") String kerberosPrincipal,
            @PluginAttribute("kerberosKeytab") String kerberosKeytab,
            @PluginAttribute("workingDir") String hdfsWorkingDir, @PluginAttribute("logPath") String logPath,
            @PluginAttribute("logQueueCapacity") int logQueueCapacity,
            @PluginAttribute("flushInterval") int flushInterval,
            @PluginAttribute("rollingByteSize") long rollingByteSize,
            @PluginElement("Layout") Layout<? extends Serializable> layout, @PluginElement("Filter") Filter filter,
            @PluginElement("Properties") Property[] properties) {
        if (appender != null) {
            return appender;
        }
        HdfsManager manager = new HdfsManager(name, layout);
        appender = new SparkDriverHdfsRollingLogAppender(name, layout, filter, false, false, properties, manager);
        appender.setKerberosEnabled(kerberosEnabled);
        if (kerberosEnabled) {
            appender.setKerberosPrincipal(kerberosPrincipal);
            appender.setKerberosKeytab(kerberosKeytab);
        }
        appender.setWorkingDir(hdfsWorkingDir);
        appender.setLogPath(logPath.concat("_processing"));
        appender.setRollingByteSize(rollingByteSize);
        if (appender.getRollingByteSize() == 0L) {
            appender.setRollingByteSize(ROLLING_BYTE_SIZE_DEFAULT);
        }
        appender.setLogQueueCapacity(logQueueCapacity);
        appender.setFlushInterval(flushInterval);
        return appender;
    }

}
