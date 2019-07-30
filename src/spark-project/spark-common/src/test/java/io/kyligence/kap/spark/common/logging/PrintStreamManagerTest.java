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

import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class PrintStreamManagerTest {

    public static final Logger logger = LoggerFactory.getLogger(PrintStreamManagerTest.class);

    public void deleteFileQuiet(String path) {
        deleteFile(path);
    }

    private boolean deleteFile(String path) {
        File file = new File(path);

        if (file.isFile() && file.exists()) {
            return file.delete();
        }

        return false;
    }

    @Test
    public void testCreateLoggingProxy() {
        String logFile = "/tmp/PrintStreamManagerTest.log";

        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "INFO,file");
        properties.setProperty("log4j.appender.file", "org.apache.log4j.RollingFileAppender");
        properties.setProperty("log4j.appender.file.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.file.File", logFile);
        properties.setProperty("log4j.appender.file.layout.ConversionPattern", "%d{ISO8601} %-5p [%t] %c{2} : %m%n");
        PropertyConfigurator.configure(properties);

        System.setOut(PrintStreamManager.createLoggingProxy(logger, System.out, false));
        System.setErr(PrintStreamManager.createLoggingProxy(logger, System.err, true));

        try {
            System.out.println("line1");
            System.out.println("line2");

            Assert.assertEquals(2, FileUtils.readLines(new File(logFile)).size());

            PrintStreamManager.resetSystemPrintStream();
            System.out.println("line3");

            Assert.assertEquals(2, FileUtils.readLines(new File(logFile)).size());
        } catch (IOException e) {

        } finally {
            deleteFileQuiet(logFile);
        }
    }

}
