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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsFileAppenderTest {

    public static final Logger logger = LoggerFactory.getLogger(HdfsFileAppenderTest.class);

    @Test
    public void testHdfsFileAppender() throws InterruptedException, IOException {
        Configuration conf = new Configuration();
        HdfsFileAppender hdfsFileAppender = new HdfsFileAppender();
        SparkEnv.setUGI(UserGroupInformation.getLoginUser());

        String testWorkDir = "/tmp/hdfs_file_appender";
        String logPath = testWorkDir + "/test_01.log";
        hdfsFileAppender.setHdfsWorkingDir(testWorkDir);
        hdfsFileAppender.setLogPath(logPath);
        try {
            boolean isFinish = RunBenchmark(hdfsFileAppender, 4, 1000000, 10_000L);
            Assert.assertTrue(isFinish);
            // concurrent put make it more than 1000000
            Assert.assertEquals(1000000, FileUtils.readLines(new File(hdfsFileAppender.getLogPath())).size());
        } finally {
            HadoopUtil.getFileSystem(new Path(testWorkDir), conf).deleteOnExit(new Path(testWorkDir));
        }
    }

    @Test
    public void testErrorHdfsFileAppender() throws InterruptedException, IOException {
        Configuration conf = new Configuration();
        TestErrorHdfsFileAppender testErrorHdfsFileAppender = new TestErrorHdfsFileAppender();
        SparkEnv.setUGI(UserGroupInformation.getLoginUser());

        String testWorkDir = "/tmp/hdfs_file_appender";
        String logPath = testWorkDir + "/test_01.log";
        testErrorHdfsFileAppender.setHdfsWorkingDir(testWorkDir);
        testErrorHdfsFileAppender.setLogPath(logPath);
        try {
            boolean isFinish = RunBenchmark(testErrorHdfsFileAppender, 4, 1000000, 10_000L);
            Assert.assertTrue(isFinish);
            // concurrent put make it more than 1000000
            Assert.assertTrue(1000000 > FileUtils.readLines(new File(testErrorHdfsFileAppender.getLogPath())).size());
        } finally {
            testErrorHdfsFileAppender.close();
            HadoopUtil.getFileSystem(new Path(testWorkDir), conf).deleteOnExit(new Path(testWorkDir));
        }
    }

    private boolean RunBenchmark(final HdfsFileAppender hdfsFileAppender, int threadNumber, final int size, long timeout) throws InterruptedException, IOException {
        long start = System.currentTimeMillis();
        hdfsFileAppender.setLayout(new Layout() {
            @Override
            public String format(LoggingEvent loggingEvent) {
                return loggingEvent.toString() + "\n";
            }

            @Override
            public boolean ignoresThrowable() {
                return false;
            }

            @Override
            public void activateOptions() {

            }
        });

        hdfsFileAppender.activateOptions();
        final LoggingEvent loggingEvent = new LoggingEvent("1", org.apache.log4j.Logger.getLogger("sdad"), 10,
                Level.ERROR, "sdadas", null);
        final AtomicLong atomicLong = new AtomicLong();
        final CountDownLatch countDownLatch = new CountDownLatch(threadNumber);
        final Semaphore semaphore = new Semaphore(size);
        final long l = System.currentTimeMillis();
        for (int i = 0; i < threadNumber; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (true) {
                            if (semaphore.tryAcquire(10L, TimeUnit.MILLISECONDS)) {
                                hdfsFileAppender.append(loggingEvent);
                                atomicLong.incrementAndGet();
                                if (atomicLong.get() % 100000 == 0) {
                                    System.out.println(
                                            atomicLong.get() / ((System.currentTimeMillis() - l + 1000) / 1000));
                                }
                            } else {
                                break;
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                }
            }).start();
        }
        countDownLatch.await();
        while (hdfsFileAppender.logBufferQue.size() > 0) {
            if ((System.currentTimeMillis() - start) > timeout) {
                return false;
            }
        }
        hdfsFileAppender.close();
        return true;
    }

}
