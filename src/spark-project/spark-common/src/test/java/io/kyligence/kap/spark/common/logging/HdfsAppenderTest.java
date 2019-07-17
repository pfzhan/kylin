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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class HdfsAppenderTest {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testIsTimeChanged() throws IllegalAccessException, ParseException {
        final HdfsAppender hdfsAppender = new HdfsAppender();
        final HdfsAppender.HdfsFlushService hdfsFlushService = hdfsAppender.new HdfsFlushService();
        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);

        // 2019-01-01 00:00:00
        mockTimestamp(mockEvent, "2019-01-01 00:00:00");
        Assert.assertTrue(hdfsFlushService.isTimeChanged(mockEvent));
        // 2019-01-01 01:00:00
        mockTimestamp(mockEvent, "2019-01-01 01:00:00");
        Assert.assertFalse(hdfsFlushService.isTimeChanged(mockEvent));
        // 2019-02-01 00:00:00
        mockTimestamp(mockEvent, "2019-02-01 00:00:00");
        Assert.assertTrue(hdfsFlushService.isTimeChanged(mockEvent));

        hdfsAppender.setRollingByHour(true);
        hdfsAppender.startTime = 0L;
        // 2019-01-01 00:00:00
        mockTimestamp(mockEvent, "2019-01-01 00:00:00");
        Assert.assertTrue(hdfsFlushService.isTimeChanged(mockEvent));
        // 2019-01-01 00:30:00
        mockTimestamp(mockEvent, "2019-01-01 00:30:00");
        Assert.assertFalse(hdfsFlushService.isTimeChanged(mockEvent));
        // 2019-01-01 01:00:00
        mockTimestamp(mockEvent, "2019-01-01 01:00:00");
        Assert.assertTrue(hdfsFlushService.isTimeChanged(mockEvent));
        // 2019-02-01 00:00:00
        mockTimestamp(mockEvent, "2019-02-01 00:00:00");
        Assert.assertTrue(hdfsFlushService.isTimeChanged(mockEvent));
    }

    @Test
    public void testUpdateOutPutDir() throws ParseException, IllegalAccessException {
        final HdfsAppender hdfsAppender = new HdfsAppender();
        final HdfsAppender.HdfsFlushService hdfsFlushService = hdfsAppender.new HdfsFlushService();
        hdfsAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsAppender.setHdfsWorkingDir("/path/to/hdfs_working_dir");
        hdfsAppender.setMetadataIdentifier("ut_metadata");
        hdfsAppender.setProject("default_project");
        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);
        mockTimestamp(mockEvent, "2019-02-01 10:00:00");

        // sparder log with rolling by days
        hdfsAppender.setCategory("sparder");
        hdfsAppender.setIdentifier("sparder_app_id");
        hdfsFlushService.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/_sparder_logs/2019-02-01/sparder_app_id/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsAppender.outPutPath);

        // job log with rolling by days
        hdfsAppender.setCategory("job");
        hdfsAppender.setIdentifier("job_id");
        hdfsAppender.setJobName("job_name");
        hdfsFlushService.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/default_project/spark_logs/2019-02-01/job_id/job_name/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsAppender.outPutPath);

        hdfsAppender.setRollingByHour(true);

        // sparder log with rolling by hours
        hdfsAppender.setCategory("sparder");
        hdfsAppender.setIdentifier("sparder_app_id");
        hdfsFlushService.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/_sparder_logs/2019-02-01/10/sparder_app_id/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsAppender.outPutPath);

        // job log with rolling by hours
        hdfsAppender.setCategory("job");
        hdfsAppender.setIdentifier("job_id");
        hdfsAppender.setJobName("job_name");
        hdfsFlushService.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/default_project/spark_logs/2019-02-01/10/job_id/job_name/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsAppender.outPutPath);
    }

    @Test
    public void testRollingClean() throws IOException, IllegalAccessException, ParseException {
        final String junitFolder = temporaryFolder.getRoot().getAbsolutePath();
        final HdfsAppender hdfsAppender = new HdfsAppender();
        final HdfsAppender.HdfsFlushService hdfsFlushService = Mockito.spy(hdfsAppender.new HdfsFlushService());
        Mockito.doReturn(junitFolder).when(hdfsFlushService).getRootPathName();
        hdfsAppender.fileSystem = new Path(temporaryFolder.getRoot().getAbsolutePath())
                .getFileSystem(new Configuration());

        hdfsAppender.setRollingPeriod(2);
        hdfsAppender.fileSystem.mkdirs(new Path(junitFolder, "2019-01-01"));
        hdfsAppender.fileSystem.mkdirs(new Path(junitFolder, "2019-01-02"));
        hdfsAppender.fileSystem.mkdirs(new Path(junitFolder, "2019-01-03"));
        hdfsAppender.fileSystem.mkdirs(new Path(junitFolder, "2019-01-04"));
        hdfsAppender.fileSystem.mkdirs(new Path(junitFolder, "2019-01-05"));
        Assert.assertEquals(5, hdfsAppender.fileSystem.listStatus(new Path(junitFolder)).length);

        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);
        // 2019-01-05 00:00:00
        mockTimestamp(mockEvent, "2019-01-05 00:00:00");

        hdfsFlushService.doRollingClean(mockEvent);
        final FileStatus[] actualFiles = hdfsAppender.fileSystem.listStatus(new Path(junitFolder));
        Assert.assertEquals(3, actualFiles.length);
        final String[] actualFileNames = Arrays.stream(actualFiles).map(f -> f.getPath().getName())
                .toArray(String[]::new);
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-05"));
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-04"));
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-03"));
    }

    private void mockTimestamp(LoggingEvent mockEvent, String dateTime) throws IllegalAccessException, ParseException {
        val timestamp = dateFormat.parse(dateTime).getTime();
        val field = FieldUtils.getField(LoggingEvent.class, "timeStamp");
        FieldUtils.removeFinalModifier(field);
        field.setLong(mockEvent, timestamp);
    }

    public static final Logger logger = LoggerFactory.getLogger(HdfsAppenderTest.class);

    @Test
    public void testHdfsAppender() throws InterruptedException, IOException {
        String workingdir = "/tmp/work";
        System.setProperty("kap.hdfs.working.dir", workingdir);
        Configuration conf = new Configuration();
        HdfsAppender hdfsAppender = new HdfsAppender();
        hdfsAppender.setIdentifier("sparder_app_id");
        hdfsAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsAppender.setCategory("job");
        SparkEnv.setUGI(UserGroupInformation.getLoginUser());
        try {
            boolean isFinish = RunBenchmark(hdfsAppender, 4, 1000000, 10_000L);
            Assert.assertTrue(isFinish);
            // concurrent put make it more than 1000000
            Assert.assertEquals(1000000, FileUtils.readLines(new File(hdfsAppender.outPutPath)).size());
        } finally {
            HadoopUtil.getFileSystem(new Path(workingdir), conf).deleteOnExit(new Path(workingdir));

        }
    }

    @Test
    public void testErrorHdfsAppender() throws InterruptedException, IOException {
        String workingdir = "/tmp/work";
        System.setProperty("kap.hdfs.working.dir", workingdir);
        Configuration conf = new Configuration();
        TestErrorHdfsAppender hdfsAppender = new TestErrorHdfsAppender();
        hdfsAppender.setIdentifier("sparder_app_id");
        hdfsAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsAppender.setCategory("job");
        SparkEnv.setUGI(UserGroupInformation.getLoginUser());
        try {
            System.setProperty("kap.hdfs.working.dir", "/tmp/work");
            boolean isFinish = RunBenchmark(hdfsAppender, 4, 1000000, 100_000L);
            Assert.assertTrue(isFinish);
            // throw exception make it miss some event
            Assert.assertTrue(1000000 > FileUtils.readLines(new File(hdfsAppender.outPutPath)).size());
        } finally {
            hdfsAppender.close();
            HadoopUtil.getFileSystem(new Path(workingdir), conf).deleteOnExit(new Path(workingdir));
        }
    }

    private boolean RunBenchmark(final HdfsAppender hdfsAppender, int threadNumber, final int size, long timeout)
            throws InterruptedException, IOException {
        long start = System.currentTimeMillis();
        hdfsAppender.setHdfsWorkingDir("/tmp/work");
        hdfsAppender.setLayout(new Layout() {
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
        hdfsAppender.activateOptions();
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
                                hdfsAppender.append(loggingEvent);
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
        while (hdfsAppender.logBufferQue.size() > 0) {
            if ((System.currentTimeMillis() - start) > timeout) {
                return false;
            }
        }
        hdfsAppender.close();
        return true;
    }
}
