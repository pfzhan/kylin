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

import lombok.val;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkEnv;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class SparkExecutorHdfsLogAppenderTest extends AbstractHdfsLogAppenderTestBase {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private void mockTimestamp(LoggingEvent mockEvent, String dateTime) throws IllegalAccessException, ParseException {
        val timestamp = dateFormat.parse(dateTime).getTime();
        val field = FieldUtils.getField(LoggingEvent.class, "timeStamp");
        FieldUtils.removeFinalModifier(field);
        field.setLong(mockEvent, timestamp);
    }

    @Test
    public void testIsTimeChanged() throws IllegalAccessException, ParseException {
        SparkExecutorHdfsLogAppender hdfsLogAppender = new SparkExecutorHdfsLogAppender();
        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);

        // 2019-01-01 00:00:00
        mockTimestamp(mockEvent, "2019-01-01 00:00:00");
        Assert.assertTrue(hdfsLogAppender.isTimeChanged(mockEvent));
        // 2019-01-01 01:00:00
        mockTimestamp(mockEvent, "2019-01-01 01:00:00");
        Assert.assertFalse(hdfsLogAppender.isTimeChanged(mockEvent));
        // 2019-02-01 00:00:00
        mockTimestamp(mockEvent, "2019-02-01 00:00:00");
        Assert.assertTrue(hdfsLogAppender.isTimeChanged(mockEvent));

        hdfsLogAppender.rollingByHour = true;
        hdfsLogAppender.startTime = 0L;
        // 2019-01-01 00:00:00
        mockTimestamp(mockEvent, "2019-01-01 00:00:00");
        Assert.assertTrue(hdfsLogAppender.isTimeChanged(mockEvent));
        // 2019-01-01 00:30:00
        mockTimestamp(mockEvent, "2019-01-01 00:30:00");
        Assert.assertFalse(hdfsLogAppender.isTimeChanged(mockEvent));
        // 2019-01-01 01:00:00
        mockTimestamp(mockEvent, "2019-01-01 01:00:00");
        Assert.assertTrue(hdfsLogAppender.isTimeChanged(mockEvent));
        // 2019-02-01 00:00:00
        mockTimestamp(mockEvent, "2019-02-01 00:00:00");
        Assert.assertTrue(hdfsLogAppender.isTimeChanged(mockEvent));
    }

    @Test
    public void testUpdateOutPutDir() throws ParseException, IllegalAccessException {
        SparkExecutorHdfsLogAppender hdfsLogAppender = new SparkExecutorHdfsLogAppender();

        hdfsLogAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsLogAppender.setHdfsWorkingDir("/path/to/hdfs_working_dir");
        hdfsLogAppender.setMetadataIdentifier("ut_metadata");
        hdfsLogAppender.setProject("default_project");
        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);
        mockTimestamp(mockEvent, "2019-02-01 10:00:00");

        // sparder log with rolling by days
        hdfsLogAppender.setCategory("sparder");
        hdfsLogAppender.setIdentifier("sparder_app_id");
        hdfsLogAppender.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/_sparder_logs/2019-02-01/sparder_app_id/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsLogAppender.outPutPath);

        // job log with rolling by days
        hdfsLogAppender.setCategory("job");
        hdfsLogAppender.setIdentifier("job_id");
        hdfsLogAppender.setJobName("job_name");
        hdfsLogAppender.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/default_project/spark_logs/2019-02-01/job_id/job_name/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsLogAppender.outPutPath);

        hdfsLogAppender.rollingByHour = true;

        // sparder log with rolling by hours
        hdfsLogAppender.setCategory("sparder");
        hdfsLogAppender.setIdentifier("sparder_app_id");
        hdfsLogAppender.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/_sparder_logs/2019-02-01/10/sparder_app_id/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsLogAppender.outPutPath);

        // job log with rolling by hours
        hdfsLogAppender.setCategory("job");
        hdfsLogAppender.setIdentifier("job_id");
        hdfsLogAppender.setJobName("job_name");
        hdfsLogAppender.updateOutPutDir(mockEvent);
        Assert.assertEquals(
                "/path/to/hdfs_working_dir/ut_metadata/default_project/spark_logs/2019-02-01/10/job_id/job_name/executor-94569abc-51aa-4f71-8ce5-f1e04835a848.log",
                hdfsLogAppender.outPutPath);
    }

    @Test
    public void testRollingClean() throws IOException, IllegalAccessException, ParseException {
        final String mainFolder = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();

        SparkExecutorHdfsLogAppender hdfsLogAppender = Mockito.spy(new SparkExecutorHdfsLogAppender());
        Mockito.doReturn(mainFolder).when(hdfsLogAppender).getRootPathName();
        Mockito.doReturn(new Path(mainFolder).getFileSystem(new Configuration())).when(hdfsLogAppender).getFileSystem();

        hdfsLogAppender.rollingPeriod = 2;
        hdfsLogAppender.getFileSystem().mkdirs(new Path(mainFolder, "2019-01-01"));
        hdfsLogAppender.getFileSystem().mkdirs(new Path(mainFolder, "2019-01-02"));
        hdfsLogAppender.getFileSystem().mkdirs(new Path(mainFolder, "2019-01-03"));
        hdfsLogAppender.getFileSystem().mkdirs(new Path(mainFolder, "2019-01-04"));
        hdfsLogAppender.getFileSystem().mkdirs(new Path(mainFolder, "2019-01-05"));
        Assert.assertEquals(5, hdfsLogAppender.getFileSystem().listStatus(new Path(mainFolder)).length);

        final LoggingEvent mockEvent = Mockito.mock(LoggingEvent.class);
        // 2019-01-05 00:00:00
        mockTimestamp(mockEvent, "2019-01-05 00:00:00");

        hdfsLogAppender.doRollingClean(mockEvent);
        final FileStatus[] actualFiles = hdfsLogAppender.getFileSystem().listStatus(new Path(mainFolder));
        Assert.assertEquals(3, actualFiles.length);
        final String[] actualFileNames = Arrays.stream(actualFiles).map(f -> f.getPath().getName())
                .toArray(String[]::new);
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-05"));
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-04"));
        Assert.assertTrue(ArrayUtils.contains(actualFileNames, "2019-01-03"));
    }

    @Test
    public void testSparkExecutorHdfsLogAppender() throws InterruptedException, IOException {
        final String mainFolder = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();

        SparkExecutorHdfsLogAppender hdfsLogAppender = new SparkExecutorHdfsLogAppender();
        hdfsLogAppender.setIdentifier("sparder_app_id");
        hdfsLogAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsLogAppender.setCategory("job");
        hdfsLogAppender.setHdfsWorkingDir(mainFolder + "/work");

        SparkEnv.setUGI(UserGroupInformation.getLoginUser());

        boolean isFinish = RunBenchmark(hdfsLogAppender, 4, 1000_000, 30_000L);
        Assert.assertTrue(isFinish);
        // concurrent put make it more than 1000000
        Assert.assertEquals(1000_000, FileUtils.readLines(new File(hdfsLogAppender.outPutPath)).size());
    }

    @Test
    public void testErrorSparkExecutorHdfsLogAppender() throws InterruptedException, IOException {
        final String mainFolder = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();

        TestErrorSparkExecutorHdfsLogAppender hdfsLogAppender = new TestErrorSparkExecutorHdfsLogAppender();
        hdfsLogAppender.setIdentifier("sparder_app_id");
        hdfsLogAppender.executorId = "94569abc-51aa-4f71-8ce5-f1e04835a848";
        hdfsLogAppender.setCategory("job");
        hdfsLogAppender.setHdfsWorkingDir(mainFolder + "/work");

        SparkEnv.setUGI(UserGroupInformation.getLoginUser());

        boolean isFinish = RunBenchmark(hdfsLogAppender, 4, 1000_000, 30_000L);
        Assert.assertTrue(isFinish);
        // throw exception make it miss some event
        Assert.assertTrue(1000_000 > FileUtils.readLines(new File(hdfsLogAppender.outPutPath)).size());
    }
}
