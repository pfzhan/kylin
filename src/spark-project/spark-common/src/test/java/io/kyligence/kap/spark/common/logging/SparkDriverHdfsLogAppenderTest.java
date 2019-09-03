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

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

public class SparkDriverHdfsLogAppenderTest extends AbstractHdfsLogAppenderTestBase {

    @Test
    public void testSparkDriverHdfsLogAppender() throws InterruptedException, IOException {
        String mainDir = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();

        SparkDriverHdfsLogAppender hdfsLogAppender = new SparkDriverHdfsLogAppender();

        String testWorkDir = mainDir + "/hdfs_file_appender";
        String logPath = testWorkDir + "/test_01.log";
        hdfsLogAppender.setHdfsWorkingDir(testWorkDir);
        hdfsLogAppender.setLogPath(logPath);

        boolean isFinish = RunBenchmark(hdfsLogAppender, 4, 1000_000, 30_000L);
        Assert.assertTrue(isFinish);
        // concurrent put make it more than 1000000
        Assert.assertEquals(1000_000, FileUtils.readLines(new File(hdfsLogAppender.getLogPath())).size());
    }

    @Test
    public void testErrorSparkDriverHdfsLogAppender() throws InterruptedException, IOException {
        String mainDir = temporaryFolder.getRoot().getAbsolutePath() + "/" + testName.getMethodName();

        TestErrorSparkDriverHdfsLogAppender hdfsLogAppender = new TestErrorSparkDriverHdfsLogAppender();
        String testWorkDir = mainDir + "/hdfs_file_appender";
        String logPath = testWorkDir + "/test_01.log";
        hdfsLogAppender.setHdfsWorkingDir(testWorkDir);
        hdfsLogAppender.setLogPath(logPath);

        boolean isFinish = RunBenchmark(hdfsLogAppender, 4, 1000_000, 30_000L);
        Assert.assertTrue(isFinish);
        // concurrent put make it more than 1000000
        Assert.assertTrue(1000_000 > FileUtils.readLines(new File(hdfsLogAppender.getLogPath())).size());
    }

}
