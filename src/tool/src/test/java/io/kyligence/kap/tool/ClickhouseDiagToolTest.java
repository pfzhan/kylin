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
package io.kyligence.kap.tool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class ClickhouseDiagToolTest extends NLocalFileMetadataTestCase {

    private static final String logs1 = "2020.05.18 18:15:50.745336 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2020.05.18 18:15:50.760828 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 258.53 MiB.\n"
            + "2021.05.18 15:15:50.923367 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68538_68538_0 to 202105_87671_87671_0.\n"
            + "2021.05.18 15:15:50.924274 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:15:58.424386 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:15:58.430833 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 539.80 MiB.\n"
            + "2021.05.18 18:15:58.434465 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68539_68539_0 to 202105_87672_87672_0.\n"
            + "2021.05.18 18:15:58.435039 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:05.935125 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:05.940605 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 642.16 MiB.\n"
            + "2021.05.18 18:16:05.944313 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68540_68540_0 to 202105_87673_87673_0.\n"
            + "2021.05.18 18:16:05.945236 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:13.445337 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:16:13.450534 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 876.80 MiB.\n"
            + "2021.05.18 18:16:13.453724 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68541_68541_0 to 202105_87674_87674_0.\n"
            + "2021.05.18 18:16:13.454482 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:20.954598 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:20.960346 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2021.05.18 18:16:20.963832 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68542_68542_0 to 202105_87675_87675_0.\n"
            + "2021.05.18 18:16:20.964657 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2022.05.18 18:16:20.975851 [ 12151 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Selected 2 parts from 202105_82621_87669_963 to 202105_87670_87670_0\n"
            + "2022.05.18 19:16:20.975914 [ 12151 ] {} <Debug> DiskLocal: Reserving 1.89 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2022.05.18 19:16:20.989143 [ 8600 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Merging 2 parts: from 202105_82621_87669_963 to 202105_87670_8";

    private static final String logs2 = "2021.05.18 18:15:58.424386 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:15:58.430833 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 539.80 MiB.\n"
            + "2021.05.18 18:15:58.434465 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68539_68539_0 to 202105_87672_87672_0.\n"
            + "2021.05.18 18:15:58.435039 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:05.935125 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:05.940605 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 642.16 MiB.\n"
            + "2021.05.18 18:16:05.944313 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68540_68540_0 to 202105_87673_87673_0.\n"
            + "2021.05.18 18:16:05.945236 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:13.445337 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:16:13.450534 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 876.80 MiB.\n"
            + "2021.05.18 18:16:13.453724 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68541_68541_0 to 202105_87674_87674_0.\n"
            + "2021.05.18 18:16:13.454482 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:20.954598 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:20.960346 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2021.05.18 18:16:20.963832 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68542_68542_0 to 202105_87675_87675_0.\n"
            + "2021.05.18 18:16:20.964657 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2022.05.18 18:16:20.975851 [ 12151 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Selected 2 parts from 202105_82621_87669_963 to 202105_87670_87670_0\n"
            + "2022.05.18 19:16:20.975914 [ 12151 ] {} <Debug> DiskLocal: Reserving 1.89 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2022.05.18 19:16:20.989143 [ 8600 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Merging 2 parts: from 202105_82621_87669_963 to 202105_87670_8\n";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();


    @Test
    public void testExtractCkLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File subDir = new File(mainDir, ClickhouseDiagTool.SUB_DIR);


        File tmpDir = new File(subDir, "tmp");
        File targetDir = new File(subDir, "targetDir");

        FileUtils.forceMkdir(mainDir);
        FileUtils.forceMkdir(subDir);
        FileUtils.forceMkdir(tmpDir);
        FileUtils.forceMkdir(targetDir);

        File ckLog = new File(tmpDir, "click-server.log");

        val dataFormat = ReflectionTestUtils.getField(ClickhouseDiagTool.class, "SECOND_DATE_FORMAT").toString();

        //2021.05.18 18:15:00 - 2021.05.18 19:15:00
        val timeRange = new Pair<>(new DateTime(1621332900000L).toString(dataFormat),
                new DateTime(1621336500000L).toString(dataFormat));

        FileUtils.writeStringToFile(ckLog, logs1);
        Assert.assertTrue(ckLog.setLastModified(1621336500000L));

        val ckLogCount = Files.lines(Paths.get(ckLog.getAbsolutePath())).count();

        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();

        ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "extractCkLogByRange", timeRange, targetDir, tmpDir);

        File extractedLogFile = new File(targetDir, "click-server.log");

        val count = Files.lines(Paths.get(extractedLogFile.getAbsolutePath())).count();
        Assert.assertEquals(ckLogCount - count, 4);

        val extractedLogFileStr = FileUtils.readFileToString(extractedLogFile);
        Assert.assertEquals(extractedLogFileStr, logs2);

    }

    @Test
    public void testGenerateCompressedFile() {
        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();

        {
            String serverLog = ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "getCompressedFileMatcher",
                    "*server-log.log", 2);
            Assert.assertEquals(serverLog, "{*server-log.log.0.gz,*server-log.log.1.gz}");
        }

        {
            String serverLog = ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "getCompressedFileMatcher",
                    "*server-log.log", 1);
            Assert.assertEquals(serverLog, "{*server-log.log.0.gz}");
        }

    }

}