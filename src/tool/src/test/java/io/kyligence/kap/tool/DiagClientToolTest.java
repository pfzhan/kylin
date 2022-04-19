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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_TIMESTAMP_COMPARE;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ZipFileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;
import io.kyligence.kap.tool.obf.KylinConfObfuscatorTest;
import lombok.val;

public class DiagClientToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinConfObfuscatorTest.prepare();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        DiagClientTool diagClientTool = new DiagClientTool();

        getTestConfig().setProperty("kylin.diag.task-timeout", "180s");
        long start = System.currentTimeMillis();
        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath() });
        long duration = System.currentTimeMillis() - start;
        Assert.assertTrue(
                "In theory, the running time of this case should not exceed two minutes. "
                        + "If other data is added subsequently, which causes the running time of the "
                        + "diagnostic package to exceed two minutes, please adjust this test.",
                duration < 2 * 60 * 1000);

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("_full_") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testExecuteWithInvalidParameter() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        try {
            DiagClientTool diagClientTool = new DiagClientTool();
            diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-startTime", "1604999712000",
                    "-endTime", "1604998712000" });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof KylinException);
            Assert.assertEquals(e.getCause().getMessage(), PARAMETER_TIMESTAMP_COMPARE.getMsg());
        }

    }

    @Test
    public void testDestDirNotExist() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File existDir = new File(mainDir, "existDir");
        FileUtils.forceMkdir(existDir);
        File notExistDir = new File(mainDir, "notExistDir");

        DiagClientTool diagClientTool = new DiagClientTool();
        diagClientTool.execute(new String[] { "-destDir", existDir.getAbsolutePath() });
        diagClientTool.execute(new String[] { "-destDir", notExistDir.getAbsolutePath() });

        String existDirFileName = existDir.listFiles()[0].listFiles()[0].getName();
        String notExistDirFileName = notExistDir.listFiles()[0].listFiles()[0].getName();
        Assert.assertTrue(existDirFileName.endsWith(".zip"));
        Assert.assertTrue(notExistDirFileName.endsWith(".zip"));
    }

    @Test
    public void testObf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        DiagClientTool diagClientTool = new DiagClientTool();

        diagClientTool.execute(new String[] { "-destDir", mainDir.getAbsolutePath() });
        File zipFile = mainDir.listFiles()[0].listFiles()[0];
        File exportFile = new File(mainDir, "output");
        FileUtils.forceMkdir(exportFile);
        ZipFileUtils.decompressZipFile(zipFile.getAbsolutePath(), exportFile.getAbsolutePath());
        File baseDiagFile = exportFile.listFiles()[0];
        val properties = io.kyligence.kap.common.util.FileUtils
                .readFromPropertiesFile(new File(baseDiagFile, "conf/kylin.properties"));
        Assert.assertTrue(properties.containsValue(SensitiveConfigKeysConstant.HIDDEN));

    }

}
