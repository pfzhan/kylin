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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.tool.util.ToolUtil;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConfToolTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExtractConf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "conf");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        File configFile = new File(confDir, "a.conf");
        FileUtils.writeStringToFile(configFile, "a=1");

        ConfTool.extractConf(mainDir);

        FileUtils.deleteQuietly(configFile);

        File newConfDir = new File(mainDir, "conf");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "a.conf").exists());
    }

    @Test
    public void testExtractHadoopConf() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "hadoop_conf");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        File configFile = new File(confDir, "a.conf");
        FileUtils.writeStringToFile(configFile, "a=1");

        ConfTool.extractHadoopConf(mainDir);

        FileUtils.deleteQuietly(configFile);

        File newConfDir = new File(mainDir, "hadoop_conf");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "a.conf").exists());
    }

    @Test
    public void testExtractBin() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File confDir = new File(ToolUtil.getKylinHome(), "bin");
        if (!confDir.exists()) {
            FileUtils.forceMkdir(confDir);
        }

        List<File> clearFileList = new ArrayList<>();
        File binFile = new File(confDir, "kylin.sh");
        if (!binFile.exists()) {
            clearFileList.add(binFile);
            FileUtils.writeStringToFile(binFile, "a=1");
        }

        File bootBinFile = new File(confDir, "bootstrap.sh");
        if (!bootBinFile.exists()) {
            clearFileList.add(bootBinFile);
            FileUtils.writeStringToFile(bootBinFile, "a=1");
        }

        ConfTool.extractBin(mainDir);

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }

        File newConfDir = new File(mainDir, "bin");
        Assert.assertTrue(newConfDir.exists());
        Assert.assertTrue(new File(newConfDir, "kylin.sh").exists());
        Assert.assertFalse(new File(newConfDir, "bootstrap.sh").exists());
    }
}
