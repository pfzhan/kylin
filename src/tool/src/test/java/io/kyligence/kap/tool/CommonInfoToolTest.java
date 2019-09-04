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

public class CommonInfoToolTest extends NLocalFileMetadataTestCase {

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
    public void testExportClientInfo() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportClientInfo(mainDir);
        File linuxDir = new File(mainDir, "client/linux");
        Assert.assertTrue(new File(linuxDir, "disk_usage").exists() && new File(linuxDir, "disk_usage").length() > 200);
        Assert.assertTrue(new File(mainDir, "client/hadoop").exists());
        Assert.assertTrue(new File(mainDir, "client/hive").exists());
        Assert.assertTrue(new File(mainDir, "client/kerberos").exists());
    }

    @Test
    public void testExportHadoopEnv() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportHadoopEnv(mainDir);

        File hadoopEnv = new File(mainDir, "hadoop_env");
        Assert.assertTrue(hadoopEnv.exists() && hadoopEnv.length() > 20);
    }

    @Test
    public void testExportKylinHomeDir() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        CommonInfoTool.exportKylinHomeDir(mainDir);

        File catalogInfo = new File(mainDir, "catalog_info");
        Assert.assertTrue(catalogInfo.exists() && catalogInfo.length() > 20);
    }

}
