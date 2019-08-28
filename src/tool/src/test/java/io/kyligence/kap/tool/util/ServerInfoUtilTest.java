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
package io.kyligence.kap.tool.util;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ServerInfoUtilTest extends NLocalFileMetadataTestCase {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Test
    public void testGetKylinClientInformation() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        String sourceKylinHome = System.getProperty("KYLIN_HOME");
        if (null == sourceKylinHome) {
            System.setProperty("KYLIN_HOME", mainDir.getAbsolutePath());
        }

        List<File> clearFileList = new ArrayList<>();
        String sha1 = "6a38664fe087f7f466ec4ad9ac9dc28415d99e52@KAP\nBuild with MANUAL at 2019-08-31 20:02:22";
        File sha1File = new File(KylinConfig.getKylinHome(), "commit_SHA1");
        if (!sha1File.exists()) {
            FileUtils.writeStringToFile(sha1File, sha1);
            clearFileList.add(sha1File);
        }

        String version = "Kyligence Enterprise 4.0.0-SNAPSHOT";
        File versionFile = new File(KylinConfig.getKylinHome(), "VERSION");
        if (!versionFile.exists()) {
            FileUtils.writeStringToFile(versionFile, version);
            clearFileList.add(versionFile);
        }

        String buf = ServerInfoUtil.getKylinClientInformation();

        if (null != sourceKylinHome) {
            System.setProperty("KYLIN_HOME", sourceKylinHome);
        }

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }

        Assert.assertTrue(buf.contains("commit:" + sha1.replace('\n', ';')));
        Assert.assertTrue(buf.contains("kap.version:" + version));
    }
}
