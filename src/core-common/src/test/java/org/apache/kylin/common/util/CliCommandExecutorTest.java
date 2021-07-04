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
package org.apache.kylin.common.util;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import lombok.val;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CliCommandExecutor.class)
@PowerMockIgnore({"javax.management.*"})
public class CliCommandExecutorTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    @Rule
    public TestName testName = new TestName();

    @Test
    public void testCopyRemoteToLocal() throws Exception {

        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File tmpDir = new File(mainDir, "from");
        File tempFile = new File(tmpDir, "temp-file.log");
        FileUtils.writeStringToFile(tempFile, "abc");
        File targetDir = new File(mainDir, "to");

        SSHClient mockSSHClient = MockSSHClient.getInstance();
        CliCommandExecutor cliSpy = PowerMockito.spy(new CliCommandExecutor("localhost", "root", null));
        PowerMockito.doReturn(mockSSHClient).when(cliSpy, "getSshClient");

        cliSpy.copyRemoteToLocal(tempFile.getAbsolutePath(), targetDir.getAbsolutePath());


        val fileList = targetDir.listFiles();
        Assert.assertNotNull(fileList);
        Assert.assertEquals(1, fileList.length);
        Assert.assertEquals(fileList[0].getName(), tempFile.getName());

    }

    @Test
    public void testCopyLocalToRemote() throws Exception {

        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        File tmpDir = new File(mainDir, "from");
        File tempFile = new File(tmpDir, "temp-file.log");
        FileUtils.writeStringToFile(tempFile, "abc");
        File targetDir = new File(mainDir, "to");

        SSHClient mockSSHClient = MockSSHClient.getInstance();
        CliCommandExecutor cliSpy = PowerMockito.spy(new CliCommandExecutor("localhost", "root", null));
        PowerMockito.doReturn(mockSSHClient).when(cliSpy, "getSshClient");

        cliSpy.copyFile(tempFile.getAbsolutePath(), targetDir.getAbsolutePath());


        val fileList = targetDir.listFiles();
        Assert.assertNotNull(fileList);
        Assert.assertEquals(1, fileList.length);
        Assert.assertEquals(fileList[0].getName(), tempFile.getName());

    }
}

class MockSSHClient extends SSHClient {

    public static MockSSHClient getInstance() {
        return new MockSSHClient(null, -1, null, null);
    }

    public MockSSHClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    @Override
    public void scpRemoteFileToLocal(String remoteFile, String localTargetDirectory) throws Exception {
        FileUtils.copyFileToDirectory(new File(remoteFile), new File(localTargetDirectory));
    }

    @Override
    public void scpFileToRemote(String localFile, String remoteTargetDirectory) throws Exception {
        FileUtils.copyFileToDirectory(new File(localFile), new File(remoteTargetDirectory));
    }
}
