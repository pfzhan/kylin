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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

import com.jcraft.jsch.Session;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kyligence.kap.junit.annotation.MetadataInfo;

/**
 * @author ysong1
 */
@MetadataInfo(onlyProps = true)
public class SSHClientTest {

    private boolean isRemote;
    private String hostname;
    private int port;
    private String username;
    private String password;

    @BeforeEach
    public void setUp() throws Exception {
        loadPropertiesFile();

    }

    private void loadPropertiesFile() throws IOException {

        KylinConfig cfg = KylinConfig.getInstanceFromEnv();

        this.isRemote = cfg.getRunAsRemoteCommand();
        this.port = cfg.getRemoteHadoopCliPort();
        this.hostname = cfg.getRemoteHadoopCliHostname();
        this.username = cfg.getRemoteHadoopCliUsername();
        this.password = cfg.getRemoteHadoopCliPassword();
    }

    @Test
    public void testCmd() throws Exception {
        if (!isRemote)
            return;

        SSHClient ssh = new SSHClient(this.hostname, this.port, this.username, this.password);
        SSHClientOutput output = ssh.execCommand("echo hello");
        Assert.assertEquals(0, output.getExitCode());
        Assert.assertEquals("hello\n", output.getText());
    }

    @Test
    public void testScpFileToRemote() throws Exception {
        if (!isRemote)
            return;

        SSHClient ssh = new SSHClient(this.hostname, this.port, this.username, this.password);
        File tmpFile = File.createTempFile("test_scp", "", new File("/tmp"));
        tmpFile.deleteOnExit();
        FileUtils.write(tmpFile, "test_scp", Charset.defaultCharset());
        ssh.scpFileToRemote(tmpFile.getAbsolutePath(), "/tmp");
    }

    @Test
    public void testRemoveKerberosPromption() throws Exception{
        SSHClient ssh = new SSHClient(this.hostname, this.port, this.username, this.password);
        Method newJSchSession = ssh.getClass().getDeclaredMethod("newJSchSession");
        newJSchSession.setAccessible(true);
        Session s = (Session)newJSchSession.invoke(ssh);
        Assert.assertEquals("no", s.getConfig("StrictHostKeyChecking"));
        Assert.assertEquals("publickey,keyboard-interactive,password", s.getConfig("PreferredAuthentications"));

    }
}
