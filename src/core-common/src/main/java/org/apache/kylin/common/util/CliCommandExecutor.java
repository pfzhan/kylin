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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.JobProcessContext;

/**
 * @author yangli9
 */
public class CliCommandExecutor {

    private String remoteHost;
    private int port;
    private String remoteUser;
    private String remotePwd;
    private int remoteTimeoutSeconds = 3600;

    public CliCommandExecutor() {
    }

    public void setRunAtRemote(String host, int port, String user, String pwd) {
        this.remoteHost = host;
        this.port = port;
        this.remoteUser = user;
        this.remotePwd = pwd;
    }

    public void setRunAtLocal() {
        this.remoteHost = null;
        this.remoteUser = null;
        this.remotePwd = null;
    }

    public void copyFile(String localFile, String destDir) throws IOException {
        if (remoteHost == null)
            copyNative(localFile, destDir);
        else
            copyRemote(localFile, destDir);
    }

    private void copyNative(String localFile, String destDir) throws IOException {
        File src = new File(localFile);
        File dest = new File(destDir, src.getName());
        FileUtils.copyFile(src, dest);
    }

    private void copyRemote(String localFile, String destDir) throws IOException {
        SSHClient ssh = new SSHClient(remoteHost, port, remoteUser, remotePwd);
        try {
            ssh.scpFileToRemote(localFile, destDir);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    public Pair<Integer, String> execute(String command, Logger logAppender) throws ShellException {
        return execute(command, logAppender, null);
    }

    public Pair<Integer, String> execute(String command, Logger logAppender, String jobId) throws ShellException {
        Pair<Integer, String> r;
        if (remoteHost == null) {
            r = runNativeCommand(command, logAppender, jobId);
        } else {
            r = runRemoteCommand(command, logAppender);
        }

        if (r.getFirst() != 0)
            throw new ShellException("OS command error exit with return code: " + r.getFirst() //
                    + ", error message: " + r.getSecond() + "The command is: \n" + command
                    + (remoteHost == null ? "" : " (remoteHost:" + remoteHost + ")") //
            );

        return r;
    }

    private Pair<Integer, String> runRemoteCommand(String command, Logger logAppender) throws ShellException {
        try {
            SSHClient ssh = new SSHClient(remoteHost, port, remoteUser, remotePwd);

            SSHClientOutput sshOutput;
            sshOutput = ssh.execCommand(command, remoteTimeoutSeconds, logAppender);
            int exitCode = sshOutput.getExitCode();
            String output = sshOutput.getText();
            return Pair.newPair(exitCode, output);
        } catch (Exception e) {
            throw new ShellException(e);
        }
    }

    private Pair<Integer, String> runNativeCommand(String command, Logger logAppender, String jobId) throws ShellException {
        try {

            String[] cmd = new String[3];
            String osName = System.getProperty("os.name");
            if (osName.startsWith("Windows")) {
                cmd[0] = "cmd.exe";
                cmd[1] = "/C";
            } else {
                cmd[0] = "/bin/bash";
                cmd[1] = "-c";
            }
            cmd[2] = command;

            ProcessBuilder builder = new ProcessBuilder(cmd);
            builder.environment().putAll(System.getenv());
            builder.redirectErrorStream(true);
            Process proc = builder.start();

            if (StringUtils.isNotBlank(jobId)) {
                JobProcessContext.registerProcess(jobId, proc);
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line;
            StringBuilder result = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                result.append(line).append('\n');
                if (logAppender != null) {
                    logAppender.log(line);
                }
            }

            try {
                int exitCode = proc.waitFor();
                return Pair.newPair(exitCode, result.toString());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
        } catch (Exception e) {
            throw new ShellException(e);
        } finally {
            if (StringUtils.isNotBlank(jobId)) {
                JobProcessContext.removeProcess(jobId);
            }
        }
    }

}
