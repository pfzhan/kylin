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

package io.kyligence.kap.secondstorage.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.kyligence.kap.common.util.EncryptUtil;

public class Node {
    private String name;
    private String ip;
    private int port;
    private String user;
    private String password;
    @JsonProperty("sshPort")
    private int sshPort;


    public Node(String name, String ip, int port, String user, String password) {
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public Node(String name, String ip, int port, String user, String password, int sshPort) {
        this.name = name;
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
        this.sshPort = sshPort;
    }

    public Node(Node node) {
        this(node.name, node.ip, node.port, node.user, node.password);
    }

    public Node() {
    }

    public String getName() {
        return name;
    }

    public Node setName(String name) {
        this.name = name;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public Node setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public int getPort() {
        return port;
    }

    public Node setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUser() {
        return user;
    }

    public Node setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return EncryptUtil.isEncrypted(password) ? EncryptUtil.decryptPassInKylin(password) : password;
    }

    public Node setPassword(String password) {
        this.password = password;
        return this;
    }

    public int getSSHPort() {
        return sshPort;
    }

    public Node setSSHPort(int sshPort) {
        this.sshPort = sshPort;
        return this;
    }
}
