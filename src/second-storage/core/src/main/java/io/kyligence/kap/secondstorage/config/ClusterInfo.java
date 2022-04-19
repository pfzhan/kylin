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


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.util.EncryptUtil;
import org.apache.kylin.common.util.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ClusterInfo {
    private Map<String, List<Node>> cluster;
    private String socketTimeout;
    private String keepAliveTimeout;
    private String installPath;
    private String logPath;

    //username of machine
    private String userName;
    private String password;

    @JsonIgnore
    public List<Node> getNodes() {
        return Collections.unmodifiableList(cluster.values().stream().flatMap(List::stream).collect(Collectors.toList()));
    }

    public ClusterInfo setCluster(Map<String, List<Node>> cluster) {
        this.cluster = cluster;
        return this;
    }

    public Map<String, List<Node>> getCluster() {
        TreeMap<String, List<Node>> orderedMap = new TreeMap<>(cluster);
        return Collections.unmodifiableMap(orderedMap);
    }

    public void transformNode() {
        for (final Map.Entry<String, List<Node>> pair : cluster.entrySet()) {
            List<Node> nodes = pair.getValue();
            List<Node> transformedNodes = new ArrayList<>();
            for (Object node : nodes) {
                Node n;
                if (!(node instanceof Node)) {
                    n = JsonUtil.readValueQuietly(
                            JsonUtil.writeValueAsStringQuietly(node).getBytes(StandardCharsets.UTF_8),
                            Node.class);
                } else {
                    n = (Node) node;
                }
                if (n.getSSHPort() == 0) {
                    n.setSSHPort(22);
                }
                transformedNodes.add(n);
            }
            cluster.put(pair.getKey(), transformedNodes);
        }
    }

    public String getSocketTimeout() {
        return socketTimeout;
    }

    public ClusterInfo setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public String getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public ClusterInfo setKeepAliveTimeout(String keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
        return this;
    }

    public String getInstallPath() {
        return installPath;
    }

    public ClusterInfo setInstallPath(String installPath) {
        this.installPath = installPath;
        return this;
    }

    public String getLogPath() {
        return logPath;
    }

    public ClusterInfo setLogPath(String logPath) {
        this.logPath = logPath;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return EncryptUtil.isEncrypted(password) ? EncryptUtil.decryptPassInKylin(password) : password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public ClusterInfo(ClusterInfo cluster) {
        this.cluster = Maps.newHashMap(cluster.getCluster());
        this.keepAliveTimeout = cluster.getKeepAliveTimeout();
        this.socketTimeout = cluster.getKeepAliveTimeout();
        this.logPath = cluster.getLogPath();
        this.userName = cluster.getUserName();
        this.password = cluster.getPassword();
        this.installPath = cluster.getInstallPath();
    }

    public boolean emptyCluster() {
        return cluster == null || cluster.isEmpty();
    }

    public ClusterInfo() {
    }

}


