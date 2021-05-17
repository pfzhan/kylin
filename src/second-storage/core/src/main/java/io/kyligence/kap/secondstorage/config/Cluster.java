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


import java.util.ArrayList;
import java.util.List;

public class Cluster {
    private List<Node> nodes;
    private String socketTimeout;
    private String keepAliveTimeout;
    private String installPath;
    private String logPath;

    public List<Node> getNodes() {
        return nodes;
    }

    public Cluster setNodes(List<Node> nodes) {
        this.nodes = nodes;
        return this;
    }

    public String getSocketTimeout() {
        return socketTimeout;
    }

    public Cluster setSocketTimeout(String socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public String getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public Cluster setKeepAliveTimeout(String keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
        return this;
    }

    public String getInstallPath() {
        return installPath;
    }

    public Cluster setInstallPath(String installPath) {
        this.installPath = installPath;
        return this;
    }

    public String getLogPath() {
        return logPath;
    }

    public Cluster setLogPath(String logPath) {
        this.logPath = logPath;
        return this;
    }

    public Cluster(Cluster cluster) {
        this.nodes = new ArrayList<>();
        this.keepAliveTimeout = cluster.getKeepAliveTimeout();
        this.socketTimeout =cluster.getKeepAliveTimeout();
        cluster.getNodes().forEach(node -> this.getNodes().add(new Node(node)));
    }

    public Cluster() {}

}


