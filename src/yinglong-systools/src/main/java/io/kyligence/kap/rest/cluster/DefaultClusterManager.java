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
package io.kyligence.kap.rest.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.common.util.ClusterConstant;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class DefaultClusterManager implements ClusterManager {

    int port;

    @Override
    public String getLocalServer() {
        try {
            return InetAddress.getLocalHost().getHostName() + ":" + port;
        } catch (UnknownHostException e) {
            log.warn("cannot get hostname", e);
            return "localhost:" + port;
        }
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        List<ServerInfoResponse> servers = new ArrayList<>();
        ServerInfoResponse server = new ServerInfoResponse();
        server.setHost(getLocalServer());
        server.setMode(ClusterConstant.ALL);
        servers.add(server);
        return servers;
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return getQueryServers();
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getQueryServers();
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return getQueryServers();
    }
}
