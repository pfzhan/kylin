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
package io.kyligence.kap.rest;

import static io.kyligence.kap.common.util.ClusterConstant.ServerModeEnum;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.stereotype.Component;

import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.discovery.KylinServiceDiscoveryCache;
import io.kyligence.kap.rest.discovery.KylinServiceDiscoveryClient;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class ZookeeperClusterManager implements ClusterManager {

    @Autowired
    private KylinServiceDiscoveryCache serviceCache;

    @Autowired
    private KylinServiceDiscoveryClient discoveryClient;

    public ZookeeperClusterManager() {
    }

    @Override
    public String getLocalServer() {
        return discoveryClient.getLocalServiceServer();
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.QUERY);
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return getServers();
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB);
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY);
    }

    private List<ServerInfoResponse> getServerByMode(@Nullable ServerModeEnum... serverModeEnum) {
        List<ServerInfoResponse> servers = new ArrayList<>();

        if (ArrayUtils.isEmpty(serverModeEnum)) {
            return servers;
        }

        for (val nodeModeType : serverModeEnum) {
            servers.addAll(serviceCache.getServerInfoByServerMode(nodeModeType));
        }
        return servers;
    }

}
