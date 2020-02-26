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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryClient;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.cluster.ClusterConstant;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.val;

@ConditionalOnZookeeperEnabled
@Component
public class ZookeeperClusterManager implements ClusterManager {

    @Autowired
    ZookeeperDiscoveryClient discoveryClient;

    @Override
    public String getLocalServer() {
        val instance = discoveryClient.getLocalServiceInstance();
        return instance.getHost() + ":" + instance.getPort();
    }

    private List<String> getQueryServers(String serviceId) {
        checkServiceId(serviceId);
        val list = discoveryClient.getInstances(serviceId);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        }
        return list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                .collect(Collectors.toList());
    }

    private void checkServiceId(String serviceId) {
        Preconditions.checkArgument(serviceId.trim().equals(ClusterConstant.QUERY)
                || serviceId.trim().equals(ClusterConstant.ALL) || serviceId.trim().equals(ClusterConstant.JOB));
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        List<ServerInfoResponse> servers = new ArrayList<>();
        for (val nodeType : Lists.newArrayList(ClusterConstant.ALL, ClusterConstant.QUERY)) {
            for (val host : getQueryServers(nodeType)) {
                ServerInfoResponse server = new ServerInfoResponse();
                server.setHost(host);
                server.setMode(nodeType);
                servers.add(server);
            }
        }
        return servers;
    }
}
