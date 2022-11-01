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
package org.apache.kylin.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.stereotype.Component;

import com.alibaba.cloud.nacos.ConditionalOnNacosDiscoveryEnabled;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@ConditionalOnNacosDiscoveryEnabled
@Component
@Slf4j
public class NacosClusterManager implements ClusterManager {

    public static final String QUERY = "yinglong-query-booter";
    public static final String DATA_LOADING = "yinglong-data-loading-booter";
    public static final String SMART = "yinglong-smart-booter";
    public static final String METADATA = "yinglong-common-booter";
    public static final String OPS = "yinglong-ops-booter";
    private static final List<String> SERVER_IDS = Arrays.asList(QUERY, DATA_LOADING, SMART, METADATA, OPS);

    private final Registration registration;

    @Autowired
    private DiscoveryClient discoveryClient;

    public NacosClusterManager(Registration registration) {
        this.registration = registration;
    }

    @Override
    public String getLocalServer() {
        return registration.getHost() + ":" + registration.getPort();
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return getServersByServerId(QUERY);
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return getServers();
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getServersByServerId(DATA_LOADING);
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        List<ServerInfoResponse> servers = new ArrayList<>();
        for (String serverId : SERVER_IDS) {
            servers.addAll(getServersByServerId(serverId));
        }
        return servers;
    }

    public List<ServerInfoResponse> getServersByServerId(String serverId) {
        String mode;
        switch (serverId) {
        case METADATA:
            mode = ClusterConstant.METADATA;
            break;
        case QUERY:
            mode = ClusterConstant.QUERY;
            break;
        case DATA_LOADING:
            mode = ClusterConstant.DATA_LOADING;
            break;
        case SMART:
            mode = ClusterConstant.SMART;
            break;
        case OPS:
            mode = ClusterConstant.OPS;
            break;
        default:
            throw new KylinRuntimeException(String.format("Unexpected serverId: {%s}", serverId));
        }

        List<ServerInfoResponse> servers = new ArrayList<>();
        List<ServiceInstance> instances = discoveryClient.getInstances(serverId);
        for (ServiceInstance instance : instances) {
            servers.add(new ServerInfoResponse(instance2ServerStr(instance), mode));
        }
        return servers;
    }

    private String instance2ServerStr(@Nonnull ServiceInstance serviceInstance) {
        Preconditions.checkNotNull(serviceInstance, "service instance is null");
        return serviceInstance.getHost() + ":" + serviceInstance.getPort();
    }
}
