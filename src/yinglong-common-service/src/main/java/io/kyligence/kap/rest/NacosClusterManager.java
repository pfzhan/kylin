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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinRuntimeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.stereotype.Component;

import com.alibaba.cloud.nacos.ConditionalOnNacosDiscoveryEnabled;
import com.google.common.base.Preconditions;

import io.kyligence.kap.common.util.ClusterConstant;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnNacosDiscoveryEnabled
@Component
@Slf4j
public class NacosClusterManager implements ClusterManager {

    public static final String QUERY = "yinglong-query-booter";
    public static final String DATA_LOADING = "yinglong-data-loading-booter";
    public static final String SMART = "yinglong-smart-booter";
    public static final String METADATA = "yinglong-common-booter";
    private static final List<String> SERVER_IDS = Arrays.asList(QUERY, DATA_LOADING, SMART, METADATA);

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
        return Collections.emptyList();
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
