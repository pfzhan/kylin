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

import static io.kyligence.kap.common.util.AddressUtil.convertHost;
import static io.kyligence.kap.common.util.ClusterConstant.ServerModeEnum;
import static org.apache.kylin.common.ZookeeperConfig.geZKClientConnectionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.geZKClientSessionTimeoutMs;
import static org.apache.kylin.common.ZookeeperConfig.getZKConnectString;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryClient;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class ZookeeperClusterManager implements ClusterManager {

    private final ZookeeperDiscoveryClient discoveryClient;

    //<ServerMode,ServerInfoResponse>
    private final Map<ServerModeEnum, List<ServerInfoResponse>> serverInfoCachesMap;

    private final AsyncTaskExecutor executor;

    public ZookeeperClusterManager(ZookeeperDiscoveryClient discoveryClient, AsyncTaskExecutor executor)
            throws Exception {
        this.discoveryClient = discoveryClient;
        this.executor = executor;
        serverInfoCachesMap = new ConcurrentHashMap<>();

        initMonitorServer();
    }

    private void initMonitorServer() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(getZKConnectString(), geZKClientSessionTimeoutMs(),
                geZKClientConnectionTimeoutMs(), retryPolicy);

        client.start();
        monitorServer(ServerModeEnum.ALL, client);
        monitorServer(ServerModeEnum.JOB, client);
        monitorServer(ServerModeEnum.QUERY, client);
    }

    private void monitorServer(ServerModeEnum serverMode, CuratorFramework client) throws Exception {
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        String nodePath = "/kylin/" + identifier + "/services/" + serverMode.getName();

        val pathChildrenCache = new PathChildrenCache(client, nodePath, false);
        PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                switch (event.getType()) {
                case CONNECTION_LOST:
                case CONNECTION_SUSPENDED: {
                    log.debug("path listener on {} event:{}.", serverMode.getName(), event.toString());
                    break;
                }
                default: {
                    //avoid two event enter
                    synchronized (pathChildrenCache) {
                        log.debug("update service cache on {}, event:{}.", serverMode.getName(), event.toString());
                        updateServersCacheByMode(serverMode);
                    }
                    break;
                }
                }

            }
        };
        pathChildrenCache.getListenable().addListener(childrenCacheListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

    }

    private List<ServerInfoResponse> updateServersCacheByMode(ServerModeEnum serverMode) {
        serverInfoCachesMap.put(serverMode, getServerByMode(serverMode));
        return serverInfoCachesMap.get(serverMode);
    }

    @Override
    public String getLocalServer() {
        val instance = discoveryClient.getLocalServiceInstance();
        return instance.getHost() + ":" + instance.getPort();
    }

    private List<String> getServers(@Nonnull ServerModeEnum serverMode) {
        Preconditions.checkNotNull(serverMode, "server mode is null");
        String serverModeName = serverMode.getName();
        val timeout = KylinConfig.getInstanceFromEnv().getDiscoveryClientTimeoutThreshold();
        val list = submitWithTimeOut(() -> discoveryClient.getInstances(serverModeName), timeout);
        if (CollectionUtils.isEmpty(list)) {
            return Lists.newArrayList();
        } else {
            return list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.QUERY);
    }

    @Override
    public List<ServerInfoResponse> getServersFromCache() {
        return serverInfoCachesMap.values().stream().filter(CollectionUtils::isNotEmpty).flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB);
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        // please take care of this method when you want to add or remove a server mode
        return getServerByMode(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY);
    }

    private List<ServerInfoResponse> getServerByMode(ServerModeEnum... modeEnum) {
        List<ServerInfoResponse> servers = new ArrayList<>();
        for (val nodeType : modeEnum) {
            for (val host : getServers(nodeType)) {
                servers.add(new ServerInfoResponse(host, nodeType.getName()));
            }
        }
        return cleanServer(servers);
    }

    private List<ServerInfoResponse> cleanServer(List<ServerInfoResponse> servers) {
        return servers.stream()
                .sorted(Comparator.comparing(ServerInfoResponse::getHost))
                .collect(Collectors.toMap(server -> convertHost(server.getHost()), server -> server, (server1, server2) -> server2)).values()
                .stream().collect(Collectors.toList());
    }

    List<ServiceInstance> submitWithTimeOut(Callable<List<ServiceInstance>> callable, long timeout) {
        val task = executor.submit(callable);
        try {
            return task.get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("ZookeeperDiscoveryClient getInstances timeout:{}, error:", timeout, e);
        } catch (Exception e) {
            log.warn("ZookeeperDiscoveryClient getInstances error:", e);
        }
        return Lists.newArrayList();
    }

}
