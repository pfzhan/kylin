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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.ClusterConstant;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.ZookeeperUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperDiscoveryClient;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.kyligence.kap.common.util.AddressUtil.convertHost;

@ConditionalOnZookeeperEnabled
@Component
@Slf4j
public class ZookeeperClusterManager implements ClusterManager {

    @Autowired
    ZookeeperDiscoveryClient discoveryClient;

    List<ServerInfoResponse> cache;

    @Autowired
    AsyncTaskExecutor executor;

    public ZookeeperClusterManager(ZookeeperDiscoveryClient discoveryClient) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(ZookeeperUtil.getZKConnectString(), 120000, 15000, retryPolicy);
        client.start();
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        String nodePath = "/kylin/" + identifier + "/services/all";
        try (val pathChildrenCache = new PathChildrenCache(client, nodePath, false)) {
            PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                    cache = getQueryServers();
                }
            };
            pathChildrenCache.getListenable().addListener(childrenCacheListener);
            pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        }
    }


    @Override
    public String getLocalServer() {
        val instance = discoveryClient.getLocalServiceInstance();
        return instance.getHost() + ":" + instance.getPort();
    }

    private List<String> getServers(String serverMode) {
        checkServerMode(serverMode);
        val list = submitWithTimeOut(() -> discoveryClient.getInstances(serverMode), 3);
        if (CollectionUtils.isEmpty(list)) {
            log.warn("Failed to get servers info from zk");
            return Lists.newArrayList();
        } else {
            return list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                    .collect(Collectors.toList());
        }
    }

    private void checkServerMode(String serverMode) {
        Preconditions.checkArgument(serverMode.trim().equals(ClusterConstant.QUERY)
                || serverMode.trim().equals(ClusterConstant.ALL) || serverMode.trim().equals(ClusterConstant.JOB));
    }

    @Override
    public List<ServerInfoResponse> getQueryServers() {
        return getServerByMode(ClusterConstant.ALL, ClusterConstant.QUERY);
    }

    @Override
    public List<ServerInfoResponse> getQueryServersFromCache() {
        try {
            if (CollectionUtils.isNotEmpty(cache))
                return cache;
            else return getQueryServers();
        } catch (Exception e) {
            return getQueryServers();
        }
    }

    @Override
    public List<ServerInfoResponse> getJobServers() {
        return getServerByMode(ClusterConstant.ALL, ClusterConstant.JOB);
    }

    @Override
    public List<ServerInfoResponse> getServers() {
        // please take care of this method when you want to add or remove a server mode
        return getServerByMode(ClusterConstant.ALL, ClusterConstant.JOB, ClusterConstant.QUERY);
    }

    private List<ServerInfoResponse> getServerByMode(String... mode) {
        List<ServerInfoResponse> servers = new ArrayList<>();
        for (val nodeType : mode) {
            for (val host : getServers(nodeType)) {
                servers.add(new ServerInfoResponse(host, nodeType));
            }
        }
        return cleanServer(servers);
    }

    private void mergeJobNodeWithEpoch(List<String> hosts, String serverMode) {
        val ips = hosts.stream().map(host -> convertHost(host)).collect(Collectors.toList());
        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String leader : epochManager.getAllLeadersByMode(serverMode)) {
            if (!ips.contains(leader)) {
                hosts.add(leader);
            }
        }
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
        } catch (Exception e) {
            log.warn("ZookeeperDiscoveryClient getInstances error {}", e.getMessage(), e);
        }
        return Lists.newArrayList();
    }

}
