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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.epoch.EpochManager;
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

    List<ServerInfoResponse> cache;

    public ZookeeperClusterManager(ZookeeperDiscoveryClient discoveryClient) throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(ZookeeperUtil.getZKConnectString(), 120000, 15000, retryPolicy);
        client.start();
        String identifier = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
        String nodePath = "/kylin/" + identifier + "/services/all";
        val pathChildrenCache = new PathChildrenCache(client, nodePath, false);
        PathChildrenCacheListener childrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                cache = getQueryServers();
            }
        };
        pathChildrenCache.getListenable().addListener(childrenCacheListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }


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
        List<String> hosts = list.stream().map(serviceInstance -> serviceInstance.getHost() + ":" + serviceInstance.getPort())
                .collect(Collectors.toList());
        if (!Objects.equals(serviceId, ClusterConstant.QUERY)) {
            mergeJobNodeWithEpoch(hosts);
        }
        return hosts;
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
                servers.add(new ServerInfoResponse(host, nodeType));
            }
        }
        return servers;
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
        List<ServerInfoResponse> servers = new ArrayList<>();
        Set<String> hosts = Sets.newHashSet();
        for (val nodeType : Lists.newArrayList(ClusterConstant.ALL, ClusterConstant.JOB)) {
            for (val host : getQueryServers(nodeType)) {
                hosts.add(host);
                servers.add(new ServerInfoResponse(host, nodeType));
            }
        }

        return servers;
    }

    private void mergeJobNodeWithEpoch(List<String> hosts) {
        val ips = hosts.stream().map(s -> {
            val hostAndPort = s.split(":");
            String host = hostAndPort[0];
            String port = hostAndPort[1];
            try {
                return InetAddress.getByName(host).getHostAddress() + ":" + port;
            } catch (UnknownHostException e) {
                return "127.0.0.1:" + port;
            }
        }).collect(Collectors.toList());
        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String leader : epochManager.getAllLeaders()) {
            if (!ips.contains(leader)) {
                hosts.add(leader);
            }
        }
    }

}
