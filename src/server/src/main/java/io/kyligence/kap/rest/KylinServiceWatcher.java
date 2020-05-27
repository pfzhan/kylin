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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.stereotype.Component;

@ConditionalOnZookeeperEnabled
@Component
public class KylinServiceWatcher {
    private static final Logger logger = LoggerFactory.getLogger(KylinServiceWatcher.class);

    @Autowired
    ServiceDiscovery<ZookeeperInstance> serviceDiscovery;

    private ServiceCache<ZookeeperInstance> allServiceCache;
    private ServiceCache<ZookeeperInstance> queryServiceCache;
    private ServiceCache<ZookeeperInstance> jobServiceCache;

    public KylinServiceWatcher(ServiceDiscovery<ZookeeperInstance> serviceDiscovery) throws Exception {
        // all server cache
        allServiceCache = createServiceCache(serviceDiscovery, "all");

        // query server cache
        queryServiceCache = createServiceCache(serviceDiscovery, "query");

        // job server cache
        jobServiceCache = createServiceCache(serviceDiscovery, "job");

        start();
    }

    private void start() throws Exception {
        allServiceCache.start();
        queryServiceCache.start();
        jobServiceCache.start();
    }

    private ServiceCache<ZookeeperInstance> createServiceCache(ServiceDiscovery<ZookeeperInstance> serviceDiscovery, String serverMode) {
        ServiceCache<ZookeeperInstance> serviceCache = serviceDiscovery.serviceCacheBuilder()
                .name(serverMode)
                .threadFactory(Executors.defaultThreadFactory())
                .build();

        serviceCache.addListener(new ServiceCacheListener() {
            @Override
            public void cacheChanged() {
                List<ServiceInstance<ZookeeperInstance>> instances = serviceCache.getInstances();
                List<String> nodes = getServerNodes(instances);
                System.setProperty("kylin.server.cluster-mode-" + serverMode, StringUtils.join(nodes, ","));
                logger.info("kylin.server.cluster-mode-{} update to {}", serverMode, nodes);
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                logger.info("zookeeper connection state changed to {}", connectionState);
            }
        });

        return serviceCache;
    }

    private List<String> getServerNodes(List<ServiceInstance<ZookeeperInstance>> instances) {
        if (instances != null) {
            return instances.stream().map(instance -> instance.getAddress() + ":" + instance.getPort()).collect(Collectors.toList());
        } else {
            return Lists.newArrayList();
        }
    }
}