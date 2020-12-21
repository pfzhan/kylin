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

package io.kyligence.kap.rest.discovery;

import static io.kyligence.kap.common.util.ClusterConstant.ServerModeEnum;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.zookeeper.ConditionalOnZookeeperEnabled;
import org.springframework.cloud.zookeeper.discovery.ZookeeperInstance;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.val;

@ConditionalOnZookeeperEnabled
@Component
public class KylinServiceDiscoveryCache implements KylinServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(KylinServiceDiscoveryCache.class);

    @Autowired
    private KylinServiceDiscoveryClient kylinServiceDiscoveryClient;

    private final Map<ServerModeEnum, ServiceCache<ZookeeperInstance>> serverModeCacheMap;

    private static final Callback UPDATE_ALL_EPOCHS = () -> {
        try {
            EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).updateAllEpochs();
        } catch (Exception e) {
            logger.error("UpdateAllEpochs failed", e);
        }
    };

    public KylinServiceDiscoveryCache(ServiceDiscovery<ZookeeperInstance> serviceDiscovery) throws Exception {
        serverModeCacheMap = Maps.newHashMap();
        // all server cache
        serverModeCacheMap.put(ServerModeEnum.ALL,
                createServiceCache(serviceDiscovery, ServerModeEnum.ALL, UPDATE_ALL_EPOCHS));

        // query server cache
        serverModeCacheMap.put(ServerModeEnum.QUERY, createServiceCache(serviceDiscovery, ServerModeEnum.QUERY, () -> {

        }));

        // job server cache
        serverModeCacheMap.put(ServerModeEnum.JOB,
                createServiceCache(serviceDiscovery, ServerModeEnum.JOB, UPDATE_ALL_EPOCHS));

        start();
    }

    private void start() throws Exception {
        for (ServiceCache<ZookeeperInstance> serviceCache : serverModeCacheMap.values()) {
            serviceCache.start();
        }
    }

    private ServiceCache<ZookeeperInstance> createServiceCache(ServiceDiscovery<ZookeeperInstance> serviceDiscovery,
            ServerModeEnum serverMode, Callback action) {
        ServiceCache<ZookeeperInstance> serviceCache = serviceDiscovery.serviceCacheBuilder().name(serverMode.getName())
                .threadFactory(Executors.defaultThreadFactory()).build();

        serviceCache.addListener(new ServiceCacheListener() {
            @Override
            public void cacheChanged() {
                List<String> serverNodes = getServerStrByServerMode(serverMode);
                Unsafe.setProperty("kylin.server.cluster-mode-" + serverMode.getName(),
                        StringUtils.join(serverNodes, ","));
                logger.info("kylin.server.cluster-mode-{} update to {}", serverMode.getName(), serverNodes);

                // current node is active all/job nodes, try to update all epochs
                if (getServerInfoByServerMode(ServerModeEnum.JOB).stream().map(ServerInfoResponse::getHost).anyMatch(
                        server -> Objects.equals(server, kylinServiceDiscoveryClient.getLocalServiceServer()))) {
                    logger.debug("Current node is active node, try to update all epochs");
                    action.action();
                }
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                logger.info("zookeeper connection state changed to {}", connectionState);
            }
        });

        return serviceCache;
    }

    private ServiceCache<ZookeeperInstance> getServiceCacheByMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null");

        val serviceCache = serverModeCacheMap.get(serverModeEnum);

        Preconditions.checkNotNull(serviceCache, "cannot find the server cache :" + serverModeEnum.getName());

        return serviceCache;
    }

    private List<String> getServerStrByServerMode(@Nonnull ServerModeEnum serverModeEnum) {
        Preconditions.checkNotNull(serverModeEnum, "server mode is null!");

        return getServiceCacheByMode(serverModeEnum).getInstances().stream()
                .map(KylinServiceDiscoveryCache::instance2ServerStr).collect(Collectors.toList());
    }

    @Override
    public List<ServerInfoResponse> getServerInfoByServerMode(@Nullable ServerModeEnum... serverModeEnums) {
        List<ServerInfoResponse> serverInfoResponses = Lists.newArrayList();
        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return serverInfoResponses;
        }

        for (ServerModeEnum serverModeEnum : serverModeEnums) {
            serverInfoResponses.addAll(getServiceCacheByMode(serverModeEnum).getInstances().stream()
                    .map(serviceIns -> new ServerInfoResponse(instance2ServerStr(serviceIns), serverModeEnum.getName()))
                    .collect(Collectors.toList()));
        }

        return serverInfoResponses;
    }

    private static String instance2ServerStr(@Nonnull ServiceInstance<ZookeeperInstance> serviceInstance) {
        Preconditions.checkNotNull(serviceInstance, "service instance is null");

        return serviceInstance.getAddress() + ":" + serviceInstance.getPort();
    }
}