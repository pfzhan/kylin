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

package io.kyligence.kap.rest.broadcaster;

import static io.kyligence.kap.common.util.ClusterConstant.ServerModeEnum;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import io.kyligence.kap.tool.restclient.RestClient;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.AuditLogBroadcastEventNotifier;
import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.response.ServerInfoResponse;

public class Broadcaster implements Closeable {

    private ClusterManager clusterManager;

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    public static Broadcaster getInstance(KylinConfig config) {
        return config.getManager(Broadcaster.class);
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    private KylinConfig config;
    private ExecutorService announceThreadPool;
    private Map<String, RestClient> restClientMap = Maps.newHashMap();
    private BlockingQueue<BroadcastEvent> eventBlockingQueue = new LinkedBlockingDeque<>(1);

    private Broadcaster(final KylinConfig config) {
        this.config = config;
        this.announceThreadPool = new ThreadPoolExecutor(1, 10, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<>(),
                new DaemonThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());
    }

    private Set<String> getNodesByModes(ServerModeEnum... serverModeEnums) {
        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return Collections.emptySet();
        }

        Set<String> serverModeNameSets = Stream.of(serverModeEnums).filter(Objects::nonNull).map(ServerModeEnum::getName).collect(Collectors.toSet());
        if (clusterManager == null) {
            clusterManager = (ClusterManager) SpringContext.getApplicationContext().getBean("zookeeperClusterManager");
        }
        final List<ServerInfoResponse> nodes = clusterManager.getServersFromCache();
        Set<String> result = Sets.newHashSet();
        if (CollectionUtils.isEmpty(nodes)) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
        } else {
            result = nodes.stream()
                    .filter(node -> serverModeNameSets.contains(node.getMode()))
                    .map(ServerInfoResponse::getHost)
                    .collect(Collectors.toSet());
        }
        return result;
    }

    public void announce(BroadcastEvent event, BroadcastEventReadyNotifier notifier) {
        if (!eventBlockingQueue.offer(event))
            return;
        try {
            String identity = AddressUtil.getLocalInstance();
            Set<String> notifyNodes = getBroadcastNodes(notifier);
            CountDownLatch latch = new CountDownLatch(notifyNodes.size());
            for (String node : notifyNodes) {
                if (identity.equals(node) && notifier instanceof AuditLogBroadcastEventNotifier) {
                    latch.countDown();
                    continue;
                }
                RestClient client = restClientMap.get(node);
                if (client == null) {
                    client = new RestClient(node);
                    restClientMap.put(node, client);
                }
                RestClient finalClient = client;
                announceThreadPool.submit(() -> {
                    try {
                        logger.info("Broadcast to notify.");
                        finalClient.notify(notifier);
                    } catch (IOException e) {
                        logger.warn("Failed to notify.");
                    } finally {
                        latch.countDown();
                    }
                });
            }
            if (!latch.await(5, TimeUnit.SECONDS)) {
                logger.warn("Failed to broadcast due to timeout.");
            }
        } catch (Exception e) {
            logger.warn("failed to broadcast", e);
        } finally {
            eventBlockingQueue.clear();
        }
    }

    private Set<String> getBroadcastNodes(BroadcastEventReadyNotifier notifier) {
        switch (notifier.getBroadcastScope()) {
            case LEADER_NODES:
                return getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB);
            case ALL_NODES:
                return getNodesByModes(ServerModeEnum.ALL);
            case JOB_NODES:
                return getNodesByModes(ServerModeEnum.JOB);
            case QUERY_NODES:
                return getNodesByModes(ServerModeEnum.QUERY);
            default:
                return getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY);
        }
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

    public static class BroadcastEvent {
    }
}
