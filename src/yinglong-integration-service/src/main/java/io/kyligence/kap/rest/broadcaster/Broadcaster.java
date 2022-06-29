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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.BroadcastListener;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import io.kyligence.kap.tool.restclient.RestClient;

public class Broadcaster implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    private KylinConfig config;
    private ClusterManager clusterManager;

    private ExecutorService eventPollExecutor;
    private ExecutorService eventHandlerExecutor;
    private BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<BroadcastEventReadyNotifier> eventQueue = new LinkedBlockingQueue<>();

    private BroadcastListener localHandler;
    private ConcurrentHashMap<String, RestClient> restClientMap = new ConcurrentHashMap<>();

    public static Broadcaster getInstance(KylinConfig config, BroadcastListener localHandler) {
        Broadcaster broadcaster = config.getManager(Broadcaster.class);
        broadcaster.localHandler = localHandler;
        return broadcaster;
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    private Broadcaster(final KylinConfig config) {
        this.config = config;
        this.eventHandlerExecutor = new ThreadPoolExecutor(10, 10, 60L, TimeUnit.SECONDS, runnableQueue,
                new DaemonThreadFactory("BroadcastEvent-handler"), new ThreadPoolExecutor.DiscardPolicy());

        this.eventPollExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("BroadcastEvent-poll"));
        eventPollExecutor.submit(this::consumeEvent);
    }

    public void announce(BroadcastEventReadyNotifier event) {
        if (eventQueue.contains(event)) {
            logger.debug("broadcast event queue has contain this event: {}", event);
            return;
        }
        eventQueue.offer(event);
    }

    public void consumeEvent() {
        try {
            while (true) {
                BroadcastEventReadyNotifier notifier = eventQueue.take();
                handleEvent(notifier);
            }
        } catch (InterruptedException e) {
            logger.error("consume broadcast event fail: ", e);
        }
    }

    private void handleEvent(BroadcastEventReadyNotifier notifier) {
        try {
            Set<String> notifyNodes = getBroadcastNodes(notifier);
            if (notifyNodes.isEmpty()) {
                logger.debug("no need broadcast the event {} to other node.", notifier);
                return;
            }

            CountDownLatch latch = new CountDownLatch(notifyNodes.size());
            String identity = AddressUtil.getLocalInstance();
            for (String node : notifyNodes) {

                eventHandlerExecutor.submit(() -> {
                    try {
                        if (identity.equals(node)) {
                            localHandle(notifier);
                        } else {
                            remoteHandle(node, notifier);
                        }
                        logger.info("Broadcast to {} notify.", node);
                    } catch (IOException e) {
                        logger.warn("Failed to notify.", e);
                    } finally {
                        latch.countDown();
                    }
                });

            }
            if (!latch.await(5, TimeUnit.SECONDS)) {
                logger.warn("Failed to broadcast due to timeout. current BroadcastEvent-handler task num {}",
                        runnableQueue.size());
            }

        } catch (Exception e) {
            logger.warn("failed to broadcast", e);
        }
    }

    private void localHandle(BroadcastEventReadyNotifier notifier) throws IOException {
        localHandler.handle(notifier);
    }

    private void remoteHandle(String node, BroadcastEventReadyNotifier notifier) throws IOException {
        RestClient client = restClientMap.computeIfAbsent(node, k -> new RestClient(node));
        client.notify(notifier);
    }

    private Set<String> getBroadcastNodes(BroadcastEventReadyNotifier notifier) {
        Set<String> nodes;
        switch (notifier.getBroadcastScope()) {
        case LEADER_NODES:
            nodes = getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.METADATA);
            break;
        case ALL_NODES:
            nodes = getNodesByModes(ServerModeEnum.ALL);
            break;
        case JOB_NODES:
            nodes = getNodesByModes(ServerModeEnum.JOB, ServerModeEnum.DATA_LOADING);
            break;
        case QUERY_NODES:
            nodes = getNodesByModes(ServerModeEnum.QUERY);
            break;
        case QUERY_AND_ALL:
            nodes = getNodesByModes(ServerModeEnum.QUERY, ServerModeEnum.ALL);
            break;
        default:
            nodes = getNodesByModes(ServerModeEnum.ALL, ServerModeEnum.JOB, ServerModeEnum.QUERY,
                    ServerModeEnum.DATA_LOADING, ServerModeEnum.METADATA, ServerModeEnum.SMART);
        }
        if (!notifier.needBroadcastSelf()) {
            String identity = AddressUtil.getLocalInstance();
            return nodes.stream().filter(node -> !node.equals(identity)).collect(Collectors.toSet());
        }
        return nodes;
    }

    private Set<String> getNodesByModes(ServerModeEnum... serverModeEnums) {
        if (ArrayUtils.isEmpty(serverModeEnums)) {
            return Collections.emptySet();
        }

        Set<String> serverModeNameSets = Stream.of(serverModeEnums).filter(Objects::nonNull)
                .map(ServerModeEnum::getName).collect(Collectors.toSet());
        if (clusterManager == null) {
            clusterManager = SpringContext.getApplicationContext().getBean(ClusterManager.class);
        }
        final List<ServerInfoResponse> nodes = clusterManager.getServersFromCache();
        Set<String> result = Sets.newHashSet();
        if (CollectionUtils.isEmpty(nodes)) {
            logger.warn("There is no available rest server; check the 'kylin.server.cluster-servers' config");
        } else {
            result = nodes.stream().filter(node -> serverModeNameSets.contains(node.getMode()))
                    .map(ServerInfoResponse::getHost).collect(Collectors.toSet());
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }
}
