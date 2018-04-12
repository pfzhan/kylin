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

package org.apache.kylin.metadata.cachesync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.NProjectManager;

/**
 * Broadcast metadata changes across all Kylin servers.
 * 
 * The origin server announce the event via Rest API to all Kylin servers including itself.
 * On target server, listeners are registered to process events. As part of processing, a 
 * listener can re-notify a new event to other local listeners.
 * 
 * A typical project schema change event:
 * - model is update on origin server, a "model" update event is announced
 * - on all servers, model listener is invoked, reload the model, and notify a "project_schema" update event
 * - all listeners respond to the "project_schema" update -- reload cube desc, clear project L2 cache, clear calcite data source etc
 */
public class Broadcaster {

    private static final Logger logger = LoggerFactory.getLogger(Broadcaster.class);

    public static final String SYNC_ALL = "all"; // the special entity to indicate clear all
    public static final String SYNC_PRJ_SCHEMA = "project_schema"; // the special entity to indicate project schema has change, e.g. table/model/cube_desc update
    public static final String SYNC_PRJ_DATA = "project_data"; // the special entity to indicate project data has change, e.g. cube/raw_table update
    public static final String SYNC_PRJ_ACL = "project_acl"; // the special entity to indicate query ACL has change, e.g. table_acl/learn_kylin update

    public static Broadcaster getInstance(KylinConfig config) {
        return config.getManager(Broadcaster.class);
    }

    // called by reflection
    static Broadcaster newInstance(KylinConfig config) {
        return new Broadcaster(config);
    }

    public interface RemoteServerStub {
        void notifyEvent(String entity, String event, String cacheKey) throws IOException;
    }

    // ============================================================================

    static final Map<String, List<Listener>> staticListenerMap = Maps.newConcurrentMap();

    private KylinConfig config;
    private BlockingDeque<BroadcastEvent> broadcastEvents = new LinkedBlockingDeque<>();
    private Map<String, List<Listener>> listenerMap = Maps.newConcurrentMap();
    private AtomicLong counter = new AtomicLong();

    private Broadcaster(final KylinConfig config) {
        this.config = config;
        final int retryLimitTimes = config.getCacheSyncRetrys();

        final String[] nodes = config.getRestServers();
        logger.debug(nodes.length + " nodes in the cluster: " + Arrays.toString(nodes));

        Executors.newSingleThreadExecutor(new DaemonThreadFactory()).execute(new Runnable() {
            @Override
            public void run() {
                final Map<String, RemoteServerStub> remoteMap = Maps.newHashMap();
                final ExecutorService wipingCachePool = new ThreadPoolExecutor(1, 10, 60L, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory());

                while (true) {
                    try {
                        final BroadcastEvent broadcastEvent = broadcastEvents.takeFirst();
                        broadcastEvent.setRetryTime(broadcastEvent.getRetryTime() + 1);
                        if (broadcastEvent.getRetryTime() > retryLimitTimes) {
                            logger.info("broadcastEvent retry up to limit times, broadcastEvent:{}", broadcastEvent);
                            continue;
                        }

                        refillRemoteMap(config, remoteMap);

                        logger.debug("Announcing new broadcast event: " + broadcastEvent);
                        for (final RemoteServerStub remote : remoteMap.values()) {
                            wipingCachePool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        remote.notifyEvent(broadcastEvent.getEntity(), broadcastEvent.getEvent(),
                                                broadcastEvent.getCacheKey());
                                    } catch (IOException e) {
                                        logger.warn("Thread failed during wipe cache at {}, error msg: {}",
                                                broadcastEvent, e);
                                        // when sync failed, put back to queue
                                        try {
                                            broadcastEvents.putLast(broadcastEvent);
                                        } catch (InterruptedException ex) {
                                            logger.warn(
                                                    "error reentry failed broadcastEvent to queue, broacastEvent:{}, error: {} ",
                                                    broadcastEvent, ex);
                                        }
                                    }
                                }
                            });
                        }
                    } catch (Exception e) {
                        logger.error("error broadcasting event", e);
                    }
                }
            }

            private void refillRemoteMap(KylinConfig config, Map<String, RemoteServerStub> remoteMap) {
                // in UT, notify local process
                if (config.isUTEnv()) {
                    if ((remoteMap.size() == 1 && remoteMap.containsKey("")) == false) {
                        remoteMap.clear();
                        remoteMap.put("", new RemoteServerStub() {
                            @Override
                            public void notifyEvent(String entity, String event, String cacheKey) throws IOException {
                                notifyListener(entity, Event.getEvent(event), cacheKey);
                            }
                        });
                    }
                    return;
                }

                // normal case, notify peer KAP servers including self
                String[] restServers = config.getRestServers();
                logger.debug("Servers in the cluster: " + Arrays.toString(restServers));
                remoteMap.keySet().retainAll(Arrays.asList(restServers));
                for (final String node : restServers) {
                    if (remoteMap.containsKey(node) == false) {
                        remoteMap.put(node, new RemoteServerStub() {
                            @Override
                            public void notifyEvent(String entity, String event, String cacheKey) throws IOException {
                                new RestClient(node).wipeCache(entity, event, cacheKey);
                            }
                        });
                    }
                }
            }
        });
    }

    // static listener survives cache wipe and goes after normal listeners
    public void registerStaticListener(Listener listener, String... entities) {
        doRegisterListener(staticListenerMap, listener, entities);
    }

    public void registerListener(Listener listener, String... entities) {
        doRegisterListener(listenerMap, listener, entities);
    }

    private static void doRegisterListener(Map<String, List<Listener>> lmap, Listener listener, String... entities) {
        synchronized (lmap) {
            // ignore re-registration
            List<Listener> all = lmap.get(SYNC_ALL);
            if (all != null && all.contains(listener)) {
                return;
            }

            for (String entity : entities) {
                if (!StringUtils.isBlank(entity))
                    addListener(lmap, entity, listener);
            }
            addListener(lmap, SYNC_ALL, listener);
            addListener(lmap, SYNC_PRJ_SCHEMA, listener);
            addListener(lmap, SYNC_PRJ_DATA, listener);
            addListener(lmap, SYNC_PRJ_ACL, listener);
        }
    }

    private static void addListener(Map<String, List<Listener>> lmap, String entity, Listener listener) {
        List<Listener> list = lmap.get(entity);
        if (list == null) {
            list = new ArrayList<>();
            lmap.put(entity, list);
        }
        list.add(listener);
    }

    public void notifyClearAll() throws IOException {
        notifyListener(SYNC_ALL, Event.UPDATE, SYNC_ALL);
    }

    public void notifyProjectSchemaUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_SCHEMA, Event.UPDATE, project);
    }

    public void notifyProjectDataUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_DATA, Event.UPDATE, project);
    }

    public void notifyProjectACLUpdate(String project) throws IOException {
        notifyListener(SYNC_PRJ_ACL, Event.UPDATE, project);
    }

    public void notifyListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, true);
    }

    public void notifyNonStaticListener(String entity, Event event, String cacheKey) throws IOException {
        notifyListener(entity, event, cacheKey, false);
    }

    private void notifyListener(String entity, Event event, String cacheKey, boolean includeStatic) throws IOException {
        // prevents concurrent modification exception
        List<Listener> list = Lists.newArrayList();
        List<Listener> l1 = listenerMap.get(entity); // normal listeners first
        if (l1 != null)
            list.addAll(l1);

        if (includeStatic) {
            List<Listener> l2 = staticListenerMap.get(entity); // static listeners second
            if (l2 != null)
                list.addAll(l2);
        }

        if (list.isEmpty())
            return;

        logger.debug("Broadcasting " + event + ", " + entity + ", " + cacheKey);

        switch (entity) {
        case SYNC_ALL:
            for (Listener l : list) {
                try {
                    l.onClearAll(this);
                } catch (Exception ex) {
                    logger.warn("Error during broadcasting " + event + ", " + entity + ", " + cacheKey, ex);
                }
            }
            config.clearManagers(); // clear all registered managers in config
            break;
        case SYNC_PRJ_SCHEMA:
            NProjectManager.getInstance(config).clearL2Cache();
            for (Listener l : list) {
                try {
                    l.onProjectSchemaChange(this, cacheKey);
                } catch (Exception ex) {
                    logger.warn("Error during broadcasting " + event + ", " + entity + ", " + cacheKey, ex);
                }
            }
            break;
        case SYNC_PRJ_DATA:
            NProjectManager.getInstance(config).clearL2Cache(); // cube's first becoming ready leads to schema change too
            for (Listener l : list) {
                try {
                    l.onProjectDataChange(this, cacheKey);
                } catch (Exception ex) {
                    logger.warn("Error during broadcasting " + event + ", " + entity + ", " + cacheKey, ex);
                }
            }
            break;
        case SYNC_PRJ_ACL:
            NProjectManager.getInstance(config).clearL2Cache();
            for (Listener l : list) {
                try {
                    l.onProjectQueryACLChange(this, cacheKey);
                } catch (Exception ex) {
                    logger.warn("Error during broadcasting " + event + ", " + entity + ", " + cacheKey, ex);
                }
            }
            break;
        default:
            for (Listener l : list) {
                try {
                    l.onEntityChange(this, entity, event, cacheKey);
                } catch (Exception ex) {
                    logger.warn("Error during broadcasting " + event + ", " + entity + ", " + cacheKey, ex);
                }
            }
            break;
        }

        logger.debug("Done broadcasting " + event + ", " + entity + ", " + cacheKey);
    }

    /**
     * Broadcast an event out
     */
    public void queue(String entity, String event, String key) {
        if (broadcastEvents == null)
            return;

        try {
            counter.incrementAndGet();
            broadcastEvents.putLast(new BroadcastEvent(entity, event, key));
        } catch (Exception e) {
            counter.decrementAndGet();
            logger.error("error putting BroadcastEvent", e);
        }
    }

    public long getCounterAndClear() {
        return counter.getAndSet(0);
    }

    public enum Event {

        CREATE("create"), UPDATE("update"), DROP("drop");
        private String text;

        Event(String text) {
            this.text = text;
        }

        public String getType() {
            return text;
        }

        public static Event getEvent(String event) {
            for (Event one : values()) {
                if (one.getType().equalsIgnoreCase(event)) {
                    return one;
                }
            }

            return null;
        }
    }

    abstract public static class Listener {
        public void onClearAll(Broadcaster broadcaster) throws IOException {
        }

        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onProjectQueryACLChange(Broadcaster broadcaster, String project) throws IOException {
        }

        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
        }
    }

    public static class BroadcastEvent {
        private int retryTime;
        private String entity;
        private String event;
        private String cacheKey;

        public BroadcastEvent(String entity, String event, String cacheKey) {
            super();
            this.entity = entity;
            this.event = event;
            this.cacheKey = cacheKey;
        }

        public int getRetryTime() {
            return retryTime;
        }

        public void setRetryTime(int retryTime) {
            this.retryTime = retryTime;
        }

        public String getEntity() {
            return entity;
        }

        public String getEvent() {
            return event;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((event == null) ? 0 : event.hashCode());
            result = prime * result + ((cacheKey == null) ? 0 : cacheKey.hashCode());
            result = prime * result + ((entity == null) ? 0 : entity.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            BroadcastEvent other = (BroadcastEvent) obj;
            if (!StringUtils.equals(event, other.event)) {
                return false;
            }
            if (!StringUtils.equals(cacheKey, other.cacheKey)) {
                return false;
            }
            if (!StringUtils.equals(entity, other.entity)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("entity", entity).add("event", event).add("cacheKey", cacheKey)
                    .toString();
        }

    }
}
