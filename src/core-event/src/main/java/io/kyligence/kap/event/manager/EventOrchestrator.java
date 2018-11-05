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

package io.kyligence.kap.event.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.event.handle.EventHandler;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.EventStatus;
import io.kyligence.kap.shaded.influxdb.com.google.common.common.base.MoreObjects;
import io.kyligence.kap.shaded.influxdb.com.google.common.common.collect.Sets;
import lombok.val;

/**
 */
public class EventOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(EventOrchestrator.class);

    public static final String DEFAULT_GROUP = "defaultGroup";
    private String project;
    private EventDao eventDao;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService eventProcessPool;
    private final ConcurrentMap<Class<?>, CopyOnWriteArraySet<EventHandler>> subscribers = Maps.newConcurrentMap();
    private KylinConfig kylinConfig;

    private Set<String> blackList = Sets.newConcurrentHashSet();

    public EventOrchestrator(String project, KylinConfig kylinConfig) {
        Preconditions.checkNotNull(project);

        this.project = project;
        this.kylinConfig = kylinConfig;
        init();
    }

    protected class FetcherRunner implements Runnable {

        @Override
        synchronized public void run() {
            try {

                List<Future> futures = new ArrayList<>();
                List<Event> events = eventDao.getEvents();
                logger.debug("project {}: events {}", project, events);
                Map<String, List<EventSetManager>> eventsToBeProcessed = sectionalizeEvents(events);
                List<EventSetManager> eventSetManagers;
                for (Map.Entry<String, List<EventSetManager>> eventsEntry : eventsToBeProcessed.entrySet()) {
                    logger.debug("project {}: tobe processsed {}: {}", project, eventsEntry.getKey(), eventsEntry
                            .getValue().stream().flatMap(s -> s.getEvents().stream()).collect(Collectors.toList()));
                    eventSetManagers = eventsEntry.getValue();
                    futures.add(eventProcessPool.submit(new EventWorker(eventSetManagers)));
                }

                waitForEventFinished(futures);
            } catch (Exception e) {
                logger.error("Event Fetcher caught a exception ", e);
            }
        }

        protected Map<String, List<EventSetManager>> sectionalizeEvents(List<Event> events) {
            Map<String, List<EventSetManager>> eventSetGroup = Maps.newHashMap();
            if (CollectionUtils.isEmpty(events)) {
                return eventSetGroup;
            }
            filterEvents(events);
            if (CollectionUtils.isEmpty(events)) {
                return eventSetGroup;
            }
            Collections.sort(events, new Comparator<Event>() {
                @Override
                public int compare(Event o1, Event o2) {
                    if (o1.getCreateTimeNanosecond() <= o2.getCreateTimeNanosecond()) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            });

            String groupKey;
            for (Event event : events) {
                if (event.getStatus().equals(EventStatus.SUCCEED) || event.getStatus().equals(EventStatus.ERROR)
                        || !event.isApproved()) {
                    continue;
                }
                groupKey = genGroupKey(event);

                List<EventSetManager> eventSetManagerList = eventSetGroup.get(groupKey);
                if (CollectionUtils.isEmpty(eventSetManagerList)) {
                    eventSetManagerList = Lists.newArrayList();
                    eventSetGroup.put(groupKey, eventSetManagerList);
                }
                addToEventSetManager(eventSetManagerList, event);
            }
            return eventSetGroup;
        }

        private void addToEventSetManager(List<EventSetManager> eventSetManagerList, Event event) {
            EventSetManager eventSetManager = null;
            int lastIndex = eventSetManagerList.size() - 1;
            if (lastIndex >= 0) {
                eventSetManager = eventSetManagerList.get(lastIndex);
            }

            if (eventSetManager != null && event.getClass().equals(eventSetManager.getEventClassType())
                    && event.isParallel()) {
                eventSetManager.addEvent(event);
            } else {
                eventSetManager = new EventSetManager();
                eventSetManager.addEvent(event);
                eventSetManager.setEventClassType(event.getClass());
                eventSetManagerList.add(eventSetManager);
            }

        }

        private void filterEvents(List<Event> events) {
            Iterator<Event> iterator = events.iterator();
            Event event;
            while (iterator.hasNext()) {
                event = iterator.next();
                String groupKey = genGroupKey(event);
                val finalEvent = event;
                if (event.getStatus().equals(EventStatus.SUCCEED) || !event.isApproved()
                        || blackList.contains(groupKey)) {
                    iterator.remove();
                }
            }
        }

        private void waitForEventFinished(List<Future> futures) throws ExecutionException, InterruptedException {
            if (CollectionUtils.isNotEmpty(futures)) {
                for (Future future : futures) {
                    future.get();
                }
            }
        }

    }

    private String genGroupKey(Event event) {
        StringBuilder key = new StringBuilder();
        String modelName = event.getModelName();
        if (StringUtils.isBlank(modelName)) {
            return DEFAULT_GROUP;
        }
        key.append(modelName);
        key.append("_");
        key.append(event.getCubePlanName());

        return key.toString();
    }

    private class EventWorker implements Runnable {

        private final List<EventSetManager> eventSetManagers;

        public EventWorker(List<EventSetManager> eventSetManagers) {
            this.eventSetManagers = eventSetManagers;
        }

        @Override
        public void run() {
            try {
                for (EventSetManager eventSetManager : eventSetManagers) {
                    if (!eventSetManager.execute()) {
                        return;
                    }
                }
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("EventWorker eventList error : " + e.getMessage(), e);
            }
        }
    }

    private class EventRunner implements Runnable {

        private final Event event;

        public EventRunner(Event event) {
            this.event = event;
        }

        @Override
        public void run() {
            try {
                CopyOnWriteArraySet<EventHandler> eventHandlers = subscribers.get(event.getClass());
                logger.info("EventRunner event:" + event.getId() + " running....");

                if (CollectionUtils.isNotEmpty(eventHandlers)) {
                    EventContext eventContext = new EventContext(event, kylinConfig);
                    for (EventHandler eventHandler : eventHandlers) {
                        eventHandler.handle(eventContext);
                    }
                } else {
                    // TODO process dead event
                }

            } catch (Exception e) {
                logger.error("EventRunner eventList error : " + e.getMessage(), e);
            }
        }
    }

    public void init() {
        String serverMode = kylinConfig.getServerMode();
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {
            logger.info("server mode: " + serverMode + ", no need to run EventOrchestrator");
            return;
        }
        logger.info("Initializing EventOrchestrator for project {} ....", project);

        eventDao = EventDao.getInstance(kylinConfig, project);

        fetcherPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("EventOrchestratorFetchPool"));
        int corePoolSize = kylinConfig.getMaxConcurrentJobLimit();
        eventProcessPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>(), new NamedThreadFactory("EventOrchestratorPool"));

        int pollSecond = kylinConfig.getEventPollIntervalSecond();
        logger.info("Fetching events every {} seconds", pollSecond);
        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, pollSecond, pollSecond, TimeUnit.SECONDS);
    }

    public void shutdown() {
        logger.info("Shutting down EventOrchestrator ....");
        if (fetcherPool != null)
            ExecutorServiceUtil.shutdownGracefully(fetcherPool, 60);
        if (eventProcessPool != null)
            ExecutorServiceUtil.shutdownGracefully(eventProcessPool, 60);
    }

    public synchronized void register(EventHandler handler) {
        Class<?> eventClassType = handler.getEventClassType();
        CopyOnWriteArraySet<EventHandler> eventHandlers = subscribers.get(eventClassType);
        if (eventHandlers == null) {
            CopyOnWriteArraySet<EventHandler> newSet = new CopyOnWriteArraySet<>();
            eventHandlers = MoreObjects.firstNonNull(subscribers.putIfAbsent(eventClassType, newSet), newSet);
        }
        if (!eventHandlers.contains(handler)) {
            eventHandlers.add(handler);
        }
    }

    public ConcurrentMap<Class<?>, CopyOnWriteArraySet<EventHandler>> getSubscribers() {
        return subscribers;
    }

    protected class EventSetManager {

        private Class<?> eventClassType;
        private List<Event> events = Lists.newArrayList();

        public Class<?> getEventClassType() {
            return eventClassType;
        }

        public void setEventClassType(Class<?> eventClassType) {
            this.eventClassType = eventClassType;
        }

        public void addEvent(Event event) {
            this.events.add(event);
        }

        public boolean execute() throws ExecutionException, InterruptedException, PersistentException {
            if (CollectionUtils.isEmpty(events)) {
                return true;
            }
            List<Future> futureList = new ArrayList<>();
            for (Event event : events) {
                futureList.add(eventProcessPool.submit(new EventRunner(event)));
            }

            for (Future future : futureList) {
                future.get();
            }

            // if the stats of event is running and the related job stats is error
            // return false, the workflow of this eventSetManager list will not continue
            for (Event event : events) {
                Event updatedEvent = eventDao.getEvent(event.getUuid());
                EventStatus status = updatedEvent.getStatus();
                if (EventStatus.RUNNING.equals(status)) {
                    String jobId = updatedEvent.getJobId();
                    if (StringUtils.isNotBlank(jobId)) {
                        AbstractExecutable job = NExecutableManager.getInstance(kylinConfig, event.getProject())
                                .getJob(jobId);
                        if (job != null) {
                            ExecutableState jobStatus = job.getStatus();
                            if (ExecutableState.ERROR.equals(jobStatus)) {
                                blackList.add(genGroupKey(event));
                            }
                        }
                    }
                    return false;
                }
            }

            return true;
        }

        public List<Event> getEvents() {
            return events;
        }

    }

    public void cleanBlackList() {
        blackList.clear();
    }

}
