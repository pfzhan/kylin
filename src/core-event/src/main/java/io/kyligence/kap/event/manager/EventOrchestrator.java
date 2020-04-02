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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.epoch.EpochManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.event.handle.EventHandler;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.Event;
import io.kyligence.kap.event.model.EventContext;
import io.kyligence.kap.event.model.JobRelatedEvent;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

/**
 */
public class EventOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(EventOrchestrator.class);

    private static final int MAX_RUN_TIMES = 5;

    private String project;
    private EventDao eventDao;
    private ScheduledExecutorService checkerPool;
    private KylinConfig kylinConfig;
    private long epochId;

    public EventOrchestrator(String project, KylinConfig kylinConfig) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing EventOrchestrator with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);

        Preconditions.checkNotNull(project);

        this.project = project;
        this.kylinConfig = kylinConfig;

        String serverMode = kylinConfig.getServerMode();
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {
            logger.info("server mode: " + serverMode + ", no need to run EventOrchestrator");
            return;
        }
        if (!kylinConfig.isUTEnv()) {
            this.epochId = EpochManager.getInstance(kylinConfig).getEpochId(project);
        }
        eventDao = EventDao.getInstance(kylinConfig, project);

        int pollSecond = kylinConfig.getEventPollIntervalSecond();
        logger.info("Fetching events every {} seconds", pollSecond);
        EventChecker checker = new EventChecker();
        checkerPool = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("EventChecker(project:" + project + ")"));
        checkerPool.scheduleWithFixedDelay(checker, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
    }

    protected class EventChecker implements Runnable {

        @Override
        synchronized public void run() {
            if (!kylinConfig.isUTEnv()) {
                if (!EpochManager.getInstance(kylinConfig).checkEpochId(epochId, project)) {
                    logger.warn("Thread" + Thread.currentThread().getName() + " may belong to last version epoch:" + epochId);
                    forceShutdown();
                }
            }
            List<Event> events = eventDao.getEvents();
            if (!UnitOfWork.GLOBAL_UNIT.equals(project))
                logger.debug("project {} contains {} events", project, events.size());
            Map<String, Event> eventsToBeProcessed = chooseEventForeachModel(events);
            for (Map.Entry<String, Event> eventsEntry : eventsToBeProcessed.entrySet()) {

                String modelId = eventsEntry.getKey();
                Event event = eventsEntry.getValue();

                val runTimes = event.getRunTimes();
                if (runTimes >= MAX_RUN_TIMES) {
                    handleEventError(modelId);
                    continue;
                }
                logger.trace("project: {}, model: {}, events to be processed: {}", project, modelId, event);

                try {
                    EventContext eventContext = new EventContext(event, kylinConfig, project);
                    EventHandler eventHandler = event.getEventHandler();
                    eventHandler.handle(eventContext);
                } catch (Exception e) {
                    // the exception should be rare, what what if it happens?
                    // the current approach will lead to repeatable handling + repeatable error msg
                    // can't think of a better way
                    logger.error("Failed to handle event: " + event, e);

                    // continue to handle next model's event
                }
            }
        }

        private void handleEventError(String modelId) {
            logger.warn("handling event error for model {}", modelId);
            UnitOfWork.doInTransactionWithRetry(() -> {
                val eventDao = EventDao.getInstance(KylinConfig.getInstanceFromEnv(), project);
                eventDao.deleteEventsByModel(modelId);
                val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                modelManager.updateDataModel(modelId,
                        copyForWrite -> copyForWrite.setBrokenReason(NDataModel.BrokenReason.EVENT));
                return null;
            }, project);
        }

        protected Map<String, Event> chooseEventForeachModel(List<Event> events) {
            Map<String, Event> map = Maps.newHashMap();
            if (CollectionUtils.isEmpty(events)) {
                return map;
            }

            events.sort(Event::compareTo);

            Map<String, List<Event>> modelEvents = events.stream()
                    .collect(Collectors.toMap(Event::getModelId, Lists::newArrayList, (one, other) -> {
                        one.addAll(other);
                        return one;
                    }));

            val execManager = NExecutableManager.getInstance(kylinConfig, project);
            val modelExecutables = execManager.getModelExecutables(modelEvents.keySet(),
                    ExecutableState::isNotProgressing);

            modelEvents.forEach((model, value) -> {
                val executableIds = modelExecutables.getOrDefault(model, Lists.newArrayList());
                val event = value.stream().filter(e -> CollectionUtils.isEmpty(executableIds)
                        || ((e instanceof AddSegmentEvent) && !executableIds.contains(((JobRelatedEvent) e).getJobId()) // to skip Post*Event
                )).findFirst().orElse(null);
                if (event != null) {
                    String groupKey = genGroupKey(event);
                    map.put(groupKey, event);
                }

            });

            return map;
        }

        private String genGroupKey(Event event) {
            String modelId = event.getModelId();
            Preconditions.checkState(!StringUtils.isBlank(modelId));
            return modelId;
        }
    }

    public void shutdown() {
        logger.info("Shutting down EventOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.shutdownGracefully(checkerPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down EventOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.forceShutdown(checkerPool);
    }

    public void fetchEventsImmediately() {
        checkerPool.schedule(new EventChecker(), 1, TimeUnit.SECONDS);
    }
}
