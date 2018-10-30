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

import static io.kyligence.kap.event.manager.EventManager.GLOBAL;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.event.handle.EventHandler;
import io.kyligence.kap.metadata.project.NProjectManager;

/**
 */
public class EventOrchestratorManager {

    private static final Logger logger = LoggerFactory.getLogger(EventOrchestratorManager.class);

    private JobLock jobLock;

    public volatile static EventOrchestratorManager instance;

    private static volatile Map<String, EventOrchestrator> INSTANCE_MAP = Maps.newConcurrentMap();

    public synchronized static EventOrchestratorManager getInstance(KylinConfig kylinConfig) {
        if (instance != null) {
            return instance;
        }
        instance = new EventOrchestratorManager();
        initEventOrchestrators(kylinConfig);
        return instance;
    }

    private static void initEventOrchestrators(KylinConfig kylinConfig) {
        String serverMode = kylinConfig.getServerMode();
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {
            logger.info("server mode: " + serverMode + ", no need to initEventOrchestrators");
            return;
        } else {
            logger.info("server mode: " + serverMode + ", start to initEventOrchestrators");
        }
        List<ProjectInstance> projects = NProjectManager.getInstance(kylinConfig).listAllProjects();
        for (ProjectInstance project : projects) {
            String projectName = project.getName();
            EventOrchestrator ret = INSTANCE_MAP.get(project.getName());
            if (ret == null) {
                ret = new EventOrchestrator(projectName, kylinConfig);
                INSTANCE_MAP.put(projectName, ret);
            }
        }
        // for global event, add project event ex...
        INSTANCE_MAP.put(GLOBAL, new EventOrchestrator(GLOBAL, kylinConfig));
    }

    public synchronized static void destroyInstance() {
        for (Map.Entry<String, EventOrchestrator> entry : INSTANCE_MAP.entrySet()) {
            logger.info("shutting down EventOrchestrator for {}", entry.getKey());
            entry.getValue().shutdown();
        }
        INSTANCE_MAP.clear();
        instance = null;
    }

    public synchronized void register(EventHandler handler) {
        for (Map.Entry<String, EventOrchestrator> entry : INSTANCE_MAP.entrySet()) {
            entry.getValue().register(handler);
        }
    }

    public synchronized void addProject(String project) {
        if (!INSTANCE_MAP.containsKey(project)) {
            EventOrchestrator eventOrchestrator = new EventOrchestrator(project, KylinConfig.getInstanceFromEnv());
            INSTANCE_MAP.put(project, eventOrchestrator);
            for (Map.Entry<Class<?>, CopyOnWriteArraySet<EventHandler>> handleSetEntry : INSTANCE_MAP.get(GLOBAL)
                    .getSubscribers().entrySet()) {
                for (EventHandler eventHandler : handleSetEntry.getValue()) {
                    eventOrchestrator.register(eventHandler);
                }
            }
        }
    }
}
