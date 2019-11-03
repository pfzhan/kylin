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

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventCreatedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.event.model.Event;

import java.util.UUID;

public class EventManager {

    private static final Logger logger = LoggerFactory.getLogger(EventManager.class);

    public static final String GLOBAL = "_global";
    private KylinConfig config;
    private String project;
    private EventDao eventDao;

    public static EventManager getInstance(KylinConfig config) {
        return config.getManager(GLOBAL, EventManager.class);
    }

    public static EventManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, EventManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static EventManager newInstance(KylinConfig conf, String project) {
        try {
            return new EventManager(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init EventManager from " + conf, e);
        }
    }

    public EventManager(KylinConfig cfg, final String project) {
        this.config = cfg;
        this.project = project;
        this.eventDao = EventDao.getInstance(config, project);
    }

    public void post(Event event) {
        if (event.isGlobal() && !GLOBAL.equals(project)) {
            // dispatch to global
            EventManager.getInstance(config).post(event);
        } else {
            eventDao.addEvent(event);
        }

        // dispatch event-created message out
        if (KylinConfig.getInstanceFromEnv().isUTEnv())
            SchedulerEventBusFactory.getInstance(config).postWithLimit(new EventCreatedNotifier(project));
        else
            UnitOfWork.get().doAfterUnit(
                () -> SchedulerEventBusFactory.getInstance(config).postWithLimit(new EventCreatedNotifier(project)));

        NMetricsGroup.counterInc(NMetricsName.EVENT_COUNTER, NMetricsCategory.PROJECT, project);
    }

    public String postAddSegmentEvents(NDataSegment newSegment, String modelId, String userName) {
        String jobId = UUID.randomUUID().toString();

        AddSegmentEvent addSegmentEvent = new AddSegmentEvent();
        addSegmentEvent.setSegmentId(newSegment.getId());
        addSegmentEvent.setModelId(modelId);
        addSegmentEvent.setJobId(jobId);
        addSegmentEvent.setOwner(userName);
        post(addSegmentEvent);

        PostAddSegmentEvent postAddSegmentEvent = new PostAddSegmentEvent();
        postAddSegmentEvent.setSegmentId(newSegment.getId());
        postAddSegmentEvent.setModelId(modelId);
        postAddSegmentEvent.setJobId(addSegmentEvent.getJobId());
        postAddSegmentEvent.setOwner(userName);
        post(postAddSegmentEvent);

        return jobId;
    }

    public String postAddCuboidEvents(String modelId, String userName) {
        String jobId = UUID.randomUUID().toString();

        AddCuboidEvent addCuboidEvent = new AddCuboidEvent();
        addCuboidEvent.setModelId(modelId);
        addCuboidEvent.setJobId(jobId);
        addCuboidEvent.setOwner(userName);
        post(addCuboidEvent);

        PostAddCuboidEvent postAddCuboidEvent = new PostAddCuboidEvent();
        postAddCuboidEvent.setModelId(modelId);
        postAddCuboidEvent.setJobId(addCuboidEvent.getJobId());
        postAddCuboidEvent.setOwner(userName);
        post(postAddCuboidEvent);

        return jobId;
    }
}
