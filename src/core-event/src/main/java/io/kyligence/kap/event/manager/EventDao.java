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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.MergeSegmentEvent;
import io.kyligence.kap.event.model.RefreshSegmentEvent;
import lombok.val;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.kyligence.kap.event.model.Event;

/**
 */
public class EventDao {

    private static final Serializer<Event> EVENT_SERIALIZER = new JsonSerializer<Event>(Event.class);
    private static final Logger logger = LoggerFactory.getLogger(EventDao.class);

    protected static final Set<Class> buildJobRelatedEvent = new HashSet<>();

    static {
        buildJobRelatedEvent.add(AddCuboidEvent.class);
        buildJobRelatedEvent.add(AddSegmentEvent.class);
        buildJobRelatedEvent.add(MergeSegmentEvent.class);
        buildJobRelatedEvent.add(RefreshSegmentEvent.class);
    }

    public static EventDao getInstance(KylinConfig config, String project) {
        return config.getManager(project, EventDao.class);
    }

    // called by reflection
    static EventDao newInstance(KylinConfig config, String project) {
        return new EventDao(config, project);
    }

    // ============================================================================

    private ResourceStore store;
    private String project;
    private String resourceRootPath;

    private EventDao(KylinConfig config, String project) {
        logger.info("Using metadata url: " + config);
        this.store = ResourceStore.getKylinMetaStore(config);
        this.project = project;
        this.resourceRootPath = "/" + project + ResourceStore.EVENT_RESOURCE_ROOT;
    }

    private String pathOfEvent(Event event) {
        return pathOfEvent(event.getUuid());
    }

    public String pathOfEvent(String uuid) {
        return resourceRootPath + "/" + uuid;
    }

    private Event readEventResource(String path) {
        return store.getResource(path, EVENT_SERIALIZER);
    }

    private void writeEventResource(String path, Event event) {
        store.checkAndPutResource(path, event, EVENT_SERIALIZER);
    }

    private void updateEventResource(String path, Event update) {
        store.checkAndPutResource(path, update, EVENT_SERIALIZER);
    }

    private Event copyForWrite(Event event) {
        return (Event) SerializationUtils.clone(event);
    }

    public List<Event> getEvents() {
        return store.getAllResources(resourceRootPath, EVENT_SERIALIZER);
    }

    public List<Event> getEventsOrdered() {
        return getEvents().stream().sorted().collect(Collectors.toList());
    }

    //for UT
    public void deleteAllEvents() {
        List<Event> events = getEvents();
        for (Event event : events) {
            store.deleteResource(resourceRootPath + "/" + event.getUuid());
        }
    }

    public void deleteEventsByModel(String modelId) {
        List<Event> events = getEvents();
        for (Event event : events) {
            if (event.getModelId().equals(modelId)) {
                store.deleteResource(resourceRootPath + "/" + event.getUuid());
            }
        }
    }

    public void deleteEvent(String eventId) {
        store.deleteResource(resourceRootPath + "/" + eventId);
    }

    public List<Event> getEvents(long timeStart, long timeEndExclusive) {
        return store.getAllResources(resourceRootPath, timeStart, timeEndExclusive, EVENT_SERIALIZER);
    }

    public Event getEvent(String uuid) {
        return readEventResource(pathOfEvent(uuid));
    }

    public Event addEvent(Event event) {
        if (getEvent(event.getUuid()) != null) {
            throw new IllegalArgumentException("event id:" + event.getUuid() + " already exists");
        }
        writeEventResource(pathOfEvent(event), event);
        return event;
    }

    public List<Event> getJobRelatedEvents() {
        return getEvents().stream().filter(event -> buildJobRelatedEvent.contains(event.getClass()))
                .collect(Collectors.toList());
    }

    public List<Event> getJobRelatedEventsByModel(String modelId) {
        return getJobRelatedEvents().stream().filter(event -> event.getModelId().equals(modelId))
                .collect(Collectors.toList());
    }

    public void incEventRunTimes(Event event) {
        val copy = copyForWrite(event);
        copy.setRunTimes(event.getRunTimes() + 1);
        updateEventResource(pathOfEvent(copy.getUuid()), copy);
    }
}
