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

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.event.model.Event;

/**
 */
public class EventDao {

    private static final Serializer<Event> EVENT_SERIALIZER = new JsonSerializer<Event>(Event.class);
    private static final Logger logger = LoggerFactory.getLogger(EventDao.class);

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

    public List<Event> getEvents() {
        return store.getAllResources(resourceRootPath, EVENT_SERIALIZER);
    }

    //for UT
    public void deleteAllEvents() {
        List<Event> events = getEvents();
        for (Event event : events) {
            store.deleteResource(resourceRootPath + "/" + event.getUuid());
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

    public List<String> getAllEventPaths() {
        NavigableSet<String> resources = store.listResources(resourceRootPath);
        if (resources == null) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(resources);
    }
}
