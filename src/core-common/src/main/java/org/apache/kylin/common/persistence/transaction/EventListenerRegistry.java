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
package org.apache.kylin.common.persistence.transaction;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;

import com.google.common.collect.Maps;

public class EventListenerRegistry {

    private final KylinConfig config;

    public static EventListenerRegistry getInstance(KylinConfig config) {
        return config.getManager(EventListenerRegistry.class);
    }

    static EventListenerRegistry newInstance(KylinConfig config) {
        return new EventListenerRegistry(config);
    }

    private Map<String, ResourceEventListener> eventListeners = Maps.newConcurrentMap();

    public EventListenerRegistry(KylinConfig config) {
        this.config = config;
    }

    public void onUpdate(ResourceCreateOrUpdateEvent event) {
        eventListeners.forEach((k, listener) -> {
            listener.onUpdate(config, event.getCreatedOrUpdated());
        });
    }

    public void onDelete(ResourceDeleteEvent event) {
        eventListeners.forEach((k, listener) -> {
            listener.onDelete(config, event.getResPath());
        });
    }

    public void register(ResourceEventListener eventListener, String name) {
        eventListeners.put(name, eventListener);
    }

    public interface ResourceEventListener {

        void onUpdate(KylinConfig config, RawResource rawResource);

        void onDelete(KylinConfig config, String resPath);
    }

}
