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
package io.kyligence.kap.common.persistence.transaction;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.persistence.event.EndUnit;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.event.StartUnit;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventSynchronization {

    private final KylinConfig config;
    private final LeaderInitiator leaderInitiator;
    private final EventListenerRegistry eventListener;
    @Getter
    private long eventSize = 0;

    private String currentKey = null;

    public static EventSynchronization getInstance(KylinConfig config) {
        return config.getManager(EventSynchronization.class);
    }

    static EventSynchronization newInstance(KylinConfig config) {
        return new EventSynchronization(config);
    }

    private EventSynchronization(KylinConfig config) {
        this.config = config;
        leaderInitiator = LeaderInitiator.getInstance(config);
        eventListener = EventListenerRegistry.getInstance(config);
    }

    public void replay(Event event) {
        replay(event, false);
    }

    public void replay(Event event, boolean locally) {
        // No need to replay in leader
        if (leaderInitiator.isLeader() && !locally) {
            return;
        }
        if (event instanceof StartUnit) {
            UnitOfWork.startTransaction(event.getKey(), false);
            currentKey = event.getKey();
            return;
        }
        if (event instanceof EndUnit) {
            UnitOfWork.get().unlock();
            currentKey = null;
            return;
        }

        if (currentKey != null)
            Preconditions.checkState(StringUtils.equals(currentKey, event.getKey()));

        if (event instanceof ResourceCreateOrUpdateEvent) {
            replayUpdate((ResourceCreateOrUpdateEvent) event);
            eventListener.onUpdate((ResourceCreateOrUpdateEvent) event);
        } else if (event instanceof ResourceDeleteEvent) {
            replayDelete((ResourceDeleteEvent) event);
            eventListener.onDelete((ResourceDeleteEvent) event);
        }
        if (event.isVital()) {
            eventSize++;
        }
    }

    private void replayDelete(ResourceDeleteEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        log.debug("replay delete {}", event.getResPath());
        resourceStore.deleteResource(event.getResPath());
    }

    private void replayUpdate(ResourceCreateOrUpdateEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        log.debug("replay update {}, {}", event.getResPath(), event.getCreatedOrUpdated().getMvcc());
        val raw = event.getCreatedOrUpdated();
        resourceStore.checkAndPutResource(raw.getResPath(), raw.getByteSource(), raw.getMvcc());
    }

}
