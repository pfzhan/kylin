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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import io.kyligence.kap.common.cluster.LeaderInitiator;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageSynchronization {

    private final KylinConfig config;
    private final LeaderInitiator leaderInitiator;
    private final EventListenerRegistry eventListener;
    @Getter
    private long eventSize = 0;

    public static MessageSynchronization getInstance(KylinConfig config) {
        return config.getManager(MessageSynchronization.class);
    }

    static MessageSynchronization newInstance(KylinConfig config) {
        return new MessageSynchronization(config);
    }

    private MessageSynchronization(KylinConfig config) {
        this.config = config;
        leaderInitiator = LeaderInitiator.getInstance(config);
        eventListener = EventListenerRegistry.getInstance(config);
    }

    public void replay(UnitMessages event) {
        replay(event, false);
    }

    public void replay(UnitMessages messages, boolean locally) {
        // No need to replay in leader
        if (leaderInitiator.isLeader() && !locally) {
            return;
        }
        if (!locally) {
            UnitOfWork.startTransaction(messages.getKey(), false, false);
        }
        messages.getMessages().forEach(event -> {
            if (event instanceof ResourceCreateOrUpdateEvent) {
                replayUpdate((ResourceCreateOrUpdateEvent) event);
                eventListener.onUpdate((ResourceCreateOrUpdateEvent) event);
            } else if (event instanceof ResourceDeleteEvent) {
                replayDelete((ResourceDeleteEvent) event);
                eventListener.onDelete((ResourceDeleteEvent) event);
            }
            eventSize++;
        });

        if (!locally) {
            UnitOfWork.get().unlock();
        }
    }

    private void replayDelete(ResourceDeleteEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay delete for res {}", event.getResPath());
        resourceStore.deleteResource(event.getResPath());
    }

    private void replayUpdate(ResourceCreateOrUpdateEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay update for res {}, with new version: {}", event.getResPath(), event.getCreatedOrUpdated().getMvcc());
        val raw = event.getCreatedOrUpdated();
        val oldRaw = resourceStore.getResource(raw.getResPath());
        if (oldRaw == null) {
            resourceStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(), raw.getMvcc());
        } else {
            resourceStore.checkAndPutResource(raw.getResPath(), raw.getByteSource(), raw.getMvcc() - 1);
        }
    }

}
