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

import java.util.Collections;
import java.util.Comparator;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageSynchronization {

    private final KylinConfig config;
    private final EventListenerRegistry eventListener;
    @Setter
    private ResourceStore.Callback<Boolean> checker;

    public static MessageSynchronization getInstance(KylinConfig config) {
        return config.getManager(MessageSynchronization.class);
    }

    static MessageSynchronization newInstance(KylinConfig config) {
        return new MessageSynchronization(config);
    }

    private MessageSynchronization(KylinConfig config) {
        this.config = config;
        eventListener = EventListenerRegistry.getInstance(config);
    }

    public void replay(UnitMessages messages) {
        if (messages.isEmpty()) {
            return;
        }
        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().processor(() -> {
            if (checker != null && checker.check(messages)) {
                return null;
            }
            replayInTransaction(messages);
            return null;
        }).maxRetry(1).unitName(messages.getKey()).useSandbox(false).build());
    }

    void replayInTransaction(UnitMessages messages) {
        UnitOfWork.replaying.set(true);
        messages.getMessages().forEach(event -> {
            if (event instanceof ResourceCreateOrUpdateEvent) {
                replayUpdate((ResourceCreateOrUpdateEvent) event);
                eventListener.onUpdate((ResourceCreateOrUpdateEvent) event);
            } else if (event instanceof ResourceDeleteEvent) {
                replayDelete((ResourceDeleteEvent) event);
                eventListener.onDelete((ResourceDeleteEvent) event);
            }
        });
        UnitOfWork.replaying.remove();
    }

    private void replayDelete(ResourceDeleteEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay delete for res {}", event.getResPath());
        resourceStore.deleteResource(event.getResPath());
    }

    private void replayUpdate(ResourceCreateOrUpdateEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay update for res {}, with new version: {}", event.getResPath(),
                event.getCreatedOrUpdated().getMvcc());
        val raw = event.getCreatedOrUpdated();
        val oldRaw = resourceStore.getResource(raw.getResPath());
        if (!config.isJobNode()) {
            resourceStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc());
            return;
        }

        if (oldRaw == null) {
            resourceStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc());
        } else {
            resourceStore.checkAndPutResource(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc() - 1);
        }
    }

    public void replayAllMetadata() {
        val lockKeys = Lists.newArrayList(TransactionLock.getProjectLocksForRead().keySet());
        lockKeys.sort(Comparator.naturalOrder());
        try {
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).lock();
            }
            log.info("Acquired all locks, start to copy");
            UnitOfWork.replaying.set(true);
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val fixerKylinConfig = KylinConfig.createKylinConfig(kylinConfig);
            val fixerResourceStore = ResourceStore.getKylinMetaStore(fixerKylinConfig);
            log.info("Finish read all metadata from store, start to reload");
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.deleteResourceRecursively("/");
            fixerResourceStore.copy("/", resourceStore);
            resourceStore.setOffset(fixerResourceStore.getOffset());
            resourceStore.forceCatchup();
            UnitOfWork.replaying.remove();
            log.info("Reload finished");
        } finally {
            Collections.reverse(lockKeys);
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).unlock();
            }
        }
    }

}
