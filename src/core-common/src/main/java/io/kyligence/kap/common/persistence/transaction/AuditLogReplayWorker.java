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

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionConflictException;
import org.springframework.transaction.TransactionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogReplayWorker extends AbstractAuditLogReplayWorker {

    @Getter
    private volatile long logOffset = 0L;

    public void startSchedule(long currentId, boolean syncImmediately) {
        updateOffset(currentId);
        if (syncImmediately) {
            catchupInternal(1);
        }
        long interval = config.getCatchUpInterval();
        consumeExecutor.scheduleWithFixedDelay(() -> catchupInternal(1), interval, interval, TimeUnit.SECONDS);
    }

    public AuditLogReplayWorker(KylinConfig config, JdbcAuditLogStore restorer) {
        super(config, restorer);
    }

    public synchronized void updateOffset(long expected) {
        logOffset = Math.max(logOffset, expected);
    }

    public void forceUpdateOffset(long expected) {
        logOffset = expected;
    }

    @Override
    protected boolean hasCatch(long targetId) {
        return logOffset >= targetId;
    }

    @Override
    protected void catchupInternal(int countDown) {
        if (isStopped.get()) {
            log.info("Catchup Already stopped");
            return;
        }
        try {
            catchupToMaxId(logOffset);
        } catch (TransactionException | DatabaseNotAvailableException e) {
            log.warn("cannot create transaction or auditlog database connect error, ignore it", e);
            threadWait(5000);
        } catch (Exception e) {
            val rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof VersionConflictException && countDown > 0) {
                handleConflictOnce((VersionConflictException) rootCause, countDown);
            } else {
                handleReloadAll(e);
            }
        }

    }

    private void threadWait(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public void catchupToMaxId(final long currentId) {
        val replayer = MessageSynchronization.getInstance(config);
        val store = ResourceStore.getKylinMetaStore(config);
        replayer.setChecker(store.getChecker());

        val maxId = waitMaxIdOk(currentId);
        if (maxId == -1 || maxId == currentId) {
            return;
        }
        withTransaction(auditLogStore.getTransactionManager(), () -> {
            log.debug("start restore, current max_id is {}", maxId);
            var start = currentId;
            while (start < maxId) {
                val logs = auditLogStore.fetch(start, Math.min(STEP, maxId - start));
                replayLogs(replayer, logs);
                start += STEP;
            }
            return maxId;
        });
        updateOffset(maxId);
    }

    private long waitMaxIdOk(long currentId) {
        try {
            val maxId = auditLogStore.getMaxId();
            if (maxId == currentId) {
                return maxId;
            }
            if (waitLogCommit(3, currentId, maxId)) {
                return maxId;
            }
            return -1;
        } catch (Exception e) {
            throw new DatabaseNotAvailableException(e);
        }
    }

    private boolean waitLogCommit(int maxTimes, long currentId, long maxId) {
        if (!config.isNeedReplayConsecutiveLog()) {
            return true;
        }
        int count = 0;
        while (!logAllCommit(currentId, maxId)) {
            threadWait(100);
            count++;
            if (count >= maxTimes) {
                return false;
            }
        }
        return true;
    }

    private void handleConflictOnce(VersionConflictException e, int countDown) {
        val replayer = MessageSynchronization.getInstance(config);
        val originResource = e.getResource();
        val targetResource = e.getTargetResource();
        val conflictedPath = originResource.getResPath();
        log.warn("Resource <{}:{}> version conflict, msg:{}", conflictedPath, originResource.getMvcc(), e.getMessage());
        log.info("Try to reload {}", originResource.getResPath());
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val metaStore = resourceStore.getMetadataStore();
        try {
            val correctedResource = metaStore.load(conflictedPath);
            log.info("Origin version is {},  current version in store is {}", originResource.getMvcc(),
                    correctedResource.getMvcc());
            val fixResource = new AuditLog(0L, conflictedPath, correctedResource.getByteSource(),
                    correctedResource.getTimestamp(), originResource.getMvcc() + 1, null, null, null);
            replayer.replay(new UnitMessages(Lists.newArrayList(Event.fromLog(fixResource))));

            val currentAuditLog = resourceStore.getAuditLogStore().get(conflictedPath, targetResource.getMvcc());
            if (currentAuditLog != null) {
                log.info("After fix conflict, set offset to {}", currentAuditLog.getId());
                updateOffset(currentAuditLog.getId());
            }
        } catch (IOException ioException) {
            log.warn("Reload metadata <{}> failed", conflictedPath);
        }
        catchupInternal(countDown - 1);
    }

}
