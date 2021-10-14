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

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_CONNECT_META_DATABASE;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAuditLogReplayWorker {

    protected static final long STEP = 1000;
    protected final JdbcAuditLogStore auditLogStore;
    protected final KylinConfig config;

    // only a thread is necessary
    protected final ScheduledExecutorService consumeExecutor = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("ReplayWorker"));

    protected final AtomicBoolean isStopped = new AtomicBoolean(false);

    protected AbstractAuditLogReplayWorker(KylinConfig config, JdbcAuditLogStore auditLogStore) {
        this.config = config;
        this.auditLogStore = auditLogStore;
    }

    public abstract void startSchedule(long currentId, boolean syncImmediately);

    public void catchup() {
        consumeExecutor.submit(() -> catchupInternal(1));
    }

    public void close(boolean isGracefully) {
        isStopped.set(true);
        if (isGracefully) {
            ExecutorServiceUtil.shutdownGracefully(consumeExecutor, 60);
        } else {
            ExecutorServiceUtil.forceShutdown(consumeExecutor);
        }
    }

    protected void replayLogs(MessageSynchronization replayer, List<AuditLog> logs) {
        Map<String, UnitMessages> messagesMap = Maps.newLinkedHashMap();
        for (AuditLog log : logs) {
            val event = Event.fromLog(log);
            String unitId = log.getUnitId();
            if (messagesMap.get(unitId) == null) {
                UnitMessages newMessages = new UnitMessages();
                newMessages.getMessages().add(event);
                messagesMap.put(unitId, newMessages);
            } else {
                messagesMap.get(unitId).getMessages().add(event);
            }
        }

        for (UnitMessages message : messagesMap.values()) {
            log.debug("replay {} event for project:{}", message.getMessages().size(), message.getKey());
            replayer.replay(message);
        }
    }

    public abstract long getLogOffset();

    public abstract void updateOffset(long expected);

    public abstract void forceUpdateOffset(long expected);

    protected abstract void catchupInternal(int countDown);

    protected abstract boolean hasCatch(long targetId);

    public void forceCatchFrom(long expected) {
        forceUpdateOffset(expected);
        catchup();
    }

    public void catchupFrom(long expected) {
        updateOffset(expected);
        catchup();
    }

    protected boolean logAllCommit(long startOffset, long endOffset) {
        return auditLogStore.count(startOffset, endOffset) == (endOffset - startOffset);
    }

    protected void handleReloadAll(Exception e) {
        log.error("Critical exception happened, try to reload metadata ", e);
        val lockKeys = Lists.newArrayList(TransactionLock.projectLocks.keySet());
        lockKeys.sort(Comparator.naturalOrder());
        try {
            EventBusFactory.getInstance().postSync(new StartReloadEvent());
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).lock();
            }
            log.info("Acquired all locks, start to copy");
            val fixerKylinConfig = KylinConfig.createKylinConfig(config);
            val fixerResourceStore = ResourceStore.getKylinMetaStore(fixerKylinConfig);
            log.info("Finish read all metadata from store, start to reload");
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            resourceStore.deleteResourceRecursively("/");
            fixerResourceStore.copy("/", resourceStore);
            resourceStore.setOffset(fixerResourceStore.getOffset());
            updateOffset(fixerResourceStore.getOffset());
        } catch (Throwable th) {
            log.error("reload all failed", th);
        } finally {
            Collections.reverse(lockKeys);
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).unlock();
            }
            EventBusFactory.getInstance().postSync(new EndReloadEvent());
        }
        log.info("Reload finished");
    }

    public void waitForCatchup(long targetId, long timeout) throws TimeoutException {
        long endTime = System.currentTimeMillis() + timeout * 1000;
        try {
            while (System.currentTimeMillis() < endTime) {
                if (hasCatch(targetId)) {
                    return;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            log.info("Wait for catchup to {} failed", targetId, e);
            Thread.currentThread().interrupt();
        }
        throw new TimeoutException(String.format(Locale.ROOT, "Cannot reach %s before %s, current is %s", targetId,
                endTime, getLogOffset()));
    }

    public static class StartReloadEvent {
    }

    public static class EndReloadEvent {
    }

    protected static class DatabaseNotAvailableException extends KylinException {
        public DatabaseNotAvailableException(Exception e) {
            super(FAILED_CONNECT_META_DATABASE, e);
        }
    }

}
