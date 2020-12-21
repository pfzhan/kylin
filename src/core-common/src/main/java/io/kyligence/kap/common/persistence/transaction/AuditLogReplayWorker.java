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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionConflictException;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.springframework.transaction.TransactionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.guava20.shaded.common.eventbus.AsyncEventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogReplayWorker {

    private final Function<Long, Long> restorer;
    private final KylinConfig kylinConfig;

    private final AsyncEventBus replayEventBus;

    private final ScheduledExecutorService consumeExecutor = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("ReplayWorker"));
    private final AtomicBoolean isStopped = new AtomicBoolean(false);

    @Getter
    private volatile long logOffset = 0L;

    public AuditLogReplayWorker(KylinConfig config, Function<Long, Long> restorer) {
        this.kylinConfig = config;
        this.restorer = restorer;
        replayEventBus = new AsyncEventBus(consumeExecutor);
        replayEventBus.register(new CatchupEventHandler());

    }

    public void startSchedule(long interval) {
        consumeExecutor.submit(() -> replayEventBus.post(new CatchupEvent(interval)));
    }

    public void catchup() {
        replayEventBus.post(new CatchupEvent());
    }

    public void waitForCatchup(long targetId, long timeout) throws TimeoutException {
        long endTime = System.currentTimeMillis() + timeout * 1000;
        try {
            while (System.currentTimeMillis() < endTime) {
                if (logOffset >= targetId) {
                    return;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            log.info("Wait for catchup to {} failed", targetId, e);
        }
        throw new TimeoutException(
                String.format(Locale.ROOT, "Cannot reach %s before %s, current is %s", targetId, endTime, logOffset));
    }

    public void close(boolean isGracefully) {
        isStopped.set(true);
        if (isGracefully) {
            ExecutorServiceUtil.shutdownGracefully(consumeExecutor, 60);
        } else {
            ExecutorServiceUtil.forceShutdown(consumeExecutor);
        }
    }

    public synchronized void updateOffset(long expected) {
        logOffset = Math.max(logOffset, expected);
    }

    class CatchupEventHandler {

        @Subscribe
        public void onCatchupEvent(CatchupEvent event) {
            onCatchupInternal(1);
            if (event.interval > 0) {
                consumeExecutor.schedule(() -> replayEventBus.post(new CatchupEvent(event.interval)), event.interval,
                        TimeUnit.SECONDS);
            }
        }

        private void onCatchupInternal(int countDown) {
            if (isStopped.get()) {
                log.info("Catchup Already stopped");
                return;
            }
            try {
                val offset = restorer.apply(logOffset);
                updateOffset(offset);
            } catch (TransactionException e) {
                log.warn("cannot create transaction, ignore it", e);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                val rootCause = Throwables.getRootCause(e);
                if (rootCause instanceof VersionConflictException && countDown > 0) {
                    handleConflictOnce((VersionConflictException) rootCause, countDown);
                } else {
                    handleReloadAll(e);
                }
            }

        }

        private void handleConflictOnce(VersionConflictException e, int countDown) {
            val replayer = MessageSynchronization.getInstance(kylinConfig);
            val originResource = e.getResource();
            val conflictedPath = originResource.getResPath();
            log.warn("Resource <{}:{}> version conflict, msg:{}", conflictedPath, originResource.getMvcc(),
                    e.getMessage());
            log.info("Try to reload {}", originResource.getResPath());
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            val metaStore = resourceStore.getMetadataStore();
            try {
                val correctedResource = metaStore.load(conflictedPath);
                log.info("Origin version is {},  current version in store is {}", originResource.getMvcc(),
                        correctedResource.getMvcc());
                val fixResource = new AuditLog(0L, conflictedPath, correctedResource.getByteSource(),
                        correctedResource.getTimestamp(), originResource.getMvcc() + 1, null, null, null);
                replayer.replay(new UnitMessages(Lists.newArrayList(Event.fromLog(fixResource))));
            } catch (IOException ioException) {
                log.warn("Reload metadata <{}> failed", conflictedPath);
            }
            onCatchupInternal(countDown - 1);
        }

        private void handleReloadAll(Exception e) {
            log.error("Critical exception happened, try to reload metadata ", e);
            EventBusFactory.getInstance().postSync(new StartReloadEvent());
            val fixerKylinConfig = KylinConfig.createKylinConfig(kylinConfig);
            val fixerResourceStore = ResourceStore.getKylinMetaStore(fixerKylinConfig);
            log.info("Finish read all metadata from store, start to reload");

            val lockKeys = Lists.newArrayList(TransactionLock.projectLocks.keySet());
            lockKeys.sort(Comparator.naturalOrder());
            try {
                for (String lockKey : lockKeys) {
                    TransactionLock.getLock(lockKey, false).lock();
                }
                log.info("Acquired all locks, start to copy");
                val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
                resourceStore.deleteResourceRecursively("/");
                fixerResourceStore.copy("/", resourceStore);
                resourceStore.setOffset(fixerResourceStore.getOffset());
                updateOffset(fixerResourceStore.getOffset());
            } finally {
                Collections.reverse(lockKeys);
                for (String lockKey : lockKeys) {
                    TransactionLock.getLock(lockKey, false).unlock();
                }
            }
            log.info("Reload finished");

            EventBusFactory.getInstance().postSync(new EndReloadEvent());
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    static class CatchupEvent {
        private long interval;
    }

    public static class StartReloadEvent {
    }

    public static class EndReloadEvent {
    }
}
