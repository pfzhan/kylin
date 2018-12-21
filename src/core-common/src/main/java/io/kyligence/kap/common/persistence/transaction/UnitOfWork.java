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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.kyligence.kap.common.persistence.UnitMessages;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ThreadViewResourceStore;
import org.apache.kylin.common.persistence.TombRawResource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.event.EndUnit;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.event.ResourceRelatedEvent;
import io.kyligence.kap.common.persistence.event.StartUnit;
import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.common.persistence.transaction.mq.MQPublishFailureException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class UnitOfWork {
    public static final String GLOBAL_UNIT = "@global";

    private static ThreadLocal<Boolean> replaying = new ThreadLocal<>();
    private static ThreadLocal<UnitOfWork> threadLocals = new ThreadLocal<>();
    private static Map<String, ReentrantLock> projectLocks = Maps.newConcurrentMap();
    private static ReentrantLock globalLock = new ReentrantLock();

    private KylinConfig originThreadLocalConfig = null;
    private ReentrantLock currentLock = null;
    private final String project;

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName) {
        return doInTransactionWithRetry(f, unitName, 10);
    }

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName, int maxRetry) {
        int retry = 0;
        boolean isIndependentTransaction = true;
        long startTime = 0;
        while (retry++ < maxRetry) {
            try {
                T ret;
                log.debug("start unit of work for project {}", unitName);

                if (threadLocals.get() != null) {

                    if (!threadLocals.get().project.equals(unitName)) {
                        throw new IllegalStateException(
                                String.format("re-entry of UnitOfWork with different unit name? existing: %s, new: %s",
                                        threadLocals.get().project, unitName));
                    }

                    isIndependentTransaction = false;
                    retry = 100; // don't retry even sth go wrong
                    ret = f.process();

                } else {
                    startTime = System.currentTimeMillis();
                    UnitOfWork.startTransaction(unitName, true);
                    ret = f.process();
                    UnitOfWork.endTransaction();
                    long duration = System.currentTimeMillis() - startTime;
                    if (duration > 3000) {
                        log.warn("a UnitOfWork takes too long time: {}", duration);
                    }
                }
                return ret;
            } catch (MQPublishFailureException mqe) {
                throw new TransactionException("transaction failed due to Message Queue problem", mqe);
            } catch (Throwable throwable) {
                if (retry >= maxRetry) {
                    throw new TransactionException(
                            "exhausted max retry times, transaction failed due to inconsistent state", throwable);
                }
                //else proceed retry
            } finally {
                if (isIndependentTransaction) {
                    try {
                        UnitOfWork.get().unlock();
                    } catch (IllegalStateException e) {
                        //has not hold the lock yet, it's ok
                    }

                    if (threadLocals.get() != null) {
                        clearLocalConfig();
                        if (threadLocals.get().originThreadLocalConfig != null) {
                            KylinConfig.setKylinConfigThreadLocal(threadLocals.get().originThreadLocalConfig);
                        }
                        threadLocals.remove();
                    }
                }
            }
        }
        throw new IllegalStateException();
    }

    static UnitOfWork startTransaction(String project, boolean useSandboxStore) {

        ReentrantLock lock = getLock(project);

        log.debug("get lock {}, {}", project, lock.isHeldByCurrentThread());
        //re-entry is not encouraged (because it indicates complex handling logic, bad smell), let's abandon it first
        Preconditions.checkState(!lock.isHeldByCurrentThread());

        lock.lock();

        UnitOfWork unitOfWork = new UnitOfWork(project);
        unitOfWork.currentLock = lock;
        threadLocals.set(unitOfWork);

        if (useSandboxStore) {
            //put a sandbox meta store on top of base meta store for isolation
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            //only for UT
            if (KylinConfig.isKylinConfigThreadLocal()) {
                unitOfWork.originThreadLocalConfig = config;
                log.warn("unitOfWork.originThreadLocalConfig is set to {}", config);

                if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
                    throw new IllegalStateException(
                            "No thread local KylinConfig is expected when starting a UnitOfWork, current KylinConfig: "
                                    + KylinConfig.getInstanceFromEnv());
                }
            }

            ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
            KylinConfig threadLocalConfig = KylinConfig.createKylinConfig(config);
            //TODO check uderlying rs is never changed since here
            ThreadViewResourceStore rs = new ThreadViewResourceStore((InMemResourceStore) underlying,
                    threadLocalConfig);
            ResourceStore.setRS(threadLocalConfig, rs);

            KylinConfig.setKylinConfigThreadLocal(threadLocalConfig);
            log.info("sandbox RS {} now takes place for main RS {}", rs, underlying);
        }

        return unitOfWork;
    }

    public static UnitOfWork get() {
        val temp = threadLocals.get();
        Preconditions.checkNotNull(temp, "current thread is not accompanied by a UnitOfWork");

        Preconditions.checkNotNull(temp.currentLock);
        Preconditions.checkState(temp.currentLock.isHeldByCurrentThread());

        return temp;
    }

    public static boolean containsLock(String project) {
        return project.equals(GLOBAL_UNIT) || projectLocks.containsKey(project);
    }

    static void endTransaction() {

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val threadViewRS = (ThreadViewResourceStore) ResourceStore.getKylinMetaStore(config);
        List<RawResource> data = threadViewRS.getResources();
        val eventList = data.stream().map(x -> {
            if (x instanceof TombRawResource) {
                return new ResourceDeleteEvent(x.getResPath());
            } else {
                return new ResourceCreateOrUpdateEvent(
                        new RawResource(x.getResPath(), x.getByteSource(), x.getTimestamp(), x.getMvcc() - 1));
            }
        }).collect(Collectors.<Event> toList());

        //clean rs and config
        clearLocalConfig();

        val originConfig = get().originThreadLocalConfig == null ? KylinConfig.getInstanceFromEnv()
                : get().originThreadLocalConfig;
        // publish events here
        packageEvents(eventList, get().project);
        eventList.forEach(e -> e.setKey(get().project));
        val eventStore = EventStore.getInstance(originConfig);
        eventStore.getEventPublisher().publish(eventList);

        try {
            // replay in leader before release lock
            replaying.set(true);
            val replayer = EventSynchronization.getInstance(originConfig);
            replayer.replay(new UnitMessages(eventList), true);
            replaying.remove();
        } catch (Exception e) {
            // in theory, this should not happen
            log.error("Unexpected error happened! Aborting right now.", e);
            System.exit(1);
        }
    }

    private static void clearLocalConfig() {
        if (KylinConfig.isKylinConfigThreadLocal()) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            ResourceStore.clearCache(config);
            KylinConfig.removeKylinConfigThreadLocal();
        }
    }

    private static void packageEvents(List<Event> events, String project) {
        if (!project.equals(GLOBAL_UNIT)) {
            Preconditions.checkState(
                    events.stream().filter(e -> e instanceof ResourceRelatedEvent)
                            .allMatch(e -> ((ResourceRelatedEvent) e).getResPath().startsWith("/" + project)),
                    "some event are not in project " + project);
        }
        val uuid = UUID.randomUUID().toString();
        events.add(0, new StartUnit(uuid));
        events.add(new EndUnit(uuid));
    }

    public static boolean isReplaying() {
        return Objects.equals(true, replaying.get())
                || Thread.currentThread().getName().equals(EventStore.CONSUMER_THREAD_NAME);
    }

    public static ReentrantLock getLock(String project) {
        if (project.equals(GLOBAL_UNIT)) {
            return globalLock;
        }
        ReentrantLock lock = projectLocks.get(project);
        if (lock == null) {
            synchronized (UnitOfWork.class) {
                val cacheLock = projectLocks.get(project);
                if (cacheLock == null) {
                    projectLocks.put(project, new ReentrantLock());
                }
            }
        }

        return projectLocks.get(project);
    }

    public void unlock() {
        currentLock.unlock();
    }

    public interface Callback<T> {
        T process() throws Exception;
    }
}
