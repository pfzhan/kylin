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

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import static org.apache.kylin.common.persistence.lock.TransactionDeadLockHandler.THREAD_NAME_PREFIX;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.code.ErrorCodeSystem;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ThreadViewResourceStore;
import org.apache.kylin.common.persistence.TombRawResource;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.EndUnit;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.event.ResourceRelatedEvent;
import org.apache.kylin.common.persistence.event.StartUnit;
import org.apache.kylin.common.persistence.lock.DeadLockException;
import org.apache.kylin.common.persistence.lock.LockInterruptException;
import org.apache.kylin.common.persistence.lock.MemoryLockUtils;
import org.apache.kylin.common.persistence.lock.TempLock;
import org.apache.kylin.common.persistence.lock.TransactionLock;
import org.apache.kylin.common.persistence.metadata.JdbcMetadataStore;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class UnitOfWork {
    public static final String GLOBAL_UNIT = "_global";

    public static final long DEFAULT_EPOCH_ID = -1L;
    public static final int DEFAULT_MAX_RETRY = 3;

    private static EventBusFactory factory;

    static {
        factory = EventBusFactory.getInstance();
    }

    static ThreadLocal<Boolean> replaying = new ThreadLocal<>();
    private static ThreadLocal<UnitOfWorkContext> threadLocals = new ThreadLocal<>();

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName) {
        return doInTransactionWithRetry(f, unitName, 3);
    }

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName, int maxRetry) {
        return doInTransactionWithRetry(
                UnitOfWorkParams.<T> builder().processor(f).unitName(unitName).maxRetry(maxRetry).build());
    }

    public static <T> T doInTransactionWithRetry(UnitOfWorkParams<T> params) {
        try (SetThreadName ignore = new SetThreadName(THREAD_NAME_PREFIX)) {
            val f = params.getProcessor();
            // reused transaction, won't retry
            if (isAlreadyInTransaction()) {
                val unitOfWork = UnitOfWork.get();
                unitOfWork.checkReentrant(params);
                try {
                    checkEpoch(params);
                    f.preProcess();
                    return f.process();
                } catch (Throwable throwable) {
                    f.onProcessError(throwable);
                    throw new TransactionException("transaction failed due to inconsistent state", throwable);
                }
            }

            // new independent transaction with retry
            int retry = 0;
            val traceId = RandomUtil.randomUUIDStr();
            if (params.isRetryMoreTimeForDeadLockException()) {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                params.setRetryUntil(System.currentTimeMillis() + config.getMaxSecondsForDeadLockRetry() * 1000);
            }
            while (retry++ < params.getMaxRetry()) {

                val ret = doTransaction(params, retry, traceId);
                if (ret.getSecond()) {
                    return ret.getFirst();
                }
            }
            throw new IllegalStateException("Unexpected doInTransactionWithRetry end");
        }
    }

    private static <T> Pair<T, Boolean> doTransaction(UnitOfWorkParams<T> params, int retry, String traceId) {
        Pair<T, Boolean> result = Pair.newPair(null, false);
        UnitOfWorkContext context = null;
        try {
            T ret;

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                if (retry != 1) {
                    log.debug("UnitOfWork {} in project {} is retrying for {}th time", traceId, params.getUnitName(),
                            retry);
                } else {
                    log.debug("UnitOfWork {} started on project {}", traceId, params.getUnitName());
                }
            }

            long startTime = System.currentTimeMillis();
            params.getProcessor().preProcess();
            context = UnitOfWork.startTransaction(params);
            long startTransactionTime = System.currentTimeMillis();
            val waitForLockTime = startTransactionTime - startTime;
            if (waitForLockTime > 3000) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    log.warn("UnitOfWork {} takes too long time {}ms to start", traceId, waitForLockTime);
                }
            }

            ret = params.getProcessor().process();
            UnitOfWork.endTransaction(traceId, params);
            long duration = System.currentTimeMillis() - startTransactionTime;
            logIfLongTransaction(duration, traceId);

            result = Pair.newPair(ret, true);
        } catch (Throwable throwable) {
            handleError(throwable, params, retry, traceId);
        } finally {
            if (isAlreadyInTransaction()) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                    val unitOfWork = UnitOfWork.get();
                    MemoryLockUtils.removeThreadFromGraph();
                    List<TransactionLock> lockInOrder = Arrays
                            .asList(unitOfWork.getCurrentLock().toArray(new TransactionLock[0]));
                    Lists.reverse(lockInOrder).stream().filter(TransactionLock::isHeldByCurrentThread)
                            .forEach(TransactionLock::unlock);
                    unitOfWork.cleanResource();
                    val curLock = UnitOfWork.threadLocals.get().getCurrentLock();
                    curLock.stream().filter(TempLock.class::isInstance)
                            .forEach(l -> TransactionManagerInstance.INSTANCE.removeLock(l.transactionUnit()));
                } catch (IllegalStateException e) {
                    //has not hold the lock yet, it's ok
                    log.warn(e.getMessage());
                } catch (Exception e) {
                    log.error("Failed to close UnitOfWork", e);
                }
                threadLocals.remove();
            }
        }

        if (result.getSecond() && context != null) {
            context.onUnitFinished();
        }
        return result;
    }

    private static void logIfLongTransaction(long duration, String traceId) {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            if (duration > 3000) {
                log.warn("UnitOfWork {} takes too long time {}ms to complete", traceId, duration);
                if (duration > 10000) {
                    log.warn("current stack: ", new Throwable());
                }
            } else {
                log.debug("UnitOfWork {} takes {}ms to complete", traceId, duration);
            }
        }
    }

    static <T> UnitOfWorkContext startTransaction(UnitOfWorkParams<T> params) throws Exception {
        val unitName = params.getUnitName();
        val readonly = params.isReadonly();
        checkEpoch(params);
        final UnitOfWorkContext unitOfWork = initUnitOfContext(params, unitName, readonly);

        if (readonly || !params.isUseSandbox()) {
            unitOfWork.setLocalConfig(null);
            return unitOfWork;
        }

        //put a sandbox meta store on top of base meta store for isolation
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        KylinConfig configCopy = KylinConfig.createKylinConfig(config);
        //TODO check underlying rs is never changed since here
        ThreadViewResourceStore rs = new ThreadViewResourceStore((InMemResourceStore) underlying, configCopy);
        ResourceStore.setRS(configCopy, rs);
        unitOfWork.setLocalConfig(KylinConfig.setAndUnsetThreadLocalConfig(configCopy));

        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            log.trace("sandbox RS {} now takes place for main RS {}", rs, underlying);
        }

        return unitOfWork;
    }

    private static <T> UnitOfWorkContext initUnitOfContext(UnitOfWorkParams<T> params, String unitName,
            boolean readonly) {
        val unitOfWork = new UnitOfWorkContext(unitName);
        TransactionLock lock;
        if (params.isUseProjectLock()) {
            lock = TransactionManagerInstance.INSTANCE.getProjectLock(unitName);
        } else if (params.getTempLockName() != null) {
            lock = TransactionManagerInstance.INSTANCE.getTempLock(params.getTempLockName(), readonly);
        } else {
            lock = TransactionManagerInstance.INSTANCE.getLock(unitName, readonly);
            if (lock.transactionUnit().equals(unitName)) {
                params.setUseProjectLock(true);
            }
        }
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            log.info("get lock for project {}, lock is held by current thread: {}", unitName,
                    lock.isHeldByCurrentThread());
        }
        // re-entry is not encouraged (because it indicates complex handling logic, bad smell), let's abandon it first
        Preconditions.checkState(!lock.isHeldByCurrentThread());
        lock.lock();
        unitOfWork.getCurrentLock().add(lock);

        unitOfWork.setParams(params);
        threadLocals.set(unitOfWork);
        return unitOfWork;
    }

    private static <T> void checkEpoch(UnitOfWorkParams<T> params) throws Exception {
        val checker = params.getEpochChecker();
        if (checker != null && !params.isReadonly()) {
            checker.process();
        }
    }

    public static UnitOfWorkContext get() {
        val temp = threadLocals.get();
        Preconditions.checkNotNull(temp, "current thread is not accompanied by a UnitOfWork");
        temp.checkLockStatus();
        return temp;
    }

    static <T> void endTransaction(String traceId, UnitOfWorkParams<T> params) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val work = get();
        if (work.isReadonly() || !work.isUseSandbox()) {
            work.cleanResource();
            return;
        }
        val threadViewRS = (ThreadViewResourceStore) ResourceStore.getKylinMetaStore(config);
        List<RawResource> data = threadViewRS.getResources();
        if (!params.isUseProjectLock()
                && (threadViewRS.getMetadataStore() instanceof JdbcMetadataStore || config.isUTEnv())) {
            data.forEach(rawResource -> {
                if (!UnitOfWork.checkWriteLock(rawResource.getResPath())) {
                    throw new IllegalStateException(
                            "Transaction does not hold a write lock for resPath: " + rawResource.getResPath());
                }
            });
        }

        val eventList = data.stream().map(x -> {
            if (x instanceof TombRawResource) {
                return new ResourceDeleteEvent(x.getResPath());
            } else {
                return new ResourceCreateOrUpdateEvent(x);
            }
        }).collect(Collectors.<Event> toList());

        //clean rs and config
        work.cleanResource();

        val originConfig = KylinConfig.getInstanceFromEnv();
        // publish events here
        val metadataStore = ResourceStore.getKylinMetaStore(originConfig).getMetadataStore();
        val writeInterceptor = params.getWriteInterceptor();
        val unitMessages = packageEvents(eventList, get().getProject(), traceId, writeInterceptor,
                params.getProjectId());
        long entitiesSize = unitMessages.getMessages().stream().filter(event -> event instanceof ResourceRelatedEvent)
                .count();
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            log.debug("transaction {} updates {} metadata items", traceId, entitiesSize);
        }
        checkEpoch(params);
        val unitName = params.getUnitName();
        metadataStore.batchUpdate(unitMessages, get().getParams().isSkipAuditLog(), unitName, params.getEpochId());
        if (entitiesSize != 0 && !params.isReadonly() && !params.isSkipAuditLog() && !config.isUTEnv()) {
            factory.postAsync(new AuditLogBroadcastEventNotifier());
        }
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            // replayInTransaction in leader before release lock
            val replayer = MessageSynchronization.getInstance(originConfig);
            replayer.replayInTransaction(unitMessages);
        } catch (Exception e) {
            // in theory, this should not happen
            log.error("Unexpected error happened! Aborting right now.", e);
            Unsafe.systemExit(1);
        }
    }

    public static void recordLocks(String resPath, TransactionLock lock, boolean readOnly) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        UnitOfWork.get().getCurrentLock().add(lock);
        if (readOnly) {
            UnitOfWork.get().getReadLockPath().add(resPath);
        } else {
            UnitOfWork.get().getWriteLockPath().add(resPath);
        }
    }

    public static boolean checkWriteLock(String resPath) {
        return UnitOfWork.get().getWriteLockPath().contains(resPath);
    }

    private static void handleError(Throwable throwable, UnitOfWorkParams<?> params, int retry, String traceId) {
        if (throwable instanceof KylinException && Objects.nonNull(((KylinException) throwable).getErrorCodeProducer())
                && ((KylinException) throwable).getErrorCodeProducer().getErrorCode()
                        .equals(ErrorCodeSystem.MAINTENANCE_MODE_WRITE_FAILED.getErrorCode())) {
            retry = params.getMaxRetry();
        }
        if (throwable instanceof DeadLockException) {
            log.debug("DeadLock found in this transaction, will retry");
            if (params.isRetryMoreTimeForDeadLockException() && System.currentTimeMillis() < params.getRetryUntil()) {
                params.setMaxRetry(params.getMaxRetry() + 1);
            }
        }
        if (throwable instanceof LockInterruptException) {
            // Just remove the interrupted flag.
            Thread.interrupted();
            log.debug("DeadLock is found by TransactionDeadLockHandler, will retry");
        }
        if (throwable instanceof QuitTxnRightNow) {
            retry = params.getMaxRetry();
        }

        if (retry >= params.getMaxRetry()) {
            params.getProcessor().onProcessError(throwable);
            throw new TransactionException(
                    "exhausted max retry times, transaction failed due to inconsistent state, traceId:" + traceId,
                    throwable);
        }

        if (retry == 1) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.warn("transaction failed at first time, traceId:" + traceId, throwable);
            }
        }
    }

    private static UnitMessages packageEvents(List<Event> events, String project, String uuid,
            Consumer<ResourceRelatedEvent> writeInterceptor, String projectId) {
        for (Event e : events) {
            if (!(e instanceof ResourceRelatedEvent)) {
                continue;
            }
            val event = (ResourceRelatedEvent) e;
            val endWithProjectId = event.getResPath().startsWith("/" + UnitOfWork.GLOBAL_UNIT)
                    && StringUtils.isNotBlank(projectId) && event.getResPath().endsWith(projectId);
            if (!(event.getResPath().startsWith("/" + project) || event.getResPath().endsWith("/" + project + ".json")
                    || get().getParams().isAll() || endWithProjectId)) {
                throw new IllegalStateException("some event are not in project " + project);
            }
            if (writeInterceptor != null) {
                writeInterceptor.accept(event);
            }
        }
        events.add(0, new StartUnit(uuid));
        events.add(new EndUnit(uuid));
        events.forEach(e -> e.setKey(get().getProject()));
        return new UnitMessages(events);
    }

    public static boolean isAlreadyInTransaction() {
        return threadLocals.get() != null;
    }

    public static boolean isReplaying() {
        return Objects.equals(true, replaying.get());
    }

    public static boolean isReadonly() {
        return UnitOfWork.get().isReadonly();
    }

    public interface Callback<T> {
        /**
         * Pre-process stage (before transaction)
         */
        default void preProcess() {
        }

        /**
         * Process stage (within transaction)
         */
        T process() throws Exception;

        /**
         * Handle error of process stage
         * @param throwable
         */
        default void onProcessError(Throwable throwable) {
        }
    }
}
