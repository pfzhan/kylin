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
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ThreadViewResourceStore;
import org.apache.kylin.common.persistence.TombRawResource;
import org.apache.kylin.common.util.Pair;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.UnitMessages;
import io.kyligence.kap.common.persistence.event.EndUnit;
import io.kyligence.kap.common.persistence.event.Event;
import io.kyligence.kap.common.persistence.event.ResourceCreateOrUpdateEvent;
import io.kyligence.kap.common.persistence.event.ResourceDeleteEvent;
import io.kyligence.kap.common.persistence.event.ResourceRelatedEvent;
import io.kyligence.kap.common.persistence.event.StartUnit;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class UnitOfWork {
    public static final String GLOBAL_UNIT = "_global";

    static ThreadLocal<Boolean> replaying = new ThreadLocal<>();
    private static ThreadLocal<UnitOfWorkContext> threadLocals = new ThreadLocal<>();

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName) {
        return doInTransactionWithRetry(f, unitName, 10);
    }

    public static <T> T doInTransactionWithRetry(Callback<T> f, String unitName, int maxRetry) {
        return doInTransactionWithRetry(
                UnitOfWorkParams.<T> builder().processor(f).unitName(unitName).maxRetry(maxRetry).build());
    }

    public static <T> T doInTransactionWithRetry(UnitOfWorkParams<T> params) {
        val maxRetry = params.getMaxRetry();
        val f = params.getProcessor();
        // reused transaction, won't retry
        if (isAlreadyInTransaction()) {
            val unitOfWork = UnitOfWork.get();
            unitOfWork.checkReentrant(params);
            try {
                return f.process();
            } catch (Throwable throwable) {
                f.onProcessError(throwable);
                throw new TransactionException("transaction failed due to inconsistent state", throwable);
            }
        }

        // new independent transaction with retry
        int retry = 0;
        val traceId = UUID.randomUUID().toString();
        while (retry++ < maxRetry) {
            val ret = doTransaction(params, retry, traceId);
            if (ret.getSecond()) {
                return ret.getFirst();
            }
        }
        throw new IllegalStateException("Unexpected doInTransactionWithRetry end");
    }

    private static <T> Pair<T, Boolean> doTransaction(UnitOfWorkParams<T> params, int retry, String traceId) {
        Pair<T, Boolean> result = Pair.newPair(null, false);
        UnitOfWorkContext context = null;
        try {
            T ret;

            if (retry != 1) {
                log.debug("current unit of work in project {} is retrying for {}th time", params.getUnitName(), retry);
            } else {
                log.debug("start unit of work on project {}", params.getUnitName());
            }

            long startTime = System.currentTimeMillis();
            TransactionListenerRegistry.onStart(params.getUnitName());
            context = UnitOfWork.startTransaction(params);
            ret = params.getProcessor().process();
            UnitOfWork.endTransaction();
            long duration = System.currentTimeMillis() - startTime;
            if (duration > 3000) {
                log.warn("a UnitOfWork takes too long time {}ms to complete", duration);
            } else {
                log.debug("a UnitOfWork takes {}ms to complete", duration);
            }
            result = Pair.newPair(ret, true);
        } catch (Throwable throwable) {
            if (retry >= params.getMaxRetry()) {
                params.getProcessor().onProcessError(throwable);
                throw new TransactionException(
                        "exhausted max retry times, transaction failed due to inconsistent state, traceId:" + traceId,
                        throwable);
            }
            if (retry == 1) {
                log.warn("transaction failed at first time, retry it. traceId:" + traceId, throwable);
            }
        } finally {
            if (isAlreadyInTransaction()) {
                try {
                    val unitOfWork = UnitOfWork.get();
                    unitOfWork.getCurrentLock().unlock();
                    unitOfWork.cleanResource();
                    //log.debug("leaving UnitOfWork for project {}", unitOfWork.project);
                } catch (IllegalStateException e) {
                    //has not hold the lock yet, it's ok
                    log.warn(e.getMessage());
                } catch (Exception e) {
                    log.error("Failed to close UnitOfWork", e);
                }
                threadLocals.remove();
                TransactionListenerRegistry.onEnd(params.getUnitName());
            }
        }
        if (result.getSecond() && context != null) {
            context.runTasks();
        }
        return result;
    }

    static <T> UnitOfWorkContext startTransaction(UnitOfWorkParams<T> params) {
        val project = params.getUnitName();
        val readonly = params.isReadonly();
        val lock = TransactionLock.getLock(project, readonly);

        log.trace("get lock for project {}, lock is held by current thread: {}", project, lock.isHeldByCurrentThread());
        //re-entry is not encouraged (because it indicates complex handling logic, bad smell), let's abandon it first
        Preconditions.checkState(!lock.isHeldByCurrentThread());
        lock.lock();

        val unitOfWork = new UnitOfWorkContext(project);
        unitOfWork.setCurrentLock(lock);
        unitOfWork.setParams(params);
        threadLocals.set(unitOfWork);

        if (readonly || !params.isUseSandbox()) {
            unitOfWork.setLocalConfig(null);
            return unitOfWork;
        }

        //only for UT
        if (KylinConfig.isKylinConfigThreadLocal() && !KylinConfig.getInstanceFromEnv().isUTEnv()) {
            throw new IllegalStateException(
                    "No thread local KylinConfig is expected when starting a UnitOfWork, current KylinConfig: "
                            + KylinConfig.getInstanceFromEnv());
        }

        //put a sandbox meta store on top of base meta store for isolation
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ResourceStore underlying = ResourceStore.getKylinMetaStore(config);
        KylinConfig configCopy = KylinConfig.createKylinConfig(config);
        //TODO check underlying rs is never changed since here
        ThreadViewResourceStore rs = new ThreadViewResourceStore((InMemResourceStore) underlying, configCopy);
        ResourceStore.setRS(configCopy, rs);
        unitOfWork.setLocalConfig(KylinConfig.setAndUnsetThreadLocalConfig(configCopy));

        log.trace("sandbox RS {} now takes place for main RS {}", rs, underlying);

        return unitOfWork;
    }

    public static UnitOfWorkContext get() {
        val temp = threadLocals.get();
        Preconditions.checkNotNull(temp, "current thread is not accompanied by a UnitOfWork");
        temp.checkLockStatus();
        return temp;
    }

    static void endTransaction() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val work = get();
        if (work.isReadonly() || !work.isUseSandbox()) {
            work.cleanResource();
            return;
        }
        val threadViewRS = (ThreadViewResourceStore) ResourceStore.getKylinMetaStore(config);
        List<RawResource> data = threadViewRS.getResources();
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
        val unitMessages = packageEvents(eventList, get().getProject());
        val metadataStore = ResourceStore.getKylinMetaStore(originConfig).getMetadataStore();
        metadataStore.batchUpdate(unitMessages);

        try {
            // replayInTransaction in leader before release lock
            val replayer = MessageSynchronization.getInstance(originConfig);
            replayer.replayInTransaction(unitMessages);
        } catch (Exception e) {
            // in theory, this should not happen
            log.error("Unexpected error happened! Aborting right now.", e);
            System.exit(1);
        }
    }

    private static UnitMessages packageEvents(List<Event> events, String project) {
        Preconditions.checkState(events.stream().filter(e -> e instanceof ResourceRelatedEvent).allMatch(e -> {
            val event = (ResourceRelatedEvent) e;
            return event.getResPath().startsWith("/" + project) || event.getResPath().endsWith("/" + project + ".json");
        }), "some event are not in project " + project);
        val uuid = UUID.randomUUID().toString();
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
