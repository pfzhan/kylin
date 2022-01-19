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

package io.kyligence.kap.common.scheduler;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;

import io.kyligence.kap.common.persistence.transaction.BroadcastEventReadyNotifier;
import io.kyligence.kap.guava20.shaded.common.annotations.VisibleForTesting;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.eventbus.AsyncEventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.EventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.SyncThrowExceptionEventBus;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBusFactory {
    private final KylinConfig kylinConfig;

    private EventBus asyncEventBus;
    private EventBus syncEventBus;
    private EventBus broadcastEventBus;
    private EventBus serviceEventBus;

    private ThreadPoolExecutor eventExecutor;
    private ExecutorService broadcastExecutor;

    private final Map<String, RateLimiter> rateLimiters = Maps.newConcurrentMap();

    public static EventBusFactory getInstance() {
        return Singletons.getInstance(EventBusFactory.class);
    }

    private EventBusFactory() {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        init();
    }

    private void init() {
        eventExecutor = new ThreadPoolExecutor(kylinConfig.getEventBusHandleThreadCount(),
                kylinConfig.getEventBusHandleThreadCount(), 300L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new NamedThreadFactory("SchedulerEventBus"));
        broadcastExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("BroadcastEventBus"));
        eventExecutor.allowCoreThreadTimeOut(true);
        asyncEventBus = new AsyncEventBus(eventExecutor);
        syncEventBus = new SyncThrowExceptionEventBus();
        broadcastEventBus = new AsyncEventBus(broadcastExecutor);
        serviceEventBus = new SyncThrowExceptionEventBus();
    }

    public void registerBroadcast(Object broadcastListener) {
        broadcastEventBus.register(broadcastListener);
    }

    public void register(Object listener, boolean isSync) {
        if (isSync) {
            syncEventBus.register(listener);
        } else {
            asyncEventBus.register(listener);
        }
    }

    public void registerService(Object listener) {
        serviceEventBus.register(listener);
    }

    public void unregister(Object listener) {
        try {
            asyncEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
        try {
            syncEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
        try {
            broadcastEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
    }

    public void unregisterService(Object listener) {
        try {
            serviceEventBus.unregister(listener);
        } catch (IllegalArgumentException ignore) {
            // ignore it
        }
    }

    public void postWithLimit(SchedulerEventNotifier event) {
        rateLimiters.putIfAbsent(event.toString(), RateLimiter.create(kylinConfig.getSchedulerLimitPerMinute() / 60.0));
        RateLimiter rateLimiter = rateLimiters.get(event.toString());

        if (rateLimiter.tryAcquire()) {
            postAsync(event);
        }
    }

    public void postAsync(SchedulerEventNotifier event) {
        log.debug("Post event {} async", event);
        if (event instanceof BroadcastEventReadyNotifier) {
            broadcastEventBus.post(event);
        } else {
            asyncEventBus.post(event);
        }
    }

    public void postSync(Object event) {
        log.debug("Post event {} sync", event);
        syncEventBus.post(event);
    }

    public void callService(Object event) {
        log.debug("Post Service event {} sync", event);
        serviceEventBus.post(event);
    }

    @VisibleForTesting
    public void restart() {
        stopThreadPool(eventExecutor);
        stopThreadPool(broadcastExecutor);
        init();
    }

    private void stopThreadPool(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(6000, TimeUnit.SECONDS)) {
                ExecutorServiceUtil.forceShutdown(executor);
            }
        } catch (InterruptedException ex) {
            ExecutorServiceUtil.forceShutdown(executor);
            Thread.currentThread().interrupt();
        }
    }
}
