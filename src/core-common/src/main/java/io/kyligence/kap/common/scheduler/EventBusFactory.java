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
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.NamedThreadFactory;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.eventbus.AsyncEventBus;
import io.kyligence.kap.guava20.shaded.common.eventbus.EventBus;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBusFactory {
    private final KylinConfig kylinConfig;
    private static EventBus eventBus;
    private static ExecutorService executor;

    static {
        executor = Executors.newCachedThreadPool(new NamedThreadFactory("SchedulerEventBus"));
        eventBus = new AsyncEventBus(executor);
    }

    private final Map<String, RateLimiter> rateLimiters = Maps.newConcurrentMap();

    public static EventBusFactory getInstance() {
        return Singletons.getInstance(EventBusFactory.class);
    }

    private EventBusFactory() {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    public void register(Object listener) {
        eventBus.register(listener);
    }

    public void unRegister(Object listener) {
        eventBus.unregister(listener);
    }

    public void postWithLimit(SchedulerEventNotifier event) {
        rateLimiters.putIfAbsent(event.toString(), RateLimiter.create(kylinConfig.getSchedulerLimitPerMinute() / 60.0));
        RateLimiter rateLimiter = rateLimiters.get(event.toString());

        if (rateLimiter.tryAcquire())
            eventBus.post(event);
    }

    public void postAsync(Object event) {
        log.debug("Post event {}", event);
        eventBus.post(event);
    }

    //for ut
    public static void restart() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(6000, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        executor = Executors.newCachedThreadPool(new NamedThreadFactory("SchedulerEventBus"));
        eventBus = new AsyncEventBus(executor);
    }
}
