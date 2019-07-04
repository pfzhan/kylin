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
import org.apache.kylin.common.util.NamedThreadFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.RateLimiter;

public class SchedulerEventBusFactory {
    private KylinConfig kylinConfig;
    private static EventBus eventBus;
    private static ExecutorService executor;

    static {
        executor = Executors.newCachedThreadPool(new NamedThreadFactory("SchedulerEventBus"));
        eventBus = new AsyncEventBus(executor);
    }

    private Map<String, RateLimiter> rateLimiters = Maps.newConcurrentMap();

    public static SchedulerEventBusFactory getInstance(KylinConfig kylinConfig) {
        return kylinConfig.getManager(SchedulerEventBusFactory.class);
    }

    static SchedulerEventBusFactory newInstance(KylinConfig kylinConfig) {
        return new SchedulerEventBusFactory(kylinConfig);
    }

    private SchedulerEventBusFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
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

    public void post(Object event) {
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
