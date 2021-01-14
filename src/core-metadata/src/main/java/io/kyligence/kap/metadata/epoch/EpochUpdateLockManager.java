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
package io.kyligence.kap.metadata.epoch;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EpochUpdateLockManager {

    private final LoadingCache<String, ReentrantLock> epochLockCache;

    private EpochUpdateLockManager() {
        long epochExpiredTime = KylinConfig.getInstanceFromEnv().getEpochExpireTimeSecond();

        epochLockCache = CacheBuilder.newBuilder().expireAfterAccess(epochExpiredTime, TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, ReentrantLock>) notification -> log
                        .warn("epoch lock: {}, is expired after: {} seconds", notification.getKey(), epochExpiredTime))
                .build(new CacheLoader<String, ReentrantLock>() {
                    @Override
                    public ReentrantLock load(@Nonnull String project) {
                        log.debug("load new epoch lock: {}, size: {}", project, getLockCacheSize() + 1);
                        return new ReentrantLock(true);
                    }
                });
    }

    public interface Callback<T> {
        T handle() throws Exception;
    }

    private Lock getLockFromCache(String project) throws ExecutionException {
        return epochLockCache.get(project);
    }

    @VisibleForTesting
    static EpochUpdateLockManager getInstance() {
        return Singletons.getInstance(EpochUpdateLockManager.class, clz -> {
            try {
                return new EpochUpdateLockManager();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @VisibleForTesting
    long getLockCacheSize() {
        return epochLockCache.size();
    }

    /**
     * clean up cache that is expired,
     * only for test
     */
    @VisibleForTesting
    static void cleanUp() {
        getInstance().epochLockCache.cleanUp();
    }

    @VisibleForTesting
    static Lock getLock(String project) {
        try {
            return EpochUpdateLockManager.getInstance().getLockFromCache(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    static <T> T executeEpochWithLock(@Nonnull String project, @Nonnull Callback<T> callback) {
        Preconditions.checkNotNull(project, "project is null");
        Preconditions.checkNotNull(callback, "callback is null");

        val lock = getLock(project);

        Preconditions.checkNotNull(lock, "lock from cache is null");

        try {
            lock.lock();
            return callback.handle();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock();
        }
    }

}
