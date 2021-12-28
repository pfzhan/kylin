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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.kylin.common.Singletons;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EpochUpdateLockManager {

    private final Map<String, ReentrantLock> epochLockCache;

    private EpochUpdateLockManager() {
        epochLockCache = Maps.newConcurrentMap();
    }

    public interface Callback<T> {
        T handle() throws Exception;
    }

    private Lock getLockFromCache(String project) throws ExecutionException {
        if (!epochLockCache.containsKey(project)) {
            synchronized (epochLockCache) {
                epochLockCache.putIfAbsent(project, new ReentrantLock(true));
            }
        }
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

    @VisibleForTesting
    static Lock getLock(String project) {
        try {
            return EpochUpdateLockManager.getInstance().getLockFromCache(project);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    @VisibleForTesting
    static List<Lock> getLock(@Nonnull List<String> projects) {
        Preconditions.checkNotNull(projects, "projects is null");

        val sortedProjects = new ArrayList<>(projects);
        sortedProjects.sort(Comparator.naturalOrder());
        return sortedProjects.stream().map(EpochUpdateLockManager::getLock).collect(Collectors.toList());
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

    /**
     * be carefully to avoid dead lock
     * @param projects you should
     * @param callback
     * @param <T>
     * @return
     */
    static <T> T executeEpochWithLock(@Nonnull List<String> projects, @Nonnull Callback<T> callback) {
        Preconditions.checkNotNull(projects, "projects is null");
        Preconditions.checkNotNull(callback, "callback is null");

        if (projects.size() == 1) {
            return executeEpochWithLock(projects.get(0), callback);
        }
        List<Lock> locks = getLock(projects);

        Preconditions.checkState(locks.stream().allMatch(Objects::nonNull), "locks from cache is null");

        try {
            locks.forEach(Lock::lock);
            return callback.handle();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            Collections.reverse(locks);
            locks.forEach(Lock::unlock);
        }
    }

}
