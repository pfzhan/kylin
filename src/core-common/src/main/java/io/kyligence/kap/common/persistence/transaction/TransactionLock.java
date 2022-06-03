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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import lombok.experimental.Delegate;

public class TransactionLock {

    static Map<String, Pair<ReentrantReadWriteLock, AtomicLong>> projectLocks = Maps.newConcurrentMap();
    private static final Logger logger = LoggerFactory.getLogger(TransactionLock.class);

    public static TransactionLock getLock(String project, boolean readonly) {
        Pair<ReentrantReadWriteLock, AtomicLong> lockPair = projectLocks.get(project);
        if (lockPair == null) {
            synchronized (UnitOfWork.class) {
                lockPair = projectLocks.computeIfAbsent(project,
                        k -> Pair.newPair(new ReentrantReadWriteLock(true), new AtomicLong()));
            }
        }
        lockPair.getSecond().incrementAndGet();
        ReentrantReadWriteLock lock = lockPair.getFirst();
        return new TransactionLock(lock, readonly ? lock.readLock() : lock.writeLock());
    }

    public static void removeLock(String project) {
        Pair<ReentrantReadWriteLock, AtomicLong> lockPair = projectLocks.get(project);
        if (lockPair != null) {
            synchronized (UnitOfWork.class) {
                AtomicLong atomicLong = lockPair.getSecond();
                if (atomicLong.decrementAndGet() == 0) {
                    projectLocks.remove(project);
                }

            }
        }
        if (projectLocks.size() > 5000L) {
            projectLocks.forEach((k, v) -> logger.warn("lock leaks lock: {}，num: {}", k, v.getSecond()));
        }
    }

    public static boolean isWriteLocked(String project) {
        ReentrantReadWriteLock lock = projectLocks.get(project).getFirst();
        return lock != null && lock.isWriteLocked();
    }

    private ReentrantReadWriteLock rootLock;

    @Delegate
    private Lock lock;

    public TransactionLock(ReentrantReadWriteLock rootLock, Lock lock) {
        this.rootLock = rootLock;
        this.lock = lock;
    }

    boolean isHeldByCurrentThread() {
        if (lock instanceof ReentrantReadWriteLock.ReadLock) {
            return rootLock.getReadHoldCount() > 0;
        } else {
            return rootLock.getWriteHoldCount() > 0;
        }
    }

    public static Map<String, ReentrantReadWriteLock> getProjectLocksForRead() {
        Map<String, ReentrantReadWriteLock> map = Maps.newConcurrentMap();
        projectLocks.forEach((k, v) -> map.put(k, v.getFirst()));
        return Collections.unmodifiableMap(map);
    }
}
