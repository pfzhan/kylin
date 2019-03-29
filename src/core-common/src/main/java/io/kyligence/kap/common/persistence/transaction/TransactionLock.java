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

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Maps;

import lombok.val;
import lombok.experimental.Delegate;

public class TransactionLock {

    private static Map<String, ReentrantReadWriteLock> projectLocks = Maps.newConcurrentMap();

    public static TransactionLock getLock(String project) {
        return getLock(project, false);
    }

    public static TransactionLock getLock(String project, boolean readonly) {
        ReentrantReadWriteLock lock = projectLocks.get(project);
        if (lock == null) {
            synchronized (UnitOfWork.class) {
                val cacheLock = projectLocks.get(project);
                if (cacheLock == null) {
                    projectLocks.put(project, new ReentrantReadWriteLock());
                }
            }
        }

        lock = projectLocks.get(project);
        return new TransactionLock(readonly ? lock.readLock() : lock.writeLock());
    }

    @Delegate
    private Lock lock;

    public TransactionLock(Lock lock) {
        this.lock = lock;
    }

    public boolean isHeldByCurrentThread() {
        return lock instanceof ReentrantReadWriteLock.WriteLock
                && ((ReentrantReadWriteLock.WriteLock) lock).isHeldByCurrentThread();
    }
}
