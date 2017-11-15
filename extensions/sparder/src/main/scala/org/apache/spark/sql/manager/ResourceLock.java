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

package org.apache.spark.sql.manager;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

public class ResourceLock {
    private final ConcurrentHashMap<String, LockContext> lockedResources = new ConcurrentHashMap<>();
    private Long rowLockWaitDuration = 10000L; //todo config

    public Lock getLockInterna(String sourcePath) throws IOException, InterruptedException {
        LockContext lockContext = new LockContext(sourcePath);

        while (true) {
            LockContext existingContext = lockedResources.putIfAbsent(sourcePath, lockContext);
            if (existingContext == null) {
                // Row is not already locked by any thread, use newly created context.
                break;
            } else if (existingContext.ownedByCurrentThread()) {
                // Row is already locked by current thread, reuse existing context instead.
                lockContext = existingContext;
                break;
            } else {
                // Row is already locked by some other thread, give up or wait for it
                if (!existingContext.latch.await(this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Timed out waiting for lock for row: " + sourcePath);
                }
            }
        }

        // allocate new lock for this thread
        return lockContext.newLock();
    }

    class LockContext {
        private final String sourcePath;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Thread thread;
        private int lockCount = 0;

        LockContext(String sourcePath) {
            this.sourcePath = sourcePath;
            this.thread = Thread.currentThread();
        }

        boolean ownedByCurrentThread() {
            return thread == Thread.currentThread();
        }

        Lock newLock() {
            lockCount++;
            return new Lock(this);
        }

        void releaseLock() {
            if (!ownedByCurrentThread()) {
                throw new IllegalArgumentException("Lock held by thread: " + thread
                        + " cannot be released by different thread: " + Thread.currentThread());
            }
            lockCount--;
            if (lockCount == 0) {
                // no remaining locks by the thread, unlock and allow other threads to access
                LockContext existingContext = lockedResources.remove(sourcePath);
                if (existingContext != this) {
                    throw new RuntimeException(
                            "Internal sourcePath lock state inconsistent, should not happen, sourcePath: "
                                    + sourcePath);
                }
                latch.countDown();
            }
        }
    }

    public static class Lock {
        @VisibleForTesting
        final LockContext context;
        private boolean released = false;

        @VisibleForTesting
        Lock(LockContext context) {
            this.context = context;
        }

        /**
         * Release the given lock.  If there are no remaining locks held by the current thread
         * then unlock the sourcePath and allow other threads to acquire the lock.
         *
         * @throws IllegalArgumentException if called by a different thread than the lock owning thread
         */
        public void release() {
            if (!released) {
                context.releaseLock();
                released = true;
            }
        }
    }
}
