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

package io.kyligence.kap.secondstorage;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.SegmentRange;

import com.google.common.base.Preconditions;


public class SecondStorageLockUtils {
    private static final Map<Pair<String, SegmentRange<Long>>, Lock> JOB_LOCKS = new ConcurrentHashMap<>();
    private static final Object guard = new Object();


    public static boolean containsKey(String modelId, SegmentRange<Long> range) {
        return JOB_LOCKS.keySet().stream().anyMatch(item -> item.getFirst().equals(modelId) && item.getSecond().overlaps(range));
    }

    private static Optional<Pair<String, SegmentRange<Long>>> getOverlapKey(String modelId, SegmentRange<Long> range) {
        return JOB_LOCKS.keySet().stream().filter(item -> item.getFirst().equals(modelId) && item.getSecond().overlaps(range)).findFirst();
    }


    public static Lock acquireLock(String modelId, SegmentRange<Long> range) {
        Preconditions.checkNotNull(modelId);
        Preconditions.checkNotNull(range);
        synchronized (guard) {
            while (containsKey(modelId, range)) {
                Optional<Pair<String, SegmentRange<Long>>> key = getOverlapKey(modelId, range);
                if (key.isPresent()) {
                    Lock lock = JOB_LOCKS.get(key.get());
                    try {
                        if (lock.tryLock(1, TimeUnit.MINUTES)) {
                            lock.unlock();
                            JOB_LOCKS.remove(key.get());
                        } else {
                            break;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            if (containsKey(modelId, range)) {
                throw new IllegalStateException("Can't acquire job lock, job is running now.");
            }
            Preconditions.checkArgument(!containsKey(modelId, range));
            Lock lock = new ReentrantLock();
            JOB_LOCKS.put(new Pair<>(modelId, range), lock);
            return lock;
        }
    }

    public static void unlock(String modelId, SegmentRange<Long> range) {
        Pair<String, SegmentRange<Long>> key = new Pair<>(modelId, range);
        if (!JOB_LOCKS.containsKey(key)) {
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "Logical Error! This is a bug. Lock for model %s:%s is lost", modelId, range));
        }
        JOB_LOCKS.get(key).unlock();
        JOB_LOCKS.remove(key);
    }

    private SecondStorageLockUtils() {
    }
}
