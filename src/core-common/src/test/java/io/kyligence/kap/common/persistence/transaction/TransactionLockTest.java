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

import java.util.concurrent.TimeUnit;

import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionLockTest {

    @Test
    public void testStravition() {
        val threads = Lists.<LogicThread> newArrayList();

        for (int i = 0; i < 10; i++) {
            val t = new LogicThread(i % 4 != 0);
            t.start();
            threads.add(t);
        }

        Awaitility.await().atMost(2, TimeUnit.MINUTES)
                .until(() -> threads.stream().map(t -> t.getCount() > 10).reduce(true, (l, r) -> l && r));

        long maxCount = threads.stream().mapToLong(LogicThread::getCount).max().getAsLong();
        long minCount = threads.stream().mapToLong(LogicThread::getCount).min().getAsLong();
        Assert.assertTrue(maxCount - minCount <= 1);

        TransactionLock.projectLocks = Maps.newConcurrentMap();
        for (LogicThread thread : threads) {
            thread.interrupt();
        }

    }

    @RequiredArgsConstructor
    static class LogicThread extends Thread {

        final boolean isReader;

        @Getter
        long count = 0;

        @Override
        public void run() {
            super.run();
            val tLock = TransactionLock.getLock("p0", isReader);
            while (count < 20) {
                tLock.lock();
                log.debug("lock by {} {}, {}", isReader ? "reader" : "writer", Thread.currentThread().getName(), count);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    tLock.unlock();
                    count++;
                }
            }
        }
    }

}
