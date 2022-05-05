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

package org.apache.kylin.common.lock;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static io.kyligence.kap.common.util.TestUtils.getTestConfig;
import static org.awaitility.Awaitility.await;

@Slf4j
public class DistributedLockFactoryTest {

    public void testConcurrence(String key, int threadNum, int times) throws Exception {
        threadNum = Math.max(2, threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);

        final CountDownLatch tasks = new CountDownLatch(threadNum);

        int[] count = new int[] {0};

        for (int i = 0; i < threadNum; i++) {
            executorService.submit(new DirtyReadTest(key, times, tasks, count));
        }

        await().atMost(20, TimeUnit.SECONDS).until(() -> tasks.getCount() == 0);
        Assert.assertEquals(threadNum * times, count[0]);
    }

    static class DirtyReadTest implements Runnable {
        private final int times;
        private final int[] count;
        private final CountDownLatch countDownLatch;
        private final String key;

        DirtyReadTest(String key, int times, CountDownLatch countDownLatch, int[] count) {
            this.times = times;
            this.count = count;
            this.key = key;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            Lock lock = null;
            try {
                lock = getTestConfig().getDistributedLockFactory().getLockForCurrentThread(key);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            for (int i = 0; i < times; i++) {
                try {
                    lock.lock();
                    int v = count[0];
                    await().atLeast(1, TimeUnit.MILLISECONDS);
                    count[0] = v + 1;
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                } finally {
                    lock.unlock();
                }
            }
            countDownLatch.countDown();
        }
    }
}
