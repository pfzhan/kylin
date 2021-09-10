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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.lock.curator;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.kylin.common.util.RandomUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionState;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionStateListener;

public class CuratorDistributedLockFactoryTest extends NLocalFileMetadataTestCase {

    private TestingServer zkTestServer;
    private volatile boolean locked = false;
    private volatile boolean isInterrupted = false;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        zkTestServer = new TestingServer(true);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        zkTestServer.close();
    }

    @Test
    public void testBasic() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_basic/" + RandomUtil.randomUUIDStr();

        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        CuratorDistributedLock lock = getTestConfig().getDistributedLockFactory().lockForCurrentThread(path);

        Assert.assertFalse(lock.isAcquiredInThisThread());
        lock.lock();
        Assert.assertTrue(lock.isAcquiredInThisThread());
        lock.unlock();
        Assert.assertFalse(lock.isAcquiredInThisThread());
    }

    @Test
    public void testInterruptWhenLost() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_interrupt_lost/" + RandomUtil.randomUUIDStr();
        TestingServer zkTestServer2 = new TestingServer(true);

        ExecutorService executorService = Executors.newFixedThreadPool(1);

        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kap.env.zookeeper-max-retries", "1");
        overwriteSystemProp("kap.env.zookeeper-base-sleep-time", "1000");

        executorService.submit(() -> {
            CuratorDistributedLock lock = getTestConfig().getDistributedLockFactory().lockForCurrentThread(path);

            lock.lock();
            locked = true;

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        });

        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);
        Assert.assertFalse(isInterrupted);

        locked = false;
        zkTestServer.stop();

        // zk for thread1 lost
        // thread1 will be interrupted
        await().atMost(20, TimeUnit.SECONDS).until(() -> isInterrupted);

        Assert.assertFalse(locked);

        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer2.getConnectString());
        executorService.submit(() -> {
            CuratorDistributedLock lock = getTestConfig().getDistributedLockFactory().lockForCurrentThread(path);
            lock.lock();
            locked = true;
            lock.unlock();
        });

        // thread1 released the lock
        // thread2 will get the lock
        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);
    }

    @Test
    public void testInterruptWhenSuspended() throws Exception {
        String path = "/test/distributed_lock_factory_test/test_interrupt_suspended/" + RandomUtil.randomUUIDStr();
        TestingServer zkTestServer2 = new TestingServer(true);

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        CuratorDistributedLockFactory lockFactory;
        CuratorDistributedLock lock1;
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kap.env.zookeeper-max-retries", "1");
        overwriteSystemProp("kap.env.zookeeper-base-sleep-time", "1000");
        lockFactory = getTestConfig().getDistributedLockFactory();
        lock1 = lockFactory.lockForCurrentThread(path);
        executorService.submit(() -> {

            lock1.lock();
            locked = true;

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                isInterrupted = true;
            }
        });

        await().atMost(5, TimeUnit.SECONDS).until(() -> locked);

        Assert.assertFalse(isInterrupted);

        ConnectionStateListener listener = (ConnectionStateListener) ReflectionTestUtils.getField(lockFactory,
                "listener");
        CuratorFramework client = (CuratorFramework) ReflectionTestUtils.getField(lockFactory, "client");
        listener.stateChanged(client, ConnectionState.SUSPENDED);

        // zk for thread1 suspended
        // thread1 will be interrupted
        await().atMost(5, TimeUnit.SECONDS).until(() -> isInterrupted);
    }

    @Test
    public void testConcurrence() throws ExecutionException, InterruptedException {
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        String path = "/test/distributed_lock_factory_test/test_concurrence/" + RandomUtil.randomUUIDStr();
        int threadNum = 10;
        int times = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        final CountDownLatch tasks = new CountDownLatch(threadNum);
        List<Future<Integer>> futures = Lists.newArrayListWithCapacity(threadNum);

        for (int i = 0; i < threadNum; i++) {
            futures.add(executorService.submit(new LockCallable(path, 10, tasks)));
        }

        await().atMost(20, TimeUnit.SECONDS).until(() -> tasks.getCount() == 0);

        for (Future<Integer> future : futures) {
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(times, (int) future.get());
        }
    }

    static class LockCallable implements Callable<Integer> {
        private final int times;
        private int acquiredCount;
        private final CountDownLatch countDownLatch;
        private final CuratorDistributedLock lock;

        LockCallable(String path, int times, CountDownLatch countDownLatch) {
            this.times = times;
            this.lock = getTestConfig().getDistributedLockFactory().lockForCurrentThread(path);
            this.countDownLatch = countDownLatch;
        }

        @Override
        public Integer call() throws Exception {
            for (int i = 0; i < times; i++) {
                try {
                    lock.lock();
                    acquiredCount++;
                } finally {
                    lock.unlock();
                }
            }
            countDownLatch.countDown();
            return acquiredCount;
        }
    }
}
