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

package org.apache.kylin.job.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.shaded.curator.org.apache.curator.test.TestingServer;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.job.exception.ZkPeekLockInterruptException;
import org.apache.kylin.job.exception.ZkReleaseLockException;
import org.apache.kylin.job.exception.ZkReleaseLockInterruptException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ZookeeperDistributedLockTest extends NLocalFileMetadataTestCase {

    private static final String ZK_PFX = "/test/ZookeeperDistributedLockTest/" + new Random().nextInt(10000000);

    static ZookeeperDistributedLock.Factory factory;
    private TestingServer zkTestServer;

    @Before
    public void setup() throws Exception {
        zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        createTestMetadata();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl("/zklock");
        factory = new ZookeeperDistributedLock.Factory();
    }

    @After
    public void after() throws Exception {
        factory.lockForCurrentProcess().purgeLocks(ZK_PFX);
        zkTestServer.close();
        cleanupTestMetadata();
        System.clearProperty("kylin.env.zookeeper-connect-string");
    }

    @Test
    public void testLockCurrentThread() {
        DistributedLock lock = factory.lockForCurrentThread();
        String path = ZK_PFX + "/test_lock_current_thread";

        assertFalse(lock.isLocked(path));
        assertTrue(lock.lock(path));
        assertTrue(lock.lock(path));
        assertTrue(lock.lock(path));
        assertEquals(lock.getClient(), lock.peekLock(path));
        assertTrue(lock.isLocked(path));
        assertTrue(lock.isLockedByMe(path));
        lock.unlock(path);
        assertFalse(lock.isLocked(path));
    }

    @Test
    public void testLockForClients() {
        String client1 = "client1";
        String client2 = "client2";
        DistributedLock lock1 = factory.lockForClient(client1);
        DistributedLock lock2 = factory.lockForClient(client2);

        String path = ZK_PFX + "/test_lock_for_clients";
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertTrue(lock1.lock(path));
        assertTrue(lock1.lock(path));
        assertFalse(lock2.lock(path));
        assertFalse(lock2.lock(path));

        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        lock1.unlock(path);
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        assertTrue(lock2.lock(path));
        assertTrue(lock2.lock(path));
        assertFalse(lock1.lock(path));
        assertFalse(lock1.lock(path));

        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertTrue(lock2.isLockedByMe(path));

        lock2.unlock(path);
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));
    }

    @Test
    public void testBlockingLock() {
        String client1 = "client1";
        String client2 = "client2";
        DistributedLock lock1 = factory.lockForClient(client1);
        DistributedLock lock2 = factory.lockForClient(client2);

        String path = ZK_PFX + "/test_lock_for_blocking";
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertTrue(lock1.lock(path));
        assertTrue(lock1.lock(path));
        assertFalse(lock2.lock(path));
        assertFalse(lock2.lock(path));

        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        long timeout = 5000L; // 5s
        long start = System.currentTimeMillis();
        boolean gotLock = lock2.lock(path, timeout);
        long timePass = System.currentTimeMillis() - start;
        assertFalse(gotLock);
        assertTrue(timePass > timeout);

        lock1.unlock(path);
        start = System.currentTimeMillis();
        gotLock = lock2.lock(path, timeout);
        timePass = System.currentTimeMillis() - start;
        assertTrue(gotLock);
        assertTrue(timePass < timeout);
        lock2.unlock(path);
    }

    @Test
    public void testSingleClientLockWhenCatchInterruptException() {
        String path = ZK_PFX + "/test_interrupt_lock";
        DistributedLock lock = factory.lockForClient("client");
        DistributedLock spy = Mockito.spy(lock);
        // mock interruptException when peekLock only once
        Mockito.when(spy.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy.lock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkPeekLockInterruptException);
        }
        // should release lock
        Mockito.reset(spy);
        assertFalse(lock.isLocked(path));
    }

    @Test
    public void testTwoClientLockWhenCatchInterruptException() {
        String path = ZK_PFX + "/test_interrupt_lock";
        DistributedLock lock1 = factory.lockForClient("client_1");
        DistributedLock lock2 = factory.lockForClient("client_2");
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));

        // lock first by client_1
        assertTrue(lock1.lock(path));
        assertFalse(lock2.lock(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_2 to simulate lock when an InterruptException caught
        DistributedLock spy2 = Mockito.spy(lock2);
        // mock interruptException when peekLock only once
        Mockito.when(spy2.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy2.lock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkPeekLockInterruptException);
        }
        // should not release lock because lock was held by client_1
        Mockito.reset(spy2);
        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_1 to simulate lock when an InterruptException caught
        DistributedLock spy1 = Mockito.spy(lock1);
        // mock interruptException when peekLock only once
        Mockito.when(spy1.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy1.lock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkPeekLockInterruptException);
        }

        // should release lock because lock was held by client_1
        Mockito.reset(spy1);
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));
    }

    @Test
    public void testSingleClientUnlockWhenCatchInterruptExceptionOnPeekLock() {
        String path = ZK_PFX + "/test_interrupt_lock";
        DistributedLock lock = factory.lockForClient("client");

        assertFalse(lock.isLocked(path));
        assertTrue(lock.lock(path));
        assertTrue(lock.isLocked(path));
        assertTrue(lock.isLockedByMe(path));

        DistributedLock spy = Mockito.spy(lock);
        // mock interruptException when peekLock only once
        Mockito.when(spy.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkPeekLockInterruptException);
        }
        // should release lock
        Mockito.reset(spy);
        assertFalse(lock.isLocked(path));
    }

    @Test
    public void testTwoClientUnlockWhenCatchInterruptExceptionOnPeekLock() {
        String path = ZK_PFX + "/test_interrupt_lock";
        DistributedLock lock1 = factory.lockForClient("client_1");
        DistributedLock lock2 = factory.lockForClient("client_2");
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));

        // lock first by client_1
        assertTrue(lock1.lock(path));
        assertFalse(lock2.lock(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_2 to simulate lock when an InterruptException caught
        DistributedLock spy2 = Mockito.spy(lock2);
        // mock interruptException when peekLock only once
        Mockito.when(spy2.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy2.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkReleaseLockException expected: lock was held by client_1
            assertTrue(e instanceof ZkReleaseLockException);
        }
        // should not release lock because lock was held by client_1
        Mockito.reset(spy2);
        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_1 to simulate lock when an InterruptException caught
        DistributedLock spy1 = Mockito.spy(lock1);
        // mock interruptException when peekLock only once
        Mockito.when(spy1.peekLock(Mockito.anyString())).thenThrow(new ZkPeekLockInterruptException("mock interrupt"))
                .thenCallRealMethod();
        try {
            spy1.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkPeekLockInterruptException);
        }

        // should release lock because lock was held by client_1
        Mockito.reset(spy1);
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));
    }

    @Test
    public void testSingleClientUnlockWhenCatchInterruptExceptionOnPurgeLock() {
        String path = ZK_PFX + "/test_interrupt_lock";
        ZookeeperDistributedLock lock = (ZookeeperDistributedLock) factory.lockForClient("client");

        assertFalse(lock.isLocked(path));
        assertTrue(lock.lock(path));
        assertTrue(lock.isLocked(path));
        assertTrue(lock.isLockedByMe(path));

        ZookeeperDistributedLock spy = Mockito.spy(lock);
        // mock interruptException when purgeLock only once
        Mockito.doThrow(new ZkReleaseLockInterruptException("mock interrupt")).doCallRealMethod().when(spy)
                .purgeLockInternal(Mockito.anyString());
        try {
            spy.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkReleaseLockInterruptException);
        }
        // should release lock
        Mockito.reset(spy);
        assertFalse(lock.isLocked(path));
    }

    @Test
    public void testTwoClientUnlockWhenCatchInterruptExceptionOnPurgeLock() {
        String path = ZK_PFX + "/test_interrupt_lock";
        ZookeeperDistributedLock lock1 = (ZookeeperDistributedLock) factory.lockForClient("client_1");
        ZookeeperDistributedLock lock2 = (ZookeeperDistributedLock) factory.lockForClient("client_2");
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));

        // lock first by client_1
        assertTrue(lock1.lock(path));
        assertFalse(lock2.lock(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_2 to simulate lock when an InterruptException caught
        ZookeeperDistributedLock spy2 = Mockito.spy(lock2);
        // mock interruptException when purgeLock only once
        Mockito.doThrow(new ZkReleaseLockInterruptException("mock interrupt")).doCallRealMethod().when(spy2)
                .purgeLockInternal(Mockito.anyString());
        try {
            spy2.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkReleaseLockException expected: lock was held by client_1
            assertTrue(e instanceof ZkReleaseLockException);
        }
        // should not release lock because lock was held by client_1
        Mockito.reset(spy2);
        assertTrue(lock1.isLocked(path));
        assertTrue(lock2.isLocked(path));
        assertTrue(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));

        // mock lock for client_1 to simulate lock when an InterruptException caught
        ZookeeperDistributedLock spy1 = Mockito.spy(lock1);
        // mock interruptException when purgeLock only once
        Mockito.doThrow(new ZkReleaseLockInterruptException("mock interrupt")).doCallRealMethod().when(spy1)
                .purgeLockInternal(Mockito.anyString());
        try {
            spy1.unlock(path);
            fail("should throw exception");
        } catch (Exception e) {
            // ZkPeekLockInterruptException expected
            assertTrue(e instanceof ZkReleaseLockInterruptException);
        }

        // should release lock because lock was held by client_1
        Mockito.reset(spy1);
        assertFalse(lock1.isLocked(path));
        assertFalse(lock2.isLocked(path));
        assertFalse(lock1.isLockedByMe(path));
        assertFalse(lock2.isLockedByMe(path));
    }

    @Test
    public void testLockJobEngine() {
        String client1 = "client1";
        String client2 = "client2";
        ZookeeperDistributedLock lock1 = (ZookeeperDistributedLock) factory.lockForClient(client1);
        ZookeeperDistributedLock lock2 = (ZookeeperDistributedLock) factory.lockForClient(client2);

        assertTrue(lock1.lockJobEngine());
        assertTrue(lock1.lockJobEngine());
        assertFalse(lock2.lockJobEngine());
        lock1.unlockJobEngine();
        assertTrue(lock2.lockJobEngine());
        assertTrue(lock2.lockJobEngine());
        assertFalse(lock1.lockJobEngine());
        lock2.unlockJobEngine();

    }
}
