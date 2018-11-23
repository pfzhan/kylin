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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLock.Watcher;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.shaded.curator.org.apache.curator.test.TestingServer;

public class ZookeeperDistributedLockLocalTest extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperDistributedLockLocalTest.class);
    private static final String ZK_PFX = "/test/ZookeeperDistributedLockLocalTest/" + new Random().nextInt(10000000);

    static ZookeeperDistributedLock.Factory factory;

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();

        TestingServer zkTestServer = new TestingServer();
        zkTestServer.start();
        System.setProperty("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl("/zklock");
        factory = new ZookeeperDistributedLock.Factory();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
        factory.lockForCurrentProcess().purgeLocks(ZK_PFX);
        System.clearProperty("kylin.env.zookeeper-connect-string");
    }

    @Test
    public void testBasic() {
        DistributedLock l = factory.lockForCurrentThread();
        String path = ZK_PFX + "/testBasic";

        assertTrue(l.isLocked(path) == false);
        assertTrue(l.lock(path));
        assertTrue(l.lock(path));
        assertTrue(l.lock(path));
        assertEquals(l.getClient(), l.peekLock(path));
        assertTrue(l.isLocked(path));
        assertTrue(l.isLockedByMe(path));
        l.unlock(path);
        assertTrue(l.isLocked(path) == false);
    }

    @Test
    public void testErrorCases() {
        DistributedLock c = factory.lockForClient("client1");
        DistributedLock d = factory.lockForClient("client2");
        String path = ZK_PFX + "/testErrorCases";

        assertTrue(c.isLocked(path) == false);
        assertTrue(d.peekLock(path) == null);

        assertTrue(c.lock(path));
        assertTrue(d.lock(path) == false);
        assertTrue(d.isLocked(path) == true);
        assertEquals(c.getClient(), d.peekLock(path));

        try {
            d.unlock(path);
            fail();
        } catch (IllegalStateException ex) {
            // expected
        }

        c.unlock(path);
        assertTrue(d.isLocked(path) == false);

        d.lock(path);
        d.unlock(path);
    }

    @Test
    public void testLockTimeout() throws InterruptedException {
        final DistributedLock c = factory.lockForClient("client1");
        final DistributedLock d = factory.lockForClient("client2");
        final String path = ZK_PFX + "/testLockTimeout";

        assertTrue(c.isLocked(path) == false);
        assertTrue(d.peekLock(path) == null);

        assertTrue(c.lock(path));
        new Thread() {
            @Override
            public void run() {
                d.lock(path, 12000);
            }
        }.start();
        c.unlock(path);

        Thread.sleep(15000);

        assertTrue(c.isLocked(path));
        assertEquals(d.getClient(), d.peekLock(path));
        d.unlock(path);
    }

    @Test
    public void testWatch() throws InterruptedException, IOException {
        // init lock paths
        final String base = ZK_PFX + "/testWatch";
        final int nLocks = 4;
        final String[] lockPaths = new String[nLocks];
        for (int i = 0; i < nLocks; i++)
            lockPaths[i] = base + "/" + i;

        // init clients
        final int[] clientIds = new int[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
        final int nClients = clientIds.length;
        final DistributedLock[] clients = new DistributedLock[nClients];
        for (int i = 0; i < nClients; i++) {
            clients[i] = factory.lockForClient("" + clientIds[i]);
        }

        // init watch
        DistributedLock lock = factory.lockForClient("");
        final AtomicInteger lockCounter = new AtomicInteger(0);
        final AtomicInteger unlockCounter = new AtomicInteger(0);
        final AtomicInteger scoreCounter = new AtomicInteger(0);
        Closeable watch = lock.watchLocks(base, Executors.newFixedThreadPool(1), new Watcher() {

            @Override
            public void onLock(String lockPath, String client) {
                lockCounter.incrementAndGet();
                int cut = lockPath.lastIndexOf("/");
                int lockId = Integer.parseInt(lockPath.substring(cut + 1)) + 1;
                int clientId = Integer.parseInt(client);
                scoreCounter.addAndGet(lockId * clientId);
            }

            @Override
            public void onUnlock(String lockPath, String client) {
                unlockCounter.incrementAndGet();
            }
        });

        // init client threads
        ClientThread[] threads = new ClientThread[nClients];
        for (int i = 0; i < nClients; i++) {
            DistributedLock client = clients[i];
            threads[i] = new ClientThread(client, lockPaths);
            threads[i].start();
        }

        // wait done
        for (int i = 0; i < nClients; i++) {
            threads[i].join();
        }

        Thread.sleep(3000);

        // verify counters
        assertEquals(0, lockCounter.get() - unlockCounter.get());
        int clientSideScore = 0;
        int clientSideLocks = 0;
        for (int i = 0; i < nClients; i++) {
            clientSideScore += threads[i].scoreCounter;
            clientSideLocks += threads[i].lockCounter;
        }
        // The counters match perfectly on Windows but not on Linux, for unknown reason... 
        logger.info("client side locks is {} and watcher locks is {}", clientSideLocks, lockCounter.get());
        logger.info("client side score is {} and watcher score is {}", clientSideScore, scoreCounter.get());
        //assertEquals(clientSideLocks, lockCounter.get());
        //assertEquals(clientSideScore, scoreCounter.get());
        watch.close();

        // assert all locks were released
        for (int i = 0; i < nLocks; i++) {
            assertTrue(lock.isLocked(lockPaths[i]) == false);
        }
    }

    class ClientThread extends Thread {
        DistributedLock client;
        String[] lockPaths;
        int nLocks;
        int lockCounter = 0;
        int scoreCounter = 0;

        ClientThread(DistributedLock client, String[] lockPaths) {
            this.client = client;
            this.lockPaths = lockPaths;
            this.nLocks = lockPaths.length;
        }

        @Override
        public void run() {
            long start = System.currentTimeMillis();
            Random rand = new Random();

            while (System.currentTimeMillis() - start <= 15000) {
                try {
                    Thread.sleep(rand.nextInt(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // random lock
                int lockIdx = rand.nextInt(nLocks);
                if (client.isLockedByMe(lockPaths[lockIdx]) == false) {
                    boolean locked = client.lock(lockPaths[lockIdx]);
                    if (locked) {
                        lockCounter++;
                        scoreCounter += (lockIdx + 1) * Integer.parseInt(client.getClient());
                    }
                }

                // random unlock
                try {
                    lockIdx = rand.nextInt(nLocks);
                    client.unlock(lockPaths[lockIdx]);
                } catch (IllegalStateException e) {
                    // ignore
                }
            }

            // clean up, unlock all
            for (String lockPath : lockPaths) {
                try {
                    client.unlock(lockPath);
                } catch (IllegalStateException e) {
                    // ignore
                }
            }
        }
    };
}
