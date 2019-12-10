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

import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.job.exception.ZkAcquireLockException;
import org.apache.kylin.job.exception.ZkPeekLockException;
import org.apache.kylin.job.exception.ZkPeekLockInterruptException;
import org.apache.kylin.job.exception.ZkReleaseLockException;
import org.apache.kylin.job.exception.ZkReleaseLockInterruptException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.common.util.ThrowableUtils;

/**
 * A distributed lock based on zookeeper. Every instance is owned by a client, on whose behalf locks are acquired and/or released.
 *
 * All <code>lockPath</code> will be prefix-ed with "/kylin/metadata-prefix" automatically.
 */
public class ZookeeperDistributedLock implements DistributedLock, JobLock {
    private static Logger logger = LoggerFactory.getLogger(ZookeeperDistributedLock.class);
    private static final Random RANDOM = new Random();

    public static class Factory extends DistributedLockFactory {

        private static final ConcurrentMap<KylinConfig, CuratorFramework> CACHE = new ConcurrentHashMap<>();

        static {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                for (CuratorFramework c : CACHE.values()) {
                    try {
                        c.close();
                    } catch (Exception ex) {
                        logger.error("Error at closing {}", c, ex);
                    }
                }
            }));
        }

        private static CuratorFramework getZKClient(KylinConfig config) {
            CuratorFramework zkClient = CACHE.get(config);
            if (zkClient == null) {
                synchronized (ZookeeperDistributedLock.class) {
                    zkClient = CACHE.get(config);
                    if (zkClient == null) {
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
                        String zkConnectString = getZKConnectString(config);
                        ZookeeperAclBuilder zookeeperAclBuilder = new ZookeeperAclBuilder().invoke();
                        zkClient = zookeeperAclBuilder.setZKAclBuilder(CuratorFrameworkFactory.builder())
                                .connectString(zkConnectString).sessionTimeoutMs(120000).connectionTimeoutMs(15000)
                                .retryPolicy(retryPolicy).build();
                        zkClient.start();
                        CACHE.put(config, zkClient);
                        if (CACHE.size() > 1) {
                            logger.warn("More than one singleton exist");
                        }
                    }
                }
            }
            return zkClient;
        }

        private static String getZKConnectString(KylinConfig config) {
            // the ZKConnectString should come from KylinConfig, however it is taken from HBase configuration at the moment
            return ZookeeperUtil.getZKConnectString(config);
        }

        final String zkPathBase;
        final CuratorFramework curator;

        public Factory() {
            this(KylinConfig.getInstanceFromEnv());
        }

        public Factory(KylinConfig config) {
            this.curator = getZKClient(config);
            this.zkPathBase = fixSlash(config.getZookeeperBasePath() + "/" + config.getMetadataUrlPrefix());
        }

        @Override
        public DistributedLock lockForClient(String client) {
            return new ZookeeperDistributedLock(curator, zkPathBase, client);
        }
    }

    // ============================================================================

    private final CuratorFramework curator;
    private final String zkPathBase;
    private final String client;
    private final byte[] clientBytes;

    private ZookeeperDistributedLock(CuratorFramework curator, String zkPathBase, String client) {
        if (client == null)
            throw new NullPointerException("client must not be null");
        if (zkPathBase == null)
            throw new NullPointerException("zkPathBase must not be null");

        this.curator = curator;
        this.zkPathBase = zkPathBase;
        this.client = client;
        this.clientBytes = client.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String getClient() {
        return client;
    }

    @Override
    public boolean lock(String lockPath) {
        lockPath = norm(lockPath);

        logger.debug("{} trying to lock {}", client, lockPath);
        lockInternal(lockPath);

        String lockOwner;
        try {
            lockOwner = peekLock(lockPath);
            if (client.equals(lockOwner)) {
                logger.info("{} acquired lock at {}", client, lockPath);
                return true;
            } else {
                logger.debug("{} failed to acquire lock at {}, which is held by {}", client, lockPath, lockOwner);
                return false;
            }
        } catch (ZkPeekLockInterruptException zpie) {
            logger.error("{} peek owner of lock interrupt while acquire lock at {}, check to release lock", client,
                    lockPath);
            lockOwner = peekLock(lockPath);

            try {
                unlockInternal(lockOwner, lockPath);
            } catch (Exception anyEx) {
                // it's safe to swallow any exception here because here already been interrupted
                logger.warn("Exception caught to release lock when lock operation has been interrupted.", anyEx);
            }
            throw zpie;
        }
    }

    private void lockInternal(String lockPath) {
        try {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockPath, clientBytes);
        } catch (KeeperException.NodeExistsException ex) {
            logger.debug("{} see {} is already locked", client, lockPath);
        } catch (Exception ex) {
            // don't need to catch interrupt exception when locking, it's safe to throw the exception directly
            throw new ZkAcquireLockException("Error occurs while " + client + " trying to lock " + lockPath, ex);
        }
    }

    @Override
    public boolean lock(String lockPath, long timeout) {
        lockPath = norm(lockPath);

        if (lock(lockPath))
            return true;

        if (timeout <= 0)
            timeout = Long.MAX_VALUE;

        logger.debug("{} will wait for lock path {}", client, lockPath);
        long waitStart = System.currentTimeMillis();
        long sleep = 10 * 1000L; // 10 seconds

        while (System.currentTimeMillis() - waitStart <= timeout) {
            try {
                Thread.sleep((long) (1000 + sleep * RANDOM.nextDouble()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }

            if (lock(lockPath)) {
                logger.debug("{} waited {} ms for lock path {}", client, System.currentTimeMillis() - waitStart,
                        lockPath);
                return true;
            }
        }

        // timeout
        return false;
    }

    /**
     * Need to handle interrupt exception when using this peekLock during unlock
     */
    @Override
    public String peekLock(String lockPath) {
        try {
            return peekLockInternal(lockPath);
        } catch (Exception ex) {
            if (ThrowableUtils.isInterruptedException(ex)) {
                throw new ZkPeekLockInterruptException("Peeking owner of lock was interrupted at" + lockPath, ex);
            } else {
                throw new ZkPeekLockException("Error while peeking at " + lockPath, ex);
            }
        }
    }

    private String peekLockInternal(String lockPath) throws Exception {
        lockPath = norm(lockPath);
        try {
            byte[] bytes = curator.getData().forPath(lockPath);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException ex) {
            return null;
        }
    }

    @Override
    public boolean isLocked(String lockPath) {
        return peekLock(lockPath) != null;
    }

    @Override
    public boolean isLockedByMe(String lockPath) {
        return client.equals(peekLock(lockPath));
    }

    @Override
    public void unlock(String lockPath) {
        lockPath = norm(lockPath);
        logger.debug("{} trying to unlock {}", client, lockPath);

        // peek owner first
        String owner;
        ZkPeekLockInterruptException peekLockInterruptException = null;
        try {
            owner = peekLock(lockPath);
        } catch (ZkPeekLockInterruptException zie) {
            // re-peek owner of lock when interrupted
            owner = peekLock(lockPath);
            peekLockInterruptException = zie;
        } catch (ZkPeekLockException ze) {
            // this exception should be thrown to diagnose even it may cause unlock failed
            logger.error("{} failed to peekLock when unlock at {}", client, lockPath, ze);
            throw ze;
        }

        // then unlock
        ZkReleaseLockInterruptException unlockInterruptException = null;
        try {
            unlockInternal(owner, lockPath);
        } catch (ZkReleaseLockInterruptException zlie) {
            // re-unlock once when interrupted
            unlockInternal(owner, lockPath);
            unlockInterruptException = zlie;
        } catch (Exception ex) {
            throw new ZkReleaseLockException("Error while " + client + " trying to unlock " + lockPath, ex);
        }

        // need re-throw interrupt exception to avoid swallowing it
        if (peekLockInterruptException != null) {
            throw peekLockInterruptException;
        }
        if (unlockInterruptException != null) {
            throw unlockInterruptException;
        }
    }

    /**
     * May throw ZkReleaseLockException or ZkReleaseLockInterruptException
     */
    private void unlockInternal(String owner, String lockPath) {
        // only unlock the lock belongs itself
        if (owner == null)
            throw new IllegalStateException(
                    client + " cannot unlock path " + lockPath + " which is not locked currently");
        if (!client.equals(owner))
            throw new IllegalStateException(
                    client + " cannot unlock path " + lockPath + " which is locked by " + owner);
        purgeLockInternal(lockPath);
        logger.info("{} released lock at {}", client, lockPath);
    }

    @Override
    public void purgeLocks(String lockPathRoot) {
        lockPathRoot = norm(lockPathRoot);

        try {
            purgeLockInternal(lockPathRoot);
            logger.info("{} purged all locks under {}", client, lockPathRoot);
        } catch (ZkReleaseLockException zpe) {
            throw zpe;
        } catch (ZkReleaseLockInterruptException zpie) {
            // re-purge lock once when interrupted
            purgeLockInternal(lockPathRoot);
            throw zpie;
        }
    }

    @VisibleForTesting
    void purgeLockInternal(String lockPath) {
        try {
            curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);
        } catch (KeeperException.NoNodeException ex) {
            // it's safe to purge a lock when there is no node found in lockPath
            logger.warn("No node found when purge lock in Lock path: {}", lockPath, ex);
        } catch (Exception e) {
            if (ThrowableUtils.isInterruptedException(e))
                throw new ZkReleaseLockInterruptException("Purge lock was interrupted at " + lockPath, e);
            else
                throw new ZkReleaseLockException("Error while " + client + " trying to purge " + lockPath, e);
        }
    }

    @Override
    public Closeable watchLocks(String lockPathRoot, Executor executor, final Watcher watcher) {
        lockPathRoot = norm(lockPathRoot);

        PathChildrenCache cache = new PathChildrenCache(curator, lockPathRoot, true);
        try {
            cache.start();
            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                    case CHILD_ADDED:
                        watcher.onLock(event.getData().getPath(),
                                new String(event.getData().getData(), StandardCharsets.UTF_8));
                        break;
                    case CHILD_REMOVED:
                        watcher.onUnlock(event.getData().getPath(),
                                new String(event.getData().getData(), StandardCharsets.UTF_8));
                        break;
                    default:
                        break;
                    }
                }
            }, executor);
        } catch (Exception ex) {
            logger.error("Error to watch lock path " + lockPathRoot, ex);
        }
        return cache;
    }

    // normalize lock path
    private String norm(String lockPath) {
        if (!lockPath.startsWith(zkPathBase))
            lockPath = zkPathBase + (lockPath.startsWith("/") ? "" : "/") + lockPath;
        return fixSlash(lockPath);
    }

    private static String fixSlash(String path) {
        if (!path.startsWith("/"))
            path = File.separator + path;
        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 1);
        int n = Integer.MAX_VALUE;
        while (n > path.length()) {
            n = path.length();
            path = path.replace("//", "/");
        }
        return path;
    }

    // ============================================================================

    @Override
    public boolean lockJobEngine() {
        String path = jobEngineLockPath();
        return lock(path, 3000);
    }

    @Override
    public void unlockJobEngine() {
        unlock(jobEngineLockPath());
    }

    private String jobEngineLockPath() {
        return "/job_engine/global_job_engine_lock";
    }

}
