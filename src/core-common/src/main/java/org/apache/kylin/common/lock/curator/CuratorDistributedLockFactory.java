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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.DistributedLockException;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.lock.curator.CuratorDistributedLock.LockEntry;
import org.apache.kylin.common.util.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionState;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.state.ConnectionStateListener;

@SuppressWarnings({ "WeakerAccess" })
public class CuratorDistributedLockFactory extends DistributedLockFactory {
    private static final Logger logger = LoggerFactory.getLogger(CuratorDistributedLockFactory.class);
    private static final ConnectionStateListener listener = new CuratorDistributedLockListener();
    private final CuratorFramework client;

    private static CuratorFramework getZKClient(KylinConfig config) {
        try {
            return ZKUtil.getZookeeperClient(config, listener);
        } catch (Exception e) {
            throw new DistributedLockException("Failed to get curator client", e);
        }
    }

    public CuratorDistributedLockFactory() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public CuratorDistributedLockFactory(KylinConfig config) {
        client = getZKClient(config);
    }

    @Override
    public Lock getLockForClient(String client, String key) {
        return new CuratorDistributedLock(this.client, key);
    }

    @Override
    public void initialize() {
        // Do nothing.
    }

    @Override
    public CuratorDistributedLock getLockForCurrentThread(String path) {
        return new CuratorDistributedLock(client, path);
    }

    static class CuratorDistributedLockListener implements ConnectionStateListener {
        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            long sessionId = -1;
            try {
                sessionId = curatorFramework.getZookeeperClient().getZooKeeper().getSessionId();
            } catch (Exception e) {
                logger.error("Failed to get zk Session Id of {}", curatorFramework, e);
            }
            switch (connectionState) {
            case LOST:
            case SUSPENDED:
                logger.error("zk connection {}, zk Session Id: {}", connectionState, sessionId);

                ConcurrentMap<LockEntry, Boolean> locks = CuratorDistributedLock.lockedThreads.get(curatorFramework);
                if (locks != null && !locks.isEmpty()) {
                    for (Map.Entry<LockEntry, Boolean> entry : locks.entrySet()) {
                        LockEntry lockEntry = entry.getKey();
                        if (entry.getValue()) {
                            lockEntry.getThread().interrupt();
                            logger.error(
                                    "Thread interrupt: {}, zk lock {} for path: {}, lock acquired: {}, zk Session Id: {}",
                                    lockEntry.thread.getId(), connectionState, lockEntry.path, entry.getValue(),
                                    sessionId);
                        }
                    }

                    CuratorDistributedLock.lockedThreads.get(curatorFramework).clear();
                }
                break;
            default:
                logger.info("zk connection state changed to: {}, zk Session Id: {}", connectionState, sessionId);
            }
        }
    }

}
