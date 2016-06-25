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

package io.kyligence.kap.rest.sequencesql.topology;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.Pair;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import io.kyligence.kap.rest.sequencesql.DiskResultCache;

public class SequenceTopologyManager {

    private Cache<Pair<Long, Integer>, SequenceTopology> cache;
    private final DiskResultCache diskResultCache;
    private final long expire;

    public SequenceTopologyManager(DiskResultCache diskResultCache, long expire) {

        this.expire = expire;
        this.diskResultCache = diskResultCache;
        cache = CacheBuilder.newBuilder().expireAfterAccess(this.expire, TimeUnit.MILLISECONDS).removalListener(new RemovalListener<Pair<Long, Integer>, Object>() {
            @Override
            public void onRemoval(RemovalNotification<Pair<Long, Integer>, Object> notification) {
                SequenceTopologyManager.this.diskResultCache.cleanEntries(notification.getKey().getFirst() + "_" + notification.getKey().getSecond());
            }
        }).build();
    }

    public SequenceTopology getTopology(long sequenceID, int workerID) {
        return cache.getIfPresent(Pair.newPair(sequenceID, workerID));
    }

    public void addTopology(long sequenceID, int workerID) {
        cache.put(Pair.newPair(sequenceID, workerID), new SequenceTopology(workerID, diskResultCache, sequenceID));
    }

    //test use
    public void cleanup() {
        this.cache.cleanUp();
    }

}
