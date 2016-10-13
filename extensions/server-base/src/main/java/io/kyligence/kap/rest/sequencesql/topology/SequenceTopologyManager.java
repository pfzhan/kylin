/**
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
