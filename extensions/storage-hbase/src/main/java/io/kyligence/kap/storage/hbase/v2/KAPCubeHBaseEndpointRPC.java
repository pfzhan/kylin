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

package io.kyligence.kap.storage.hbase.v2;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC;

public class KAPCubeHBaseEndpointRPC extends CubeHBaseEndpointRPC {

    public KAPCubeHBaseEndpointRPC(ISegment segment, Cuboid cuboid, GTInfo fullGTInfo, StorageContext context) {
        super(segment, cuboid, fullGTInfo, context);
    }

    protected Pair<Short, Short> getShardNumAndBaseShard() {
        short shardNum = cubeSeg.getCuboidShardNum(cuboid.getId());
        short baseShard = cubeSeg.getCuboidBaseShard(cuboid.getId());
        int totalShards = cubeSeg.getTotalShards(cuboid.getId());

        Pair<Short, Short> parallelAssignment = BackdoorToggles.getShardAssignment();
        if (parallelAssignment == null)
            return Pair.newPair(shardNum, baseShard);

        short workerCount = parallelAssignment.getFirst();
        short workerID = parallelAssignment.getSecond();
        short avgShard = (short) (shardNum / workerCount);
        short remain = (short) (shardNum % workerCount);

        if (workerID < remain) {
            return Pair.newPair((short) (avgShard + 1),
                    ShardingHash.normalize(baseShard, (short) ((avgShard + 1) * workerID), totalShards));
        } else {
            return Pair.newPair(avgShard,
                    ShardingHash.normalize(baseShard, (short) (remain + avgShard * workerID), totalShards));
        }

    }

}
