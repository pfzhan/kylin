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

package io.kyligence.kap.storage.hbase.v2;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseEndpointRPC;

public class KAPCubeHBaseEndpointRPC extends CubeHBaseEndpointRPC {

    public KAPCubeHBaseEndpointRPC(ISegment segment, Cuboid cuboid, GTInfo fullGTInfo) {
        super(segment, cuboid, fullGTInfo);
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
            return Pair.newPair((short) (avgShard + 1), ShardingHash.normalize(baseShard, (short) ((avgShard + 1) * workerID), totalShards));
        } else {
            return Pair.newPair(avgShard, ShardingHash.normalize(baseShard, (short) (remain + avgShard * workerID), totalShards));
        }

    }

}
