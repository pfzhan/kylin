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

package io.kyligence.kap.secondstorage;

import com.google.common.collect.Sets;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SecondStorageQueryRouteUtil {
    private static final SecureRandom replicaRandom = new SecureRandom();
    private static final Map<String, Boolean> NODE_STATUS = new ConcurrentHashMap<>();

    private SecondStorageQueryRouteUtil() {
    }

    public static void setNodeStatus(String node, boolean status) {
        NODE_STATUS.put(node, status);
    }

    public static boolean getNodeStatus(String node) {
        return NODE_STATUS.getOrDefault(node, true);
    }

    public static TablePartition getNextPartition(List<TablePartition> partitions) {
        if (!QueryContext.current().getUsedPartitionIndexes().isEmpty() && !QueryContext.current().isLastFailed())
            return getCurrentPartition(partitions);
        int[] candidates = IntStream.range(0, partitions.size())
                .filter(i -> !QueryContext.current().getUsedPartitionIndexes().contains(i))
                .filter(i -> partitions.get(i).getShardNodes().stream().allMatch(SecondStorageQueryRouteUtil::getNodeStatus))
                .toArray();
        if (partitions.size() == 1) {
            QueryContext.current().setRetrySecondStorage(false);
            QueryContext.current().getUsedPartitionIndexes().add(candidates[0]);
            return partitions.get(candidates[0]);
        } else if (candidates.length == 0){
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("All cluster failed, no candidate found.");
        } else {
            int nextPartition = candidates[replicaRandom.nextInt(candidates.length)];
            QueryContext.current().getUsedPartitionIndexes().add(nextPartition);
            return partitions.get(nextPartition);
        }
    }

    public static TablePartition getCurrentPartition(List<TablePartition> partitions) {
        List<Integer> used = QueryContext.current().getUsedPartitionIndexes();
        return partitions.get(used.get(used.size() - 1));
    }

    public static List<String> getCurrentAliveShardReplica() {
        List<List<String>> used = QueryContext.current().getUsedSecondStorageNodes();
        return used.get(used.size() - 1);
    }

    public static List<String> getAliveShardReplica(List<TablePartition> partitions, String project, Set<String> allSegIds) {
        // collect all node which partition used
        Set<String> allSegmentUsedNode = Sets.newHashSet();
        for (TablePartition partition : partitions) {
            if (allSegIds.contains(partition.getSegmentId())) {
                allSegmentUsedNode.addAll(partition.getShardNodes());
            }
        }

        if (allSegmentUsedNode.isEmpty()) {
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("Segment node is empty.");
        }
        List<NodeGroup> nodeGroups = SecondStorage.nodeGroupManager(KylinConfig.getInstanceFromEnv(), project).listAll();

        if (nodeGroups.isEmpty()) {
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("Node groups is empty.");
        }

        List<Set<String>> shards = groupsToShards(nodeGroups);
        Set<String> usedNodes = getUsedNodes();
        List<Set<String>> segmentUsedShard = getSegmentUsedShard(shards, allSegmentUsedNode);
        List<String> availableShardReplica = getAvailableShardReplica(usedNodes, segmentUsedShard);

        if (availableShardReplica.size() != segmentUsedShard.size()) {
            QueryContext.current().setRetrySecondStorage(false);
            throw new IllegalStateException("One shard all replica has down");
        }

        QueryContext.current().getUsedSecondStorageNodes().add(availableShardReplica);
        return availableShardReplica;
    }

    /**
     * groups to shard
     * group [replica][shardSize] to shard[shardSize][replica]
     *
     * @param groups group
     * @return shards
     */
    private static List<Set<String>> groupsToShards(List<NodeGroup> groups) {
        int shardSize = groups.get(0).getNodeNames().size();
        // key is shard num, value is replica name
        Map<Integer, Set<String>> shards = new HashMap<>(shardSize);

        // if shard has different replica， will became a bug
        for (int shardNum = 0; shardNum < shardSize; shardNum++) {
            for (NodeGroup group : groups) {
                shards.computeIfAbsent(shardNum, key -> new HashSet<>()).add(group.getNodeNames().get(shardNum));
            }
        }

        return new ArrayList<>(shards.values());
    }

    /**
     * Get used node in last try
     *
     * @return used nodes
     */
    private static Set<String> getUsedNodes() {
        return QueryContext.current().getUsedSecondStorageNodes()
                .stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    /**
     * Get segment used nodes
     *
     * @param shards             shards
     * @param allSegmentUsedNode all segment used node
     * @return segment used nodes
     */
    private static List<Set<String>> getSegmentUsedShard(List<Set<String>> shards, Set<String> allSegmentUsedNode) {
        // filter which shards used by partitions
        return shards.stream().filter(replicas -> {
            for (String nodeName : allSegmentUsedNode) {
                if (replicas.contains(nodeName)) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    /**
     * Get available shard replica to group
     *
     * @param usedNodes        last retry used nodes
     * @param segmentUsedShard segments used shard
     * @return group
     */
    private static List<String> getAvailableShardReplica(Set<String> usedNodes, List<Set<String>> segmentUsedShard) {
        List<String> availableShardReplica = new ArrayList<>(segmentUsedShard.size());

        for (Set<String> replicas : segmentUsedShard) {
            List<String> available = replicas.stream().filter(SecondStorageQueryRouteUtil::getNodeStatus).filter(nodeName -> !usedNodes.contains(nodeName)).collect(Collectors.toList());

            if (available.isEmpty()) {
                QueryContext.current().setRetrySecondStorage(false);
                throw new IllegalStateException("One shard all replica has down");
            }

            if (available.size() == 1) {
                QueryContext.current().setRetrySecondStorage(false);
            }

            availableShardReplica.add(available.get(0));
        }

        return availableShardReplica;
    }

}
