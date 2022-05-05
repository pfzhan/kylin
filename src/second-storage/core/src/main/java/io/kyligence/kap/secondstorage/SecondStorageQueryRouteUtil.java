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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SecondStorageQueryRouteUtil {
    private static final Map<String, Boolean> NODE_STATUS = new ConcurrentHashMap<>();

    private SecondStorageQueryRouteUtil() {
    }

    public static void setNodeStatus(String node, boolean status) {
        NODE_STATUS.put(node, status);
    }

    public static boolean getNodeStatus(String node) {
        return NODE_STATUS.getOrDefault(node, true);
    }

    public static List<Set<String>> getUsedShard(List<TablePartition> partitions, String project, Set<String> allSegIds) {
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

        List<Set<String>> shards = SecondStorageNodeHelper.groupsToShards(nodeGroups);
        List<Set<String>> segmentUsedShard = getSegmentUsedShard(shards, allSegmentUsedNode);
        filterAvailableReplica(segmentUsedShard);

        return segmentUsedShard;
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
     * filter available replica
     *
     * @param segmentUsedShard segments used shard
     */
    private static void filterAvailableReplica(List<Set<String>> segmentUsedShard) {
        for (Set<String> replicas : segmentUsedShard) {
            replicas.removeIf(replica -> !SecondStorageQueryRouteUtil.getNodeStatus(replica));

            if (replicas.isEmpty()) {
                QueryContext.current().setRetrySecondStorage(false);
                throw new IllegalStateException("One shard all replica has down");
            }
        }
    }

}
