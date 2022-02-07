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

import io.kyligence.kap.secondstorage.metadata.TablePartition;
import org.apache.kylin.common.QueryContext;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
}
