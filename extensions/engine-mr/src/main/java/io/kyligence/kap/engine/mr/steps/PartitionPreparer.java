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

package io.kyligence.kap.engine.mr.steps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Comparator;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.buffer.PriorityBuffer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class PartitionPreparer {
    protected static final Logger logger = LoggerFactory.getLogger(PartitionPreparer.class);

    public static Map<Pair<Long, Short>, Integer> preparePartitionMapping(KylinConfig config, CubeSegment cubeSeg,
            int reduceNum, Map<Long, HLLCounter> hllMap) throws IOException {
        logger.info("preparePartitionMapping in KapSpliceInMemCuboidJob");
        Map<Pair<Long, Short>, Integer> partitionMap = Maps.newHashMap();
        PriorityBuffer bucketPriorityQueue = new PriorityBuffer(new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                if (o1 instanceof SizeBucket && o2 instanceof SizeBucket) {
                    SizeBucket bucket1 = (SizeBucket) o1;
                    SizeBucket bucket2 = (SizeBucket) o2;

                    if (bucket1.size > bucket2.size) {
                        return 1;
                    } else if (bucket1.size < bucket2.size) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
                return 0;
            }
        });

        // Bucket initialize
        for (int reduceId = 0; reduceId < reduceNum; reduceId++) {
            bucketPriorityQueue.add(new SizeBucket(reduceId));
        }

        Map<Long, Double> cuboidSizeMapping;
        if (hllMap != null)
            cuboidSizeMapping = CubeStatsReader.getCuboidSizeMapFromRowCount(cubeSeg,
                    CubeStatsReader.getCuboidRowCountMapFromSampling(hllMap, 100));
        else
            cuboidSizeMapping = new CubeStatsReader(cubeSeg, config).getCuboidSizeMap();
        for (long cuboidId : cuboidSizeMapping.keySet()) {
            short shardNum = cubeSeg.getCuboidShardNum(cuboidId);
            long shardAvgSize = (long) (cuboidSizeMapping.get(cuboidId) / shardNum);
            SizeBucket[] bucketHolder = new SizeBucket[Math.min(shardNum, reduceNum)];
            // Prepare bucket
            for (int holderId = 0; holderId < bucketHolder.length; holderId++) {
                bucketHolder[holderId] = (SizeBucket) bucketPriorityQueue.remove();
            }
            // Create mapping
            for (short shardId = 0; shardId < shardNum; shardId++) {
                int holderId = shardId % bucketHolder.length;
                bucketHolder[holderId].size += shardAvgSize;
                partitionMap.put(new Pair(cuboidId, shardId), bucketHolder[holderId].reduceId);
            }
            // Re-add bucket
            for (SizeBucket bucket : bucketHolder) {
                bucketPriorityQueue.add(bucket);
            }
        }

        if (logger.isTraceEnabled()) {
            for (Pair<Long, Short> key : partitionMap.keySet()) {
                logger.trace("Cuboid {} Shard {} --> Reducer {}", key.getFirst(), key.getSecond(),
                        partitionMap.get(key));
            }
        }
        return partitionMap;
    }

    public static Map<Pair<Long, Short>, Integer> preparePartitionMapping(KylinConfig config, CubeSegment cubeSeg,
            int reduceNum) throws IOException {
        return preparePartitionMapping(config, cubeSeg, reduceNum, null);
    }

    public static void preparePartitionMapping(Job job, KylinConfig config, CubeSegment cubeSeg, int reduceNum,
            Map<Long, HLLCounter> hllMap) throws IOException {

        Map<Pair<Long, Short>, Integer> partitionMap = preparePartitionMapping(config, cubeSeg, reduceNum, hllMap);
        // Serialize mapping
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(partitionMap);
        oos.close();
        byte[] serialized = baos.toByteArray();
        job.getConfiguration().set(ByteArrayConfigurationBasedPartitioner.CUBOID_SHARD_REDUCE_MAPPING,
                new String(Base64.encodeBase64(serialized)));
    }

    public static void preparePartitionMapping(Job job, KylinConfig config, CubeSegment cubeSeg, int reduceNum)
            throws IOException {
        preparePartitionMapping(job, config, cubeSeg, reduceNum, null);
    }

    private static class SizeBucket {
        private long size;
        private int reduceId;

        public SizeBucket(int reduceId) {
            this.reduceId = reduceId;
            size = 0;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public int getReduceId() {
            return reduceId;
        }

        public void setReduceId(int reduceId) {
            this.reduceId = reduceId;
        }

        @Override
        public String toString() {
            return "size=" + size + ", reduceId=" + reduceId;
        }
    }
}
