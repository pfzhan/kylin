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

package io.kyligence.kap.engine.spark;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.ReducerNumSizing;
import org.apache.kylin.engine.spark.SparkCubingByLayer;
import org.apache.kylin.job.exception.JobException;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.steps.PartitionPreparer;

public class KapSparkCubingByLayer extends SparkCubingByLayer {
    protected static final Logger logger = LoggerFactory.getLogger(KapSparkCubingByLayer.class);

    public KapSparkCubingByLayer() {
        super();
    }

    @Override
    protected JavaPairRDD<ByteArray, Object[]> prepareOutput(JavaPairRDD<ByteArray, Object[]> rdd, KylinConfig config,
            CubeSegment segment, int level) {
        long levelRddSize = SizeEstimator.estimate(rdd) / (1024 * 1024);
        int repartition;
        Map<Pair<Long, Short>, Integer> partitionMap;
        try {
            repartition = repartitionForOutput(segment, levelRddSize, level);
            partitionMap = PartitionPreparer.preparePartitionMapping(config, segment, repartition);
        } catch (ClassNotFoundException | JobException | InterruptedException | IOException e) {
            throw new RuntimeException("Failed to get partiton during Spark cubing: " + e);
        }
        return rdd.repartitionAndSortWithinPartitions(new SparkCubingPartitioner(repartition, partitionMap));
    }

    protected int repartitionForOutput(CubeSegment segment, long rddSize, int level)
            throws ClassNotFoundException, JobException, InterruptedException, IOException {
        int partition = ReducerNumSizing.getLayeredCubingReduceTaskNum(segment, (double) rddSize, level);
        List<List<Long>> layeredCuboids = segment.getCuboidScheduler().getCuboidsByLayer();
        for (Long cuboidId : layeredCuboids.get(level)) {
            partition = Math.max(partition, segment.getCuboidShardNum(cuboidId));
        }
        logger.info("Repartition for Kystorage, level: {}, rdd size: {}, partition: {}", level, rddSize, partition);
        return partition;
    }

    @Override
    protected void setHadoopConf(Job job, CubeSegment segment, String metaUrl) throws Exception {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, segment.getCubeInstance().getName());
        job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segment.getUuid());
        job.getConfiguration().set(BatchConstants.CFG_MR_SPARK_JOB, "spark");
        job.getConfiguration().set(BatchConstants.CFG_SPARK_META_URL, metaUrl);
    }

    static class SparkCubingPartitioner extends Partitioner {
        private Map<Pair<Long, Short>, Integer> partitionMap;
        private int partitions;

        public SparkCubingPartitioner(int partitions, Map<Pair<Long, Short>, Integer> partitionMap) {
            this.partitions = partitions;
            this.partitionMap = partitionMap;
        }

        public int getByteArrayPartition(byte[] key) {
            short shardId = (short) BytesUtil.readShort(key, 0, RowConstants.ROWKEY_SHARDID_LEN);
            long cuboidId = BytesUtil.readLong(key, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
            if (partitionMap == null) {
                return mod(key, 0, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, partitions);
            }
            int partition = partitionMap.get(new Pair<>(cuboidId, shardId));
            return partition;
        }

        protected int mod(byte[] src, int start, int end, int total) {
            int sum = Bytes.hashBytes(src, start, end - start);
            int mod = sum % total;
            if (mod < 0)
                mod += total;

            return mod;
        }

        @Override
        public int numPartitions() {
            return this.partitions;
        }

        @Override
        public int getPartition(Object o) {
            return getByteArrayPartition(((ByteArray) o).array());
        }
    }
}
