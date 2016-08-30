package io.kyligence.kap.engine.mr.steps;

import org.apache.hadoop.io.Text;

import io.kyligence.kap.cube.raw.kv.RawTableConstants;

public class ShardPartitioner extends ShardCuboidPartitioner {
    public int getPartition(Text key, Text value, int numReduceTasks) {
        return hash(key.getBytes(), 0, RawTableConstants.SHARDID_LEN, numReduceTasks);
    }
}
