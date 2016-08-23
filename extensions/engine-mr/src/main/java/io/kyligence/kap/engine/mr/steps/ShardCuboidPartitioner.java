package io.kyligence.kap.engine.mr.steps;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.cube.kv.RowConstants;

public class ShardCuboidPartitioner extends Partitioner<Text, Text> {

    protected int hash(byte[] src, int start, int end, int max) {
        int sum = 0;
        for (int i = start; i < end; ++i) {
            int temp = src[i];
            sum += (temp << 5 - temp);
        }
        return Math.abs(sum) % max;
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        return hash(key.getBytes(), 0, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, numReduceTasks);
    }
}
