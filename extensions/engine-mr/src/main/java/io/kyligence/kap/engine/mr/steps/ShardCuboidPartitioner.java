package io.kyligence.kap.engine.mr.steps;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.cube.kv.RowConstants;

public class ShardCuboidPartitioner extends Partitioner<Text, Text> {

    private int hash(byte[] src, int max) {
        int sum = 0;
        for (byte s : src) {
            sum += (int) s;
        }
        return Math.abs(sum) % max;
    }

    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        // TODO: no copy here
        byte[] partId = Arrays.copyOf(key.getBytes(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN);
        return hash(partId, numReduceTasks);
    }
}
