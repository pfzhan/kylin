package io.kyligence.kap.engine.mr.steps;

import java.util.Arrays;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.ByteArrayWritable;

/**
 * Created by roger on 5/27/16.
 */
public class ByteArrayShardCuboidPartitioner extends Partitioner<ByteArrayWritable, ByteArrayWritable> {

    private int hash(byte[] src, int max) {
        int sum = 0;
        for (byte s : src) {
            sum += (int) s;
        }
        return Math.abs(sum) % max;
    }

    @Override
    public int getPartition(ByteArrayWritable key, ByteArrayWritable value, int numReduceTasks) {
        // TODO: no copy here
        byte[] partId = Arrays.copyOf(key.array(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN);
        return hash(partId, numReduceTasks);
    }
}
