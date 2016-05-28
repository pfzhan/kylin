package io.kyligence.kap.storage.parquet.steps;

import java.util.Arrays;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.ByteArrayWritable;

/**
 * Created by roger on 5/27/16.
 */
public class ShardCuboidPartitioner extends Partitioner<ByteArrayWritable, ByteArrayWritable> {

    private int hash(byte[] src, int max) {
        int sum = 0;
        for (byte s: src) {
            sum += (int) s;
        }
        return sum % max;
    }

    @Override
    public int getPartition(ByteArrayWritable key, ByteArrayWritable value, int numReduceTasks) {

        byte[] partId = Arrays.copyOf(key.array(), RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN);
        return hash(partId, numReduceTasks);
    }
}
