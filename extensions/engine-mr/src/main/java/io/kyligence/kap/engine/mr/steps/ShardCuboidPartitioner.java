package io.kyligence.kap.engine.mr.steps;

import java.util.Arrays;

import io.kyligence.kap.cube.kv.KapRowConstants;
import org.apache.hadoop.mapreduce.Partitioner;
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

        byte[] partId = Arrays.copyOf(key.array(), KapRowConstants.ROWKEY_FOUR_IDS_LEN);
        return hash(partId, numReduceTasks);
    }
}
