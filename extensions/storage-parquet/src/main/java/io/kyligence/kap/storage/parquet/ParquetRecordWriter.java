package io.kyligence.kap.storage.parquet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.security.InvalidParameterException;

/**
 * Created by roger on 5/30/16.
 */
public class ParquetRecordWriter <K,V> extends RecordWriter<K, V>{
    private Class<?> keyClass;
    private Class<?> valueClass;

    private String curCubeId = null;
    private String curSegmentId = null;
    private String curCuboidId = null;
    private String curShardId = null;

    public ParquetRecordWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;

        if (keyClass != Text.class || valueClass != Text.class) {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    // Only support Text type
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }
}
