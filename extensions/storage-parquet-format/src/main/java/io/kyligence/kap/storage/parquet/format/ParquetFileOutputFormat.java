package io.kyligence.kap.storage.parquet.format;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ParquetFileOutputFormat <K,V> extends FileOutputFormat<K, V> {
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetRecordWriter(job, job.getOutputKeyClass(), job.getOutputValueClass());
    }
}
