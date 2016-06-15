package io.kyligence.kap.storage.parquet.format;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class ParquetPageIndexInputFormat <K, V> extends FileInputFormat<K, V> {
    public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetPageIndexRecordReader<>();
    }
}
