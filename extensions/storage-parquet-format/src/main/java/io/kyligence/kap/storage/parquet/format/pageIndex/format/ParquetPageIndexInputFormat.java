package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by dong on 6/21/16.
 */
public class ParquetPageIndexInputFormat<K, V> extends FileInputFormat<K, V> {
    public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetPageIndexRecordReader<>();
    }
}
