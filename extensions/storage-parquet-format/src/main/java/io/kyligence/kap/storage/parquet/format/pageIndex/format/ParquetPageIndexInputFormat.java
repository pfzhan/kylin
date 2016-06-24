package io.kyligence.kap.storage.parquet.format.pageIndex.format;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * InputFormat to read inverted index of parquet
 *
 * @param <K> Path of parquet data file (*.parquet)
 * @param <V> Instance of ParquetPageIndexTable
 */
public class ParquetPageIndexInputFormat<K, V> extends FileInputFormat<K, V> {
    public org.apache.hadoop.mapreduce.RecordReader<K, V> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetPageIndexRecordReader<>();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
