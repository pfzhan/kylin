package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * cube build output format
 */
public class ParquetFileOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetFileWriter(job, job.getOutputKeyClass(), job.getOutputValueClass());
    }
}
