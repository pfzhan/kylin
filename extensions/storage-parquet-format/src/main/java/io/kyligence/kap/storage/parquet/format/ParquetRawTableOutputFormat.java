package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParquetRawTableOutputFormat extends FileOutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new ParquetRawTableFileWriter(this.getDefaultWorkFile(job, "raw").getParent(), job, job.getOutputKeyClass(), job.getOutputValueClass());
    }
}
