package io.kyligence.kap.storage.parquet.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by wangcheng on 8/25/16.
 */
public class ParquetRawTableFileInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return null;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
