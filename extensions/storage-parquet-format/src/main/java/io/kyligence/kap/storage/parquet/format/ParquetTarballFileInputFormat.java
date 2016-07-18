package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * spark rdd input 
 */
public class ParquetTarballFileInputFormat extends FileInputFormat<Text, Text> {

    public org.apache.hadoop.mapreduce.RecordReader<Text, Text> createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new ParquetTarballFileReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
