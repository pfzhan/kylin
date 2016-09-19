package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public abstract class ParquetOrderedFileWriter extends RecordWriter<Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOrderedFileWriter.class);

    protected ParquetRawWriter writer = null;
    protected Path tmpDir = null;
    protected Path tmpPath = null;

    /**
     * create parquet file writer
     * @return new parquet writer
     */
    abstract protected ParquetRawWriter newWriter() throws IOException, InterruptedException;

    protected void cleanWriter() throws IOException {
        if (writer != null) {
            Path dest = getDestPath();
            writer.close();
            writer = null;
            FileSystem fs = HadoopUtil.getFileSystem(tmpPath.toString());
            logger.info("move {} to {}", tmpPath, dest);
            fs.mkdirs(dest.getParent());
            fs.rename(tmpPath, dest);
        }
    }

    /**
     * write data to parquet file
     * @param key
     * @param value
     */
    abstract protected void writeData(Text key, Text value);

    /**
     * Fresh parquet file writer.
     * If there's no writer, create one.
     * Otherwise close the
     * @return new parquet writer
     */
    abstract protected void freshWriter(Text key, Text value) throws IOException, InterruptedException;

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        freshWriter(key, value);
        writeData(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        cleanWriter();
    }

    abstract protected Path getDestPath();
}
