package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public abstract class ParquetOrderedFileWriter extends RecordWriter<Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(ParquetOrderedFileWriter.class);

    protected ParquetRawWriter writer = null;

    /**
     * decide whether to write to a new parquet file, the default behavior is not
     * @param key
     * @param value
     * @return
     */
    protected boolean needCutUp(Text key, Text value) {
        if (writer == null) {
            return true;
        }
        return false;
    }

    /**
     * create parquet file writer
     * @return new parquet writer
     */
    abstract protected ParquetRawWriter newWriter() throws IOException;

    /**
     * write data to parquet file
     * @param key
     * @param value
     */
    abstract protected void writeData(Text key, Text value);

    @Override
    public void write(Text key, Text value) throws IOException, InterruptedException {
        if (needCutUp(key, value)) {
            if (writer != null) {
                logger.info("close file");
                writer.close();
            }
            writer = newWriter();
        }

        writeData(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        
        if (writer != null) {
            writer.close();
        }
    }
}
