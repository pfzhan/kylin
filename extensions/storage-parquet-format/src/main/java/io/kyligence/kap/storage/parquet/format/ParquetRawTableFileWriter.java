package io.kyligence.kap.storage.parquet.format;

import java.io.IOException;
import java.security.InvalidParameterException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.file.ParquetRawWriter;

public class ParquetRawTableFileWriter extends ParquetOrderedFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(ParquetRawTableFileWriter.class);

    public ParquetRawTableFileWriter(TaskAttemptContext context, Class<?> keyClass, Class<?> valueClass) {
        if (keyClass == Text.class && valueClass == Text.class) {
            logger.info("KV class is Text");
        } else {
            throw new InvalidParameterException("ParquetRecordWriter only support Text type now");
        }
    }

    @Override
    protected boolean needCutUp(Text key, Text value) {
        return super.needCutUp(key, value);
    }

    @Override
    protected ParquetRawWriter newWriter() throws IOException {
        return null;
    }

    @Override
    protected void writeData(Text key, Text value) {

    }
}
