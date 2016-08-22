package io.kyligence.kap.storage.parquet.steps;

import io.kyligence.kap.storage.parquet.format.ParquetRawTablePageInputFormat;
import org.apache.hadoop.mapreduce.Job;

public class RawTablePageIndexJob extends ParquetPageIndexJob {
    protected void setMapperClass(Job job) {
        job.setMapperClass(ParquetPageIndexMapper.class);
    }

    @Override
    protected void setInputFormatClass(Job job) {
        job.setInputFormatClass(ParquetRawTablePageInputFormat.class);
    }
}
