package io.kyligence.kap.storage.parquet.steps;

import io.kyligence.kap.storage.parquet.format.ParquetExtendColumnPageInputFormat;
import org.apache.hadoop.mapreduce.Job;

public class ParquetExtendColumnPageIndexJob extends ParquetPageIndexJob {
    protected void setMapperClass(Job job) {
        job.setMapperClass(ParquetPageIndexMapper.class);
    }

    @Override
    protected void setInputFormatClass(Job job) {
        job.setInputFormatClass(ParquetExtendColumnPageInputFormat.class);
    }
}
