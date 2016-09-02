package io.kyligence.kap.storage.parquet.steps;

import org.apache.hadoop.mapreduce.Job;

public class RawTableFuzzyIndexJob extends RawTablePageIndexJob {
    @Override
    protected void setMapperClass(Job job) {
        job.setMapperClass(RawTableFuzzyIndexMapper.class);
    }
}
