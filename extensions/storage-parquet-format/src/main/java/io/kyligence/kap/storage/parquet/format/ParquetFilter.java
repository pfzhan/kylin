package io.kyligence.kap.storage.parquet.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class ParquetFilter extends Configured implements PathFilter {

    Configuration conf;

    @Override
    public boolean accept(Path path) {
        return path.getName().endsWith("parquet");
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
