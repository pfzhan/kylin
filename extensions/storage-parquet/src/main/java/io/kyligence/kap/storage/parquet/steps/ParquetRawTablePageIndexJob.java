package io.kyligence.kap.storage.parquet.steps;

import static io.kyligence.kap.engine.mr.steps.ParquertMRJobUtils.addParquetInputFile;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.ParquetPageInputFormat;

public class ParquetRawTablePageIndexJob extends ParquetPageIndexJob {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetRawTablePageIndexJob.class);

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    protected int setJobInputFile(Job job, Path path) throws IOException {
        int ret = 0;
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (!fs.exists(path)) {
            logger.warn("Input {} does not exist.", path.toString());
        } else if (fs.isDirectory(path)) {
            for (FileStatus fileStatus : fs.listStatus(path)) {
                // build index for only rawtable file
                if (!fileStatus.getPath().getName().matches("^\\d+$")) {
                    ret += addParquetInputFile(job, fileStatus.getPath());
                }
            }
        } else {
            logger.warn("Input Path: {} should be directory", path);
        }
        return ret;
    }

    @Override
    protected void setMapperClass(Job job) {
        job.setMapperClass(ParquetPageIndexMapper.class);
    }

    @Override
    protected void setInputFormatClass(Job job) {
        job.setInputFormatClass(ParquetPageInputFormat.class);
    }
}
