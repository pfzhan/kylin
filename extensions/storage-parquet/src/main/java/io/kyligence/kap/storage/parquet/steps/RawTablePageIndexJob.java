package io.kyligence.kap.storage.parquet.steps;

import io.kyligence.kap.storage.parquet.format.ParquetRawTablePageInputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static io.kyligence.kap.engine.mr.steps.ParquertMRJobUtils.addParquetInputFile;

public class RawTablePageIndexJob extends ParquetPageIndexJob {
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

    protected void setMapperClass(Job job) {
        job.setMapperClass(RawTablePageIndexMapper.class);
    }

    @Override
    protected void setInputFormatClass(Job job) {
        job.setInputFormatClass(ParquetRawTablePageInputFormat.class);
    }
}
