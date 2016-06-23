package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.ParquetPageInputFormat;

/**
 * Created by dongli on 5/31/16.
 */
public class ParquetPageIndexJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, cube.getConfig());

            addParquetInputFile(job, new Path(getOptionValue(OPTION_INPUT_PATH)));

            job.setInputFormatClass(ParquetPageInputFormat.class);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setMapperClass(ParquetPageIndexMapper.class);
            job.setNumReduceTasks(0);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());
            return waitForCompletion(job);
        } catch (Exception e) {
            printUsage(options);
            throw e;
        } finally {
            if (job != null) {
                cleanupTempConfFile(job.getConfiguration());
            }
        }
    }

    private void addParquetInputFile(Job job, Path path) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (!fs.exists(path)) {
            logger.warn("Input {} does not exist.", path.toString());
        } else if (fs.isDirectory(path)) {
            for (FileStatus fileStatus : fs.listStatus(path)) {
                addParquetInputFile(job, fileStatus.getPath());
            }
        } else if (fs.isFile(path)) {
            if (isParquetFile(path)) {
                FileInputFormat.addInputPath(job, path);
                logger.debug("Input Path: " + path);
            }
        }
    }

    private boolean isParquetFile(Path path) {
        return path.getName().endsWith("parquet");
    }

}
