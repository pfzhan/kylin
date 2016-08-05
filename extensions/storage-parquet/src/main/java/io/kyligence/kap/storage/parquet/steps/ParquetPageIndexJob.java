package io.kyligence.kap.storage.parquet.steps;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.steps.ParquertMRJobUtils;
import io.kyligence.kap.storage.parquet.format.ParquetPageInputFormat;

public class ParquetPageIndexJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(ParquetPageIndexJob.class);

    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, cube.getConfig());

            int inputNum = ParquertMRJobUtils.addParquetInputFile(job, new Path(getOptionValue(OPTION_INPUT_PATH)));
            if (inputNum == 0) {
                skipped = true;
                logger.info("ParquetPageIndexJob is skipped because there's no input file");
                return 0;
            }

            setInputFormatClass(job);
            job.setOutputFormatClass(NullOutputFormat.class);
            setMapperClass(job);
            job.setNumReduceTasks(0);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

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

    protected void setMapperClass(Job job) {
        job.setMapperClass(ParquetPageIndexMapper.class);
    }

    protected void setInputFormatClass(Job job) {
        job.setInputFormatClass(ParquetPageInputFormat.class);
    }
}
