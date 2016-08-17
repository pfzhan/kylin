package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.engine.mr.steps.InMemCuboidMapper;
import org.apache.kylin.engine.mr.steps.InMemCuboidReducer;
import org.apache.kylin.job.manager.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.ParquetFileOutputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;

public class KapInMemCuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(KapInMemCuboidJob.class);

    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    private boolean checkSkip(String cubingJobId) {
        if (cubingJobId == null)
            return false;

        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(cubingJobId);
        skipped = cubingJob.isInMemCubing() == false;
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        int reduceNum = 1;

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String output = getOptionValue(OPTION_OUTPUT_PATH);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            //config = cube.getConfig();
            CubeSegment cubeSeg = cube.getSegmentById(segmentID);
            String cubingJobId = getOptionValue(OPTION_CUBING_JOB_ID);

            if (checkSkip(cubingJobId)) {
                logger.info("Skip job " + getOptionValue(OPTION_JOB_NAME) + " for " + cubeSeg);
                return 0;
            }

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(config, cube, cubeSeg));
            logger.info("Kylin Working directory: " + getWorkingDir(config, cube, cubeSeg));

            // set input
            IMRInput.IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);

            // set mapper
            job.setMapperClass(InMemCuboidMapper.class);
            job.setMapOutputKeyClass(ByteArrayWritable.class);
            job.setMapOutputValueClass(ByteArrayWritable.class);

            // set partitioner
            job.setPartitionerClass(ByteArrayShardCuboidPartitioner.class);

            // set output
            job.setReducerClass(InMemCuboidReducer.class);
            reduceNum = calculateReducerNum(cubeSeg);
            job.setNumReduceTasks(reduceNum);

            // the cuboid file and KV class must be compatible with 0.7 version for smooth upgrade
            job.setOutputFormatClass(ParquetFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(job, outputPath);

            HadoopUtil.deletePath(job.getConfiguration(), outputPath);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private String getWorkingDir(KylinConfig config, CubeInstance cube, CubeSegment cubeSegment) {
        logger.info("get Hdfs Working Directory: " + config.getHdfsWorkingDirectory());
        return new StringBuffer(config.getHdfsWorkingDirectory()).append("parquet/").append(cube.getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }

    private int calculateReducerNum(CubeSegment cubeSeg) throws IOException {
        KylinConfig kylinConfig = cubeSeg.getConfig();

        Map<Long, Double> cubeSizeMap = new CubeStatsReader(cubeSeg, kylinConfig).getCuboidSizeMap();
        double totalSizeInM = 0;
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }

        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(totalSizeInM / perReduceInputMB);

        // at least 1 reducer
        numReduceTasks = Math.max(1, numReduceTasks);
        // no more than 5000 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalSizeInM));
        logger.info("Having per reduce MB " + perReduceInputMB);
        logger.info("Setting " + "mapred.reduce.tasks" + "=" + numReduceTasks);
        return numReduceTasks;
    }

    public static void main(String[] args) throws Exception {
        InMemCuboidJob job = new InMemCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
