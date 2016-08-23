package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.storage.parquet.format.ParquetFileInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableOutputFormat;

public class KapRawTableJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(KapRawTableJob.class);
    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

    public KapRawTableJob() {
        this.setMapperClass(HiveToRawTableMapper.class);
    }

    @SuppressWarnings("rawtypes")
    private Class<? extends Mapper> mapperClass;

    private boolean skipped = false;

    @Override
    public boolean isSkipped() {
        return skipped;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (this.mapperClass == null)
            throw new Exception("Mapper class is not set!");

        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_ID);
            options.addOption(OPTION_INPUT_PATH);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_NCUBOID_LEVEL);
            options.addOption(OPTION_INPUT_FORMAT);
            options.addOption(OPTION_CUBING_JOB_ID);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            int nCuboidLevel = Integer.parseInt(getOptionValue(OPTION_NCUBOID_LEVEL));
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);
            String cubingJobId = getOptionValue(OPTION_CUBING_JOB_ID);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegmentById(segmentID);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job, cube.getConfig());

            // Mapper
            int numFiles = configureMapperInputFormat(config, nCuboidLevel, cube, cube.getSegmentById(segmentID), cube.getDescriptor());
            if (numFiles == 0) {
                skipped = true;
                logger.info("{} is skipped because there's no input file", getOptionValue(OPTION_JOB_NAME));
                return 0;
            }

            job.setMapperClass(this.mapperClass);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(KylinReducer.class); // for base cuboid shuffle skew, some rowkey aggregates far more records than others

            // Reducer
            job.setReducerClass(KylinReducer.class);
            job.setOutputFormatClass(ParquetRawTableOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // Partitioner
            job.setPartitionerClass(ShardCuboidPartitioner.class);

            FileOutputFormat.setOutputPath(job, output);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
            job.getConfiguration().setInt(BatchConstants.CFG_CUBE_CUBOID_LEVEL, nCuboidLevel);

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(config, cube, cubeSeg));

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            setReduceTaskNum(job, cube.getDescriptor(), nCuboidLevel);

            this.deletePath(job.getConfiguration(), output);

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

    public static String getWorkingDir(KylinConfig config, CubeInstance cube, CubeSegment cubeSegment) {
        return new StringBuffer(KapConfig.wrap(config).getParquentStoragePath()).append(cube.getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }

    private int setInputFiles(KylinConfig config, int level, CubeInstance cube, CubeSegment cubeSegment, CubeDesc desc) throws IOException {
        // base cuboid should not enter this method
        Preconditions.checkState(level > 0);

        Set<Long> parentSet = new HashSet<Long>();
        Set<Long> childSet = null;
        parentSet.add(Cuboid.getBaseCuboidId(desc));
        CuboidScheduler scheduler = new CuboidScheduler(desc);
        for (int i = 0; i < (level - 1); ++i) {
            childSet = new HashSet<Long>();
            for (long parent : parentSet) {
                childSet.addAll(scheduler.getSpanningCuboid(parent));
            }
            parentSet = childSet;
        }

        int numFiles = 0;
        for (long parent : parentSet) {
            Path path = new Path(getWorkingDir(config, cube, cubeSegment) + parent);
            //FileInputFormat.setInputPathFilter(job, ParquetFilter.class);
            numFiles += ParquertMRJobUtils.addParquetInputFile(job, path);
        }
        return numFiles;

    }

    private int configureMapperInputFormat(KylinConfig config, int level, CubeInstance cube, CubeSegment cubeSeg, CubeDesc desc) throws IOException {
        String input = getOptionValue(OPTION_INPUT_PATH);

        if ("FLAT_TABLE".equals(input)) {
            // base cuboid case
            IMRInput.IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);
            return 1; //return a non-zero value
        } else {
            // n-dimension cuboid case
            if (hasOption(OPTION_INPUT_FORMAT) && ("textinputformat".equalsIgnoreCase(getOptionValue(OPTION_INPUT_FORMAT)))) {
                //                FileInputFormat.setInputPaths(job, new Path(input));
                //                job.setInputFormatClass(TextInputFormat.class);
                throw new IllegalStateException();
            } else {
                // default intput is parquet file
                int numFiles = setInputFiles(config, level, cube, cubeSeg, desc);
                job.setInputFormatClass(ParquetFileInputFormat.class);
                return numFiles;
            }
        }
    }

    protected void setReduceTaskNum(Job job, CubeDesc cubeDesc, int level) throws ClassNotFoundException, IOException, InterruptedException, JobException {
        Configuration jobConf = job.getConfiguration();

        KylinConfig kylinConfig = cubeDesc.getConfig();

        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        double reduceCountRatio = kylinConfig.getDefaultHadoopJobReducerCountRatio();

        // total map input MB
        double totalMapInputMB = this.getTotalMapInputMB();

        // output / input ratio
        int preLevelCuboids, thisLevelCuboids;
        if (level == 0) { // base cuboid
            preLevelCuboids = thisLevelCuboids = 1;
        } else { // n-cuboid
            int[] allLevelCount = CuboidCLI.calculateAllLevelCount(cubeDesc);
            preLevelCuboids = allLevelCount[level - 1];
            thisLevelCuboids = allLevelCount[level];
        }

        // total reduce input MB
        double totalReduceInputMB = totalMapInputMB * thisLevelCuboids / preLevelCuboids;

        // number of reduce tasks
        int numReduceTasks = (int) Math.round(totalReduceInputMB / perReduceInputMB * reduceCountRatio);

        // adjust reducer number for cube which has DISTINCT_COUNT measures for better performance
        if (cubeDesc.hasMemoryHungryMeasures()) {
            numReduceTasks = numReduceTasks * 4;
        }

        // at least 1 reducer by default
        numReduceTasks = Math.max(kylinConfig.getHadoopJobMinReducerNumber(), numReduceTasks);
        // no more than 500 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        jobConf.setInt(MAPRED_REDUCE_TASKS, numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalMapInputMB));
        logger.info("Having level " + level + ", pre-level cuboids " + preLevelCuboids + ", this level cuboids " + thisLevelCuboids);
        logger.info("Having per reduce MB " + perReduceInputMB + ", reduce count ratio " + reduceCountRatio);
        logger.info("Setting " + MAPRED_REDUCE_TASKS + "=" + numReduceTasks);
    }

    /**
     * @param mapperClass
     *            the mapperClass to set
     */
    @SuppressWarnings("rawtypes")
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }
}
