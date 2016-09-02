package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.storage.parquet.format.ParquetFormatConstants;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableMergeInputFormat;
import io.kyligence.kap.storage.parquet.format.ParquetRawTableOutputFormat;

public class KapMergeRawTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(KapMergeRawTableJob.class);

    private boolean skipped = false;

    private static final String MAPRED_REDUCE_TASKS = "mapred.reduce.tasks";

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
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentID = getOptionValue(OPTION_SEGMENT_ID);

            CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegmentById(segmentID);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            logger.info("Starting: " + jobName);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, cube.getConfig());

            // set inputs
            int folderNum = addRawInputDirs(getOptionValue(OPTION_INPUT_PATH), job);
            if (folderNum == 0) {
                skipped = true;
                logger.info("{} is skipped because there's no input folder", getOptionValue(OPTION_JOB_NAME));
                return 0;
            }

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            FileOutputFormat.setOutputPath(job, output);

            // Mapper
            job.setInputFormatClass(ParquetRawTableMergeInputFormat.class);
            job.setMapperClass(KapMergeRawTableMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setCombinerClass(KylinReducer.class);
            job.setPartitionerClass(ShardPartitioner.class);

            job.setReducerClass(KylinReducer.class);
            job.setOutputFormatClass(ParquetRawTableOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(cube.getDescriptor().getConfig(), cube, cubeSeg));

            //push down kylin config
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, KylinConfig.getInstanceFromEnv().getConfigAsString());

            setReduceTaskNum(job, cube.getDescriptor(), 0);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in MergeCuboidJob", e);
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

    public int addRawInputDirs(String input, Job job) throws IOException {
        int folderNum = addRawInputDirs(StringSplitter.split(input, ","), job);
        logger.info("Number of added folders:" + folderNum);
        return folderNum;
    }

    public int addRawInputDirs(String[] inputs, Job job) throws IOException {
        int ret = 0;//return number of added folders
        for (String inp : inputs) {
            inp = inp.trim();
            if (inp.endsWith("/*")) {
                inp = inp.substring(0, inp.length() - 2);
                FileSystem fs = FileSystem.get(job.getConfiguration());
                Path path = new Path(inp);

                if (!fs.exists(path)) {
                    logger.warn("Path not exist:" + path.toString());
                    continue;
                }

                FileStatus[] fileStatuses = fs.listStatus(path);
                boolean hasDir = false;
                for (FileStatus stat : fileStatuses) {
                    String name = stat.getPath().getName();
                    //if (stat.isDirectory() && !stat.getPath().getName().startsWith("_") && stat.getPath().getName().equals("RawTable")) {
                    //hasDir = true;
                    if (stat.getPath().getName().endsWith(".parquet"))
                        ret += addRawInputDirs(new String[] { stat.getPath().toString() }, job);
                    //}
                }
            } else {
                logger.info("Add input " + inp);
                FileInputFormat.addInputPath(job, new Path(inp));
                ret++;
            }
        }
        return ret;
    }

    public static class RawTablePathFilter implements PathFilter {
        public RawTablePathFilter() {
        }

        @Override
        public boolean accept(Path path) {
            String name = path.getName();
            boolean ret = name.endsWith(".parquet");
            return ret;
        }
    }
}
