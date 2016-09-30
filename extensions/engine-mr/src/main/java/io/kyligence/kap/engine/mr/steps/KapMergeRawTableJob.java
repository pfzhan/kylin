package io.kyligence.kap.engine.mr.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.cube.raw.RawTableSegment;
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

            RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());
            RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);
            RawTableSegment rawSeg = raw.getSegmentById(segmentID);
            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = cubeManager.getCube(cubeName);

            // start job
            String jobName = getOptionValue(OPTION_JOB_NAME);
            logger.info("Starting: " + jobName);
            job = Job.getInstance(getConf(), jobName);

            setJobClasspath(job, raw.getConfig());

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
            attachKylinPropsAndMetadata(rawSeg, cube, job.getConfiguration());

            // set path for output
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_OUTPUT_DIR, getWorkingDir(raw.getConfig(), raw, rawSeg));

            //push down kylin config
            job.getConfiguration().set(ParquetFormatConstants.KYLIN_SCAN_PROPERTIES, KylinConfig.getInstanceFromEnv().getConfigAsString());

            if (raw.getShardNumber() > 0)
                job.getConfiguration().setInt(MAPRED_REDUCE_TASKS, raw.getShardNumber());

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

    private void attachKylinPropsAndMetadata(RawTableSegment rawSegment, CubeInstance cube, Configuration conf) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(rawSegment.getConfig());
        RawTableInstance instance = rawSegment.getRawTableInstance();
        // write raw/cube / model_desc / raw_desc / dict / table
        ArrayList<String> dumpList = new ArrayList<String>();
        dumpList.add(instance.getResourcePath());
        dumpList.add(instance.getRawTableDesc().getModel().getResourcePath());
        dumpList.add(instance.getRawTableDesc().getResourcePath());
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());

        for (String tableName : instance.getRawTableDesc().getModel().getAllTables()) {
            TableDesc table = metaMgr.getTableDesc(tableName);
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);
            dumpList.addAll(dependentResources);
        }
        attachKylinPropsAndMetadata(dumpList, instance.getConfig(), conf);
    }

    public static String getWorkingDir(KylinConfig config, RawTableInstance raw, RawTableSegment rawSegment) {
        return new StringBuffer(KapConfig.wrap(config).getParquentStoragePath()).append(raw.getUuid()).append("/").append(rawSegment.getUuid()).append("/").toString();
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
                for (FileStatus stat : fileStatuses) {
                    if (stat.getPath().getName().endsWith(".parquet"))
                        ret += addRawInputDirs(new String[] { stat.getPath().toString() }, job);
                }
            } else {
                logger.info("Add input " + inp);
                FileInputFormat.addInputPath(job, new Path(inp));
                ret++;
            }
        }
        return ret;
    }
}
