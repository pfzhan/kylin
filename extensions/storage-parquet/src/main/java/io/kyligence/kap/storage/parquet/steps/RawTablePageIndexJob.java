package io.kyligence.kap.storage.parquet.steps;

import static io.kyligence.kap.engine.mr.steps.ParquertMRJobUtils.addParquetInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kylin.common.KylinConfig;
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
import io.kyligence.kap.storage.parquet.format.ParquetRawTablePageInputFormat;

public class RawTablePageIndexJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(RawTablePageIndexJob.class);

    protected boolean skipped = false;

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

            RawTableManager rawMgr = RawTableManager.getInstance(KylinConfig.getInstanceFromEnv());
            RawTableInstance raw = rawMgr.getRawTableInstance(cubeName);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            setJobClasspath(job, raw.getConfig());

            int inputNum = setJobInputFile(job, new Path(getOptionValue(OPTION_INPUT_PATH)));
            if (inputNum == 0) {
                skipped = true;
                logger.info("ParquetPageIndexJob is skipped because there's no input file");
                return 0;
            }

            setMapperClass(job);
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setInputFormatClass(ParquetRawTablePageInputFormat.class);
            job.setNumReduceTasks(0);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(raw.getSegmentById(segmentID), job.getConfiguration());
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
        job.setMapperClass(RawTablePageIndexMapper.class);
    }

    private void attachKylinPropsAndMetadata(RawTableSegment rawSegment, Configuration conf) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(rawSegment.getConfig());
        RawTableInstance instance = rawSegment.getRawTableInstance();
        // write raw / model_desc / raw_desc / dict / table
        ArrayList<String> dumpList = new ArrayList<String>();
        dumpList.add(instance.getResourcePath());
        dumpList.add(instance.getRawTableDesc().getModel().getResourcePath());
        dumpList.add(instance.getRawTableDesc().getResourcePath());

        for (String tableName : instance.getRawTableDesc().getModel().getAllTables()) {
            TableDesc table = metaMgr.getTableDesc(tableName);
            dumpList.add(table.getResourcePath());
            List<String> dependentResources = SourceFactory.getMRDependentResources(table);
            dumpList.addAll(dependentResources);
        }
        attachKylinPropsAndMetadata(dumpList, instance.getConfig(), conf);
    }

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
}
