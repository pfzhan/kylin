package io.kyligence.kap.storage.parquet.steps;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ParquetMRSteps extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(ParquetMRSteps.class);

    public ParquetMRSteps(CubeSegment seg) {
        super(seg, null);
    }

    public MapReduceExecutable createMergeCuboidDataStep(CubeSegment seg, List<CubeSegment> mergingSegments, String jobID, Class<? extends AbstractHadoopJob> clazz) {

        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getParquetFolderPath(merging) + "*");
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = getParquetFolderPath(seg);

        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(clazz);
        return mergeCuboidDataStep;
    }

    public MapReduceExecutable createParquetPageIndex(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Parquet Page Index");
        result.setMapReduceJobClass(ParquetPageIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Parquet_Page_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetFolderPath((CubeSegment) seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getJobWorkingDir(jobId) + "/parquet.inv"); // just tmp files

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public MapReduceExecutable createParquetTarballJob(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Tarball Parquet Files");
        result.setMapReduceJobClass(ParquetTarballJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Tarball_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetFolderPath((CubeSegment) seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getJobWorkingDir(jobId) + "/parquet.inv"); // just tmp files

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    public List<String> getMergingSegmentsParquetFolders() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> ret = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            ret.add(getParquetFolderPath(merging));
        }
        return ret;
    }

    public List<String> getMergingSegmentJobWorkingDirs() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization()).getMergingSegments((CubeSegment) seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHDFSPaths.add(getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }

    public void addMergingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {

        //clean two parts: 1.parquet storage folders 2. working dirs
        List<String> toCleanFolders = getMergingSegmentsParquetFolders();
        toCleanFolders.addAll(getMergingSegmentJobWorkingDirs());

        logger.info("toCleanFolders are :" + toCleanFolders);

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(null);//delete all source folder for merge

        jobFlow.addTask(step);
    }

    public void addCubingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        List<String> toCleanFolders = Lists.newArrayList(getParquetFolderPath(seg));
        List<String> toCleanFileSuffixs = Lists.newArrayList(".parquet", ".parquet.inv");

        ParquetStorageCleanupStep step = new ParquetStorageCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(toCleanFileSuffixs);

        jobFlow.addTask(step);
    }

    private String getParquetFolderPath(CubeSegment cubeSegment) {
        return new StringBuffer(KapConfig.wrap(config.getConfig()).getParquentStoragePath()).append(cubeSegment.getCubeInstance().getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }


}
