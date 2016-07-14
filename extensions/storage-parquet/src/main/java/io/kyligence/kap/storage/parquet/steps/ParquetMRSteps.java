package io.kyligence.kap.storage.parquet.steps;

import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class ParquetMRSteps extends JobBuilderSupport {
    public ParquetMRSteps(CubeSegment seg) {
        super(seg, null);
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

    public void addMergingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        List<String> toCleanFolders = getMergingSegmentsParquetFolders();

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
        return new StringBuffer(config.getHdfsWorkingDirectory()).append("parquet/").append(cubeSegment.getCubeInstance().getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }
}
