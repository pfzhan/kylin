package io.kyligence.kap.storage.parquet.steps;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.metadata.realization.IRealizationSegment;

public class ParquetMRSteps extends JobBuilderSupport {
    public ParquetMRSteps(IRealizationSegment seg) {
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
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetPath((CubeSegment) seg));
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

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Tarball_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getParquetPath((CubeSegment) seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getJobWorkingDir(jobId) + "/parquet.inv"); // just tmp files

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private String getParquetPath(CubeSegment cubeSegment) {
        return new StringBuffer(config.getHdfsWorkingDirectory()).append("parquet/").append(cubeSegment.getCubeInstance().getUuid()).append("/").append(cubeSegment.getUuid()).append("/").toString();
    }
}
