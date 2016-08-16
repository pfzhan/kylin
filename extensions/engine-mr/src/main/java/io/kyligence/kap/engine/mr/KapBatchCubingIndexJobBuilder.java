package io.kyligence.kap.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.engine.mr.steps.KapBaseCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapInMemCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapNDCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapRawTableJob;
import io.kyligence.kap.engine.mr.steps.SecondaryIndexJob;

/**
 * Created by wangcheng on 8/4/16.
 */
public class KapBatchCubingIndexJobBuilder extends BatchCubingJobBuilder2 {
    private static final Logger logger = LoggerFactory.getLogger(KapBatchCubingIndexJobBuilder.class);

    private final IMRInput.IMRBatchCubingInputSide inputSide;
    private final IMROutput2.IMRBatchCubingOutputSide2 outputSide;

    public KapBatchCubingIndexJobBuilder(CubeSegment newSegment, String submitter) {

        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2((CubeSegment) seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);

        final CubingJob result = CubingJob.createBuildJob((CubeSegment) seg, submitter, config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        result.addTask(createFactDistinctColumnsStepWithStats(jobId));
        result.addTask(createBuildDictionaryStep(jobId));
        result.addTask(createSaveStatisticsStep(jobId));
        outputSide.addStepPhase2_BuildDictionary(result);

        if (RawTableInstance.isRawTableEnabled(seg.getCubeDesc())) {
            RawTableInstance rawIns = new RawTableInstance(seg.getCubeInstance());
            RawTableDesc rawDesc = rawIns.getRawTableDesc();
            rawDesc.getDimensions();
            result.addTask(createRawTableStep(cuboidRootPath, jobId));
        }

        // Phase 3: Build Cube
        buildBasicCuboidOnly(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(getStatisticsPath(jobId), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    private void buildBasicCuboidOnly(final CubingJob result, final String jobId, final String cuboidRootPath) {
        RowKeyDesc rowKeyDesc = ((CubeSegment) seg).getCubeDesc().getRowkey();
        final int groupRowkeyColumnsCount = ((CubeSegment) seg).getCubeDesc().getBuildLevel();
        final int totalRowkeyColumnsCount = rowKeyDesc.getRowKeyColumns().length;
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);
        // base cuboid step
        result.addTask(createBaseCuboidStep(cuboidOutputTempPath, jobId));
    }

    private MapReduceExecutable createBaseCuboidStep(String[] cuboidOutputTempPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputTempPath[0]);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Base_Cuboid_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(getBaseCuboidJob());
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
    }

    private MapReduceExecutable createRawTableStep(String rawTableOutputTempPath, String jobId) {
        // base cuboid job
        MapReduceExecutable rawTableStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        rawTableStep.setName(ExecutableConstants.STEP_NAME_BUILD_RAWTABLE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, rawTableOutputTempPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Raw_Table_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        rawTableStep.setMapReduceParams(cmd.toString());
        rawTableStep.setMapReduceJobClass(getRawTableJob());
        rawTableStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return rawTableStep;
    }

    @SuppressWarnings("unused")
    private MapReduceExecutable createBuildSecondaryIndexStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Secondary Index");
        result.setMapReduceJobClass(SecondaryIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getSecondaryIndexPath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Second_Index_" + seg.getRealization().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    protected Class<? extends AbstractHadoopJob> getInMemCuboidJob() {
        return KapInMemCuboidJob.class;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return KapNDCuboidJob.class;
    }

    protected Class<? extends AbstractHadoopJob> getBaseCuboidJob() {
        return KapBaseCuboidJob.class;
    }

    protected Class<? extends AbstractHadoopJob> getRawTableJob() {
        return KapRawTableJob.class;
    }
}
