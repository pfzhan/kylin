/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.mr;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterBuildStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.engine.mr.steps.KapBaseCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapInMemCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapNDCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapUpdateCubeInfoAfterBuildStep;
import io.kyligence.kap.engine.mr.steps.UpdateOutputDirStep;
import io.kyligence.kap.engine.mr.steps.UpdateRawTableInfoAfterBuildStep;

public class KapBatchCubingJobBuilder extends JobBuilderSupport {

    private static final Logger logger = LoggerFactory.getLogger(KapBatchCubingJobBuilder.class);

    private final String InmemCubeTmpFolderPrefix = "inmem";
    protected final IMRInput.IMRBatchCubingInputSide inputSide;
    protected final IMROutput2.IMRBatchCubingOutputSide2 outputSide;

    public KapBatchCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);

        final CubingJob result = CubingJob.createBuildJob((CubeSegment) seg, submitter, config);
        final String jobId = result.getId();
        final String cubeRootPath = getCubeRootPath(seg);

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        result.addTask(createFactDistinctColumnsStep(jobId));
        result.addTask(createBuildDictionaryStep(jobId));
        result.addTask(createSaveStatisticsStep(jobId));
        outputSide.addStepPhase2_BuildDictionary(result);

        // Phase 3: Build Cube and RawTable
        addLayerCubingSteps(result, jobId, cubeRootPath); // layer cubing, only selected algorithm will execute
        addInmemCubingSteps(result, jobId, cubeRootPath); // inmem cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));
        if (null != detectRawTable())
            result.addTask(createUpdateRawTableInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private String getCubeRootPath(CubeSegment seg) {
        CubeInstance cube = seg.getCubeInstance();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        return new StringBuffer(KapConfig.wrap(kylinConfig).getWriteParquetStoragePath()).append(cube.getUuid())
                .append("/")
                .append(seg.getUuid()).append("/").toString();
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

    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        final int maxLevel = seg.getCuboidScheduler().getBuildLevel();
        // base cuboid step
        result.addTask(createBaseCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, 0), jobId));
        // n dim cuboid steps
        for (int i = 1; i <= maxLevel; i++) {
            result.addTask(createNDimensionCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, i - 1),
                    getCuboidOutputPathsByLevel(cuboidRootPath, i), i, jobId));
        }

        result.addTask(createUpdateLayerOutputDirStep(cuboidRootPath, jobId));
    }

    protected void addInmemCubingSteps(final CubingJob result, final String jobId, final String cubeRootPath) {
        result.addTask(createInMemCubingStep(jobId, cubeRootPath));
        result.addTask(createUpdateInmemOutputDirStep(cubeRootPath, jobId));
    }

    protected UpdateOutputDirStep createUpdateLayerOutputDirStep(final String cubeRootPath, final String jobId) {
        UpdateOutputDirStep updateDirStep = new UpdateOutputDirStep();
        updateDirStep.setOutputDir(cubeRootPath);
        updateDirStep.setSubdirFilter(LayeredCuboidFolderPrefix);
        updateDirStep.setJobId(jobId);
        updateDirStep.setCheckSkip(true);
        updateDirStep.setCheckAlgorithm("LAYER");
        updateDirStep.setName("Clean Layer Cubing Output");
        return updateDirStep;
    }

    private UpdateOutputDirStep createUpdateInmemOutputDirStep(final String cubeRootPath, final String jobId) {
        UpdateOutputDirStep updateDirStep = new UpdateOutputDirStep();
        updateDirStep.setOutputDir(cubeRootPath);
        updateDirStep.setSubdirFilter(InmemCubeTmpFolderPrefix);
        updateDirStep.setJobId(jobId);
        updateDirStep.setCheckSkip(true);
        updateDirStep.setCheckAlgorithm("INMEM");
        updateDirStep.setName("Clean InMem Cubing Output");
        return updateDirStep;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String parentPath, String outputPath, int level,
            String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : level " + level);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, parentPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + level);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    private MapReduceExecutable createInMemCubingStep(String jobId, String cuboidRootPath) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidRootPath + "/" + InmemCubeTmpFolderPrefix);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(getInMemCuboidJob());
        cubeStep.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

    private MapReduceExecutable createBaseCuboidStep(String cubeOutputPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cubeOutputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Base_Cuboid_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(getBaseCuboidJob());
        return baseCuboidStep;
    }

    private RawTableInstance detectRawTable() {
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig())
                .getAccompanyRawTable(seg.getCubeInstance());
        logger.info("Raw table is " + (rawInstance == null ? "not " : "") + "specified in this cubing job " + seg);
        return rawInstance;
    }

    public UpdateRawTableInfoAfterBuildStep createUpdateRawTableInfoAfterBuildStep(String jobId) {
        final UpdateRawTableInfoAfterBuildStep result = new UpdateRawTableInfoAfterBuildStep();
        result.setName("Update RawTable Information");

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    @Override
    public UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(String jobId) {
        final KapUpdateCubeInfoAfterBuildStep result = new KapUpdateCubeInfoAfterBuildStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.getParams().put(BatchConstants.CFG_OUTPUT_PATH, getFactDistinctColumnsPath(jobId));

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());

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
}
