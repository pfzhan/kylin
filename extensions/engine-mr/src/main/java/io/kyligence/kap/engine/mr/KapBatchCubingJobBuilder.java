/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package io.kyligence.kap.engine.mr;

import io.kyligence.kap.engine.mr.IMROutput3.IMRBatchCubingOutputSide3 ;
import io.kyligence.kap.engine.mr.steps.KapBaseCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapInMemCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapNDCuboidJob;
import io.kyligence.kap.engine.mr.steps.SecondaryIndexJob;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KapBatchCubingJobBuilder extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    private final IMRBatchCubingInputSide inputSide;
    private final IMRBatchCubingOutputSide3 outputSide;

    public KapBatchCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = KapMRUtil.getBatchCubingOutputSide3((CubeSegment) seg);
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

        // Phase 3: Build Cube
        addLayerCubingSteps(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute
        result.addTask(createInMemCubingStep(jobId, cuboidRootPath)); // inmem cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result, cuboidRootPath);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
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

    private MapReduceExecutable createInMemCubingStep(String jobId, String cuboidRootPath) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidRootPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(KapInMemCuboidJob.class);
        cubeStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES + "," + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

    private void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        RowKeyDesc rowKeyDesc = ((CubeSegment) seg).getCubeDesc().getRowkey();
        final int groupRowkeyColumnsCount = ((CubeSegment) seg).getCubeDesc().getBuildLevel();
        final int totalRowkeyColumnsCount = rowKeyDesc.getRowKeyColumns().length;
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);
        // base cuboid step
        result.addTask(createBaseCuboidStep(cuboidOutputTempPath, jobId));
        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnsCount - i;
            result.addTask(createNDimensionCuboidStep(cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount, jobId));
        }
    }

    private MapReduceExecutable createNDimensionCuboidStep(String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount, String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + (totalRowkeyColumnCount - dimNum));
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(KapNDCuboidJob.class);
        return ndCuboidStep;
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
        baseCuboidStep.setMapReduceJobClass(KapBaseCuboidJob.class);
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
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
}
