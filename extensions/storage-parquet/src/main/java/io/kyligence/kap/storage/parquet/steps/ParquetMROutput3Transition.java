package io.kyligence.kap.storage.parquet.steps;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.engine.mr.IMROutput3;

public class ParquetMROutput3Transition implements IMROutput3 {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ParquetMROutput3Transition.class);

    @Override
    public IMROutput3.IMRBatchCubingOutputSide3 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMROutput3.IMRBatchCubingOutputSide3() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
                jobFlow.addTask(steps.createParquetPageIndex(jobFlow.getId()));
                jobFlow.addTask(steps.createParquetTarballJob(jobFlow.getId()));
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }
        };
    }

    @Override
    public IMROutput3.IMRBatchMergeOutputSide3 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMROutput3.IMRBatchMergeOutputSide3() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                //                jobFlow.addTask(steps.createCreateHTableStepWithStats(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
                //                jobFlow.addTask(steps.createConvertCuboidToHfileStep(cuboidRootPath, jobFlow.getId()));
                //                jobFlow.addTask(steps.createBulkLoadStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                //                steps.addMergingGarbageCollectionSteps(jobFlow);
            }
        };
    }
}
