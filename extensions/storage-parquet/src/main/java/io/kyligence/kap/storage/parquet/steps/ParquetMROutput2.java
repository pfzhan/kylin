package io.kyligence.kap.storage.parquet.steps;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetMROutput2 implements IMROutput2 {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ParquetMROutput2.class);

    @Override
    public IMROutput2.IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchCubingOutputSide2() {
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
    public IMROutput2.IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchMergeOutputSide2() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }

            @Override
            public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
                jobFlow.addTask(steps.createParquetPageIndex(jobFlow.getId()));
                jobFlow.addTask(steps.createParquetTarballJob(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                // nothing to do
            }
        };
    }
}
