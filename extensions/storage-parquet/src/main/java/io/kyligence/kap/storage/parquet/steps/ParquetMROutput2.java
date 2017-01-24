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

package io.kyligence.kap.storage.parquet.steps;

import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.engine.mr.steps.KapMergeCuboidJob;
import io.kyligence.kap.engine.mr.steps.KapMergeRawTableJob;

public class ParquetMROutput2 implements IMROutput2 {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ParquetMROutput2.class);

    protected ParquetMRSteps createParquetMRSteps(CubeSegment seg) {
        return new ParquetMRSteps(seg);
    }

    @Override
    public IMROutput2.IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchCubingOutputSide2() {
            ParquetMRSteps steps = createParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig()).getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubeShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubePageIndexStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubePageIndexCleanupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballCleaupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeInfoCollectionStep(jobFlow.getId()));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createRawtableShardSizingStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexCleanupStep(jobFlow.getId()));
                }
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubeGarbageCollectionSteps(jobFlow);
            }
        };
    }

    @Override
    public IMROutput2.IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchMergeOutputSide2() {
            ParquetMRSteps steps = createParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig()).getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubeShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments, DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createCubeMergeStep(seg, mergingSegments, jobFlow.getId(), getCubeMergeJobClass()));
                jobFlow.addTask(steps.createCubeMergeCleanupStep(jobFlow.getId(), seg));
                jobFlow.addTask(steps.createCubePageIndexStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubePageIndexCleanupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeTarballCleaupStep(jobFlow.getId()));
                jobFlow.addTask(steps.createCubeInfoCollectionStep(jobFlow.getId()));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createRawtableMergeStep(seg, jobFlow.getId(), KapMergeRawTableJob.class));
                    jobFlow.addTask(steps.createRawtableMergeCleanupStep(jobFlow.getId(), seg));
                    jobFlow.addTask(steps.createRawtablePageIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtablePageIndexCleanupStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawtableFuzzyIndexCleanupStep(jobFlow.getId()));
                }
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubeGarbageCollectionSteps(jobFlow);
                steps.addMergeGarbageCollectionSteps(jobFlow);
            }
        };
    }

    protected Class<? extends AbstractHadoopJob> getCubeMergeJobClass() {
        return KapMergeCuboidJob.class;
    }
}
