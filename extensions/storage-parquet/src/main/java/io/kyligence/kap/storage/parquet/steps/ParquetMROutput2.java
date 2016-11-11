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

    @Override
    public IMROutput2.IMRBatchCubingOutputSide2 getBatchCubingOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchCubingOutputSide2() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig()).getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createParquetShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createParquetPageIndex(jobFlow.getId()));
                jobFlow.addTask(steps.createParquetTarballJob(jobFlow.getId()));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createRawShardSizingStep(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawTableStep());
                    jobFlow.addTask(steps.createRawTableParquetPageIndex(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawTableParquetPageFuzzyIndex(jobFlow.getId()));
                }
            }

            @Override
            public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubingGarbageCollectionSteps(jobFlow);
                if (isRawTableEnable) {
                    steps.addRawTableCubingGarbageCollectionSteps(jobFlow);
                }
            }
        };
    }

    @Override
    public IMROutput2.IMRBatchMergeOutputSide2 getBatchMergeOutputSide(final CubeSegment seg) {
        return new IMROutput2.IMRBatchMergeOutputSide2() {
            ParquetMRSteps steps = new ParquetMRSteps(seg);
            RawTableInstance raw = RawTableManager.getInstance(seg.getConfig()).getAccompanyRawTable(seg.getCubeInstance());
            boolean isRawTableEnable = (null != raw);

            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createParquetShardSizingStep(jobFlow.getId()));
            }

            @Override
            public void addStepPhase2_BuildCube(CubeSegment seg, List<CubeSegment> mergingSegments, DefaultChainedExecutable jobFlow) {
                jobFlow.addTask(steps.createMergeCuboidDataStep(seg, mergingSegments, jobFlow.getId(), KapMergeCuboidJob.class));
                jobFlow.addTask(steps.createParquetPageIndex(jobFlow.getId()));
                jobFlow.addTask(steps.createParquetTarballJob(jobFlow.getId()));
                if (isRawTableEnable) {
                    jobFlow.addTask(steps.createMergeRawDataStep(seg, jobFlow.getId(), KapMergeRawTableJob.class));
                    jobFlow.addTask(steps.createRawTableParquetPageIndex(jobFlow.getId()));
                    jobFlow.addTask(steps.createRawTableParquetPageFuzzyIndex(jobFlow.getId()));
                }
            }

            @Override
            public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow) {
                steps.addCubingGarbageCollectionSteps(jobFlow);
                steps.addMergingGarbageCollectionSteps(jobFlow);
            }
        };
    }
}
