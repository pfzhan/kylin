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
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import com.google.common.collect.Lists;

public class ParquetSpliceMRSteps extends ParquetMRSteps {
    public ParquetSpliceMRSteps(CubeSegment seg) {
        super(seg);
    }

    @Override
    public MapReduceExecutable createCubePageIndexStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Build Columnar Page Index");
        result.setMapReduceJobClass(ParuqetSplicePageIndexJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Build_Columnar_Page_Index_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getCubeFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getCubePageIndexTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    @Override
    public MapReduceExecutable createCubeTarballStep(String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName("Tarball Columnar Files");
        result.setMapReduceJobClass(ParquetSpliceTarballJob.class);
        result.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Parquet_Tarball_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getCubeFolderPath(seg));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getCubeTarballTmpFolderPath(seg));

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    @Override
    public CubeShardSizingStep createCubeShardSizingStep(String jobId) {
        CubeShardSizingStep result = new CubeSpliceShardSizingStep();
        result.setName("Sizing Columnar Shards");
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    @Override
    public void addCubeGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        List<String> toCleanFolders = Lists.newArrayList(getCubeFolderPath(seg));
        List<String> toCleanFileSuffixs = Lists.newArrayList(".parquet", ".parquet.inv", ".tmp");

        ParquetStorageSpliceCleanupStep step = new ParquetStorageSpliceCleanupStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setToCleanFolders(toCleanFolders);
        step.setToCleanFileSuffix(toCleanFileSuffixs);

        jobFlow.addTask(step);
    }

    @Override
    public ParquetCubeInfoCollectionStep createCubeInfoCollectionStep(String jobId, CubeSegment segment) {
        ParquetCubeInfoCollectionStep step = new ParquetCubeInfoCollectionStep(segment);
        step.setName("Collect Cube File Mapping");
        step.setParam(ParquetCubeInfoCollectionStep.INPUT_PATH, getCubeFolderPath(seg));
        step.setParam(ParquetCubeInfoCollectionStep.OUTPUT_PATH, getCubeFolderPath(seg) + ParquetCubeInfoCollectionStep.CUBE_INFO_NAME);
        return step;
    }
}
