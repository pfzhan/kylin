/**
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

import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.BatchMergeJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.MergeStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.parquet.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.cube.raw.RawTableManager;
import io.kyligence.kap.engine.mr.steps.KapMergeCuboidJob;
import io.kyligence.kap.engine.mr.steps.MergeSecondaryIndexStep;
import io.kyligence.kap.engine.mr.steps.UpdateRawTableInfoAfterMergeStep;

public class KapBatchMergeJobBuilder extends JobBuilderSupport {

    private static final Logger logger = LoggerFactory.getLogger(BatchMergeJobBuilder2.class);

    private final IMROutput2.IMRBatchMergeOutputSide2 outputSide;

    public KapBatchMergeJobBuilder(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = MRUtil.getBatchMergeOutputSide2(seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to MERGE segment " + seg);

        final CubeSegment cubeSegment = seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge, target segment " + cubeSegment);
        final List<String> mergingSegmentIds = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
        }

        // Phase 1: Merge Dictionary
        result.addTask(createMergeDictionaryStep(mergingSegmentIds));
        result.addTask(createMergeStatisticsStep(cubeSegment, mergingSegmentIds, getStatisticsPath(jobId)));
        outputSide.addStepPhase1_MergeDictionary(result);

        // Phase 2: Merge Cube Files
        outputSide.addStepPhase2_BuildCube(seg, mergingSegments, result);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        if (null != detectRawTable())
            result.addTask(createUpdateRawTableInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    private MergeStatisticsStep createMergeStatisticsStep(CubeSegment seg, List<String> mergingSegmentIds, String mergedStatisticsFolder) {
        MergeStatisticsStep result = new MergeStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        CubingExecutableUtil.setMergedStatisticsPath(mergedStatisticsFolder, result.getParams());

        return result;
    }

    private RawTableInstance detectRawTable() {
        RawTableInstance rawInstance = RawTableManager.getInstance(seg.getConfig()).getRawTableInstance(seg.getRealization().getName());
        logger.info("Raw table is " + (rawInstance == null ? "not " : "") + "specified in this cubing job " + seg);
        return rawInstance;
    }

    private MergeSecondaryIndexStep createMergeSecondaryIndexStep(CubingJob cubingJob) {
        MergeSecondaryIndexStep result = new MergeSecondaryIndexStep();
        result.setName("Merge Secondary Index");

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setIndexPath(this.getSecondaryIndexPath(cubingJob.getId()), result.getParams());
        return result;
    }

    public UpdateRawTableInfoAfterMergeStep createUpdateRawTableInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateRawTableInfoAfterMergeStep result = new UpdateRawTableInfoAfterMergeStep();
        result.setName("Update RawTable Info After Merge");

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        return result;
    }

    protected Class<? extends AbstractHadoopJob> getMergeCuboidJob() {
        return KapMergeCuboidJob.class;
    }
}
