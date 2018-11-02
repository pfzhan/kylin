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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.kyligence.kap.cube.model.NDataflowManager;
import io.kyligence.kap.cube.model.NDataflowUpdate;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;

/**
 */
public class NSparkCubingJob extends DefaultChainedExecutable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    public NSparkCubingJob() {
    }

    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<NCuboidLayout> layouts, String submitter) {
        return create(segments, layouts, submitter, JobTypeEnum.INDEX_BUILD);
    }

    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<NCuboidLayout> layouts, String submitter,
            JobTypeEnum jobType) {
        Preconditions.checkArgument(segments.size() > 0);
        Preconditions.checkArgument(layouts.size() > 0);
        Preconditions.checkArgument(submitter != null);
        NDataflow df = segments.iterator().next().getDataflow();
        NSparkCubingJob job = new NSparkCubingJob();
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : segments) {
            startTime = startTime < Long.parseLong(segment.getSegRange().getStart().toString()) ? startTime
                    : Long.parseLong(segment.getSegRange().getStart().toString());
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        job.setName(jobType.toString());
        job.setDataRangeStart(startTime);
        job.setDataRangeEnd(endTime);
        job.setTargetSubject(segments.iterator().next().getModel().getAlias());
        job.setProject(df.getProject());
        job.setSubmitter(submitter);
        job.addSparkAnalysisStep(segments, layouts);
        job.addSparkCubingStep(segments, layouts);
        job.addUpdateAfterBuildStep();
        return job;
    }

    public NSparkAnalysisStep getSparkAnalysisStep() {
        return (NSparkAnalysisStep) getTasks().get(0);
    }

    public NSparkCubingStep getSparkCubingStep() {
        return (NSparkCubingStep) getTasks().get(1);
    }

    private void addSparkAnalysisStep(Set<NDataSegment> segments, Set<NCuboidLayout> layouts) {
        final NSparkAnalysisStep step = new NSparkAnalysisStep();
        NDataflow df = segments.iterator().next().getDataflow();
        KylinConfigExt config = df.getConfig();
        step.setName(ExecutableConstants.STEP_NAME_DATA_PROFILING);
        step.setJobId(getId());
        step.setProject(getProject());
        step.setProjectParam();
        step.setDataflowName(df.getName());
        step.setSegmentIds(NSparkCubingUtil.toSegmentIds(segments));
        step.setCuboidLayoutIds(NSparkCubingUtil.toCuboidLayoutIds(layouts));
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(step.getId()).toString());
        this.addTask(step);
    }

    private void addSparkCubingStep(Set<NDataSegment> segments, Set<NCuboidLayout> layouts) {
        NSparkCubingStep step = new NSparkCubingStep();
        NDataflow df = segments.iterator().next().getDataflow();
        KylinConfigExt config = df.getConfig();
        step.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
        step.setProject(getProject());
        step.setProjectParam();
        step.setDataflowName(df.getName());
        step.setSegmentIds(NSparkCubingUtil.toSegmentIds(segments));
        step.setCuboidLayoutIds(NSparkCubingUtil.toCuboidLayoutIds(layouts));
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(step.getId()).toString());
        step.setJobId(getId());
        this.addTask(step);
    }

    private void addUpdateAfterBuildStep() {
        NSparkCubingUpdateAfterBuildStep step = new NSparkCubingUpdateAfterBuildStep();
        step.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        step.setProject(getProject());
        this.addTask(step);
    }

    @Override
    public void cancelJob() throws IOException {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowName());
        List<NDataSegment> segments = new ArrayList<>();
        for (Integer id : getSparkCubingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)) {
                segments.add(segment);
            }
        }
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getName());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        NDefaultScheduler.stopThread(getId());
    }

}
