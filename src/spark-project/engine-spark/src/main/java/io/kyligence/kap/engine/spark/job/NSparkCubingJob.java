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

import static org.apache.kylin.job.factory.JobFactoryConstant.CUBE_JOB_FACTORY;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import io.kyligence.kap.common.scheduler.CubingJobFinishedNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import lombok.val;

/**
 *
 */
public class NSparkCubingJob extends DefaultChainedExecutableOnModel {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkCubingJob.class);

    static {
        JobFactory.register(CUBE_JOB_FACTORY, new CubingJobFactory());
    }

    static class CubingJobFactory extends JobFactory {

        private CubingJobFactory() {
        }

        @Override
        protected NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
                JobTypeEnum jobType, String jobId, Set<LayoutEntity> toBeDeletedLayouts) {
            return NSparkCubingJob.create(segments, layouts, submitter, jobType, jobId, toBeDeletedLayouts);
        }
    }

    // for test use only
    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter) {
        return create(segments, layouts, submitter, JobTypeEnum.INDEX_BUILD, UUID.randomUUID().toString());
    }

    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
            JobTypeEnum jobType, String jobId, Set<LayoutEntity> toBeDeletedLayouts) {

        NSparkCubingJob sparkCubingJob = create(segments, layouts, submitter, jobType, jobId);
        if (CollectionUtils.isNotEmpty(toBeDeletedLayouts)) {
            sparkCubingJob.setParam(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS,
                    NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(toBeDeletedLayouts)));
        }
        return sparkCubingJob;
    }

    public static NSparkCubingJob create(Set<NDataSegment> segments, Set<LayoutEntity> layouts, String submitter,
            JobTypeEnum jobType, String jobId) {
        Preconditions.checkArgument(!segments.isEmpty());
        Preconditions.checkArgument(submitter != null);
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            Preconditions.checkArgument(!layouts.isEmpty());
        }
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
        job.setId(jobId);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setTargetSubject(segments.iterator().next().getModel().getUuid());
        job.setTargetSegments(segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        job.setProject(df.getProject());
        job.setSubmitter(submitter);

        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_PROJECT_NAME, df.getProject());
        job.setParam(NBatchConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(NBatchConstants.P_DATAFLOW_ID, df.getId());
        job.setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        job.setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));

        JobStepFactory.addStep(job, JobStepType.RESOURCE_DETECT, segments);
        JobStepFactory.addStep(job, JobStepType.CUBING, segments);
        JobStepFactory.addStep(job, JobStepType.UPDATE_METADATA, segments);
        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

    public NSparkCubingStep getSparkCubingStep() {
        return getTask(NSparkCubingStep.class);
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    @Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowId());
        List<NDataSegment> toRemovedSegments = new ArrayList<>();
        for (String id : getSparkCubingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)
                    && !segment.getStatus().equals(SegmentStatusEnum.WARNING)) {
                toRemovedSegments.add(segment);
            }
        }
        NDataSegment[] nDataSegments = toRemovedSegments.toArray(new NDataSegment[0]);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
    }

    @Override
    public boolean safetyIfDiscard() {
        if (checkSuicide() || this.getStatus().isFinalState() || this.getJobType() != JobTypeEnum.INC_BUILD) {
            return true;
        }

        val dataflow = NDataflowManager.getInstance(getConfig(), getProject())
                .getDataflow(getSparkCubingStep().getDataflowId());
        val segs = dataflow.getSegments().stream()
                .filter(nDataSegment -> !getTargetSegments().contains(nDataSegment.getId()))
                .collect(Collectors.toList());
        val toDeletedSeg = dataflow.getSegments().stream()
                .filter(nDataSegment -> getTargetSegments().contains(nDataSegment.getId()))
                .collect(Collectors.toList());
        val segHoles = NDataflowManager.getInstance(getConfig(), getProject())
                .calculateHoles(getSparkCubingStep().getDataflowId(), segs);

        for (NDataSegment segHole : segHoles) {
            for (NDataSegment deleteSeg : toDeletedSeg) {
                if (segHole.getSegRange().overlaps(deleteSeg.getSegRange())
                        || segHole.getSegRange().contains(deleteSeg.getSegRange())) {
                    return false;
                }

            }
        }

        return true;
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result) throws JobStoppedException {
        super.onExecuteFinished(result);

        // post cubing job finished event with project and model Id
        EventBusFactory.getInstance()
                .postAsync(new CubingJobFinishedNotifier(getProject(), getTargetSubject(), getSubmitter()));
    }
}
