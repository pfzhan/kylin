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

package io.kyligence.kap.job.execution;

import static java.util.stream.Collectors.joining;
import static org.apache.kylin.job.factory.JobFactoryConstant.MERGE_JOB_FACTORY;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.job.execution.step.JobStepType;
import io.kyligence.kap.job.execution.step.NResourceDetectStep;
import io.kyligence.kap.job.execution.step.NSparkCleanupAfterMergeStep;
import io.kyligence.kap.job.execution.step.NSparkMergingStep;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.job.JobBucket;

public class NSparkMergingJob extends DefaultChainedExecutableOnModel {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    static {
        JobFactory.register(MERGE_JOB_FACTORY, new MergingJobFactory());
    }

    static class MergingJobFactory extends JobFactory {

        private MergingJobFactory() {
        }

        @Override
        protected NSparkMergingJob create(JobBuildParams jobBuildParams) {
            if (jobBuildParams.getSegments() == null || jobBuildParams.getSegments().size() != 1) {
                return null;
            }
            return merge(jobBuildParams.getSegments().iterator().next(), jobBuildParams.getLayouts(),
                    jobBuildParams.getSubmitter(), jobBuildParams.getJobId(), jobBuildParams.getPartitions(),
                    jobBuildParams.getBuckets());
        }
    }

    public NSparkMergingJob() {
        super();
    }

    public NSparkMergingJob(Object notSetId) {
        super(notSetId);
    }

    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter,
            String jobId) {
        return merge(mergedSegment, layouts, submitter, jobId, null, null);
    }

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     * @param layouts,       user is allowed to specify the cuboids to merge. By default, it is null and merge all
     *                       the ready cuboids in the segments.
     */
    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter,
            String jobId, Set<Long> partitions, Set<JobBucket> buckets) {
        Preconditions.checkArgument(mergedSegment != null);
        Preconditions.checkArgument(submitter != null);

        NDataflow df = mergedSegment.getDataflow();
        if (layouts == null) {
            layouts = Sets.newHashSet(df.getIndexPlan().getAllLayouts());
        }

        NSparkMergingJob job = new NSparkMergingJob();
        job.setName(JobTypeEnum.INDEX_MERGE.toString());
        job.setJobType(JobTypeEnum.INDEX_MERGE);
        job.setId(jobId);
        job.setTargetSubject(mergedSegment.getModel().getUuid());
        job.setTargetSegments(Lists.newArrayList(String.valueOf(mergedSegment.getId())));
        job.setProject(mergedSegment.getProject());
        job.setSubmitter(submitter);

        if (CollectionUtils.isNotEmpty(partitions)) {
            job.setTargetPartitions(partitions);
            job.setParam(NBatchConstants.P_PARTITION_IDS,
                    job.getTargetPartitions().stream().map(String::valueOf).collect(joining(",")));
        }
        if (CollectionUtils.isNotEmpty(buckets)) {
            job.setParam(NBatchConstants.P_BUCKETS, ExecutableParams.toBucketParam(buckets));
        }
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_PROJECT_NAME, df.getProject());
        job.setParam(NBatchConstants.P_TARGET_MODEL, job.getTargetSubject());
        job.setParam(NBatchConstants.P_DATAFLOW_ID, df.getId());
        job.setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(layouts)));
        job.setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", job.getTargetSegments()));
        job.setParam(NBatchConstants.P_DATA_RANGE_START, mergedSegment.getSegRange().getStart().toString());
        job.setParam(NBatchConstants.P_DATA_RANGE_END, mergedSegment.getSegRange().getEnd().toString());

        KylinConfig config = df.getConfig();
        JobStepType.RESOURCE_DETECT.createStep(job, config);
        JobStepType.MERGING.createStep(job, config);
        AbstractExecutable cleanStep = JobStepType.CLEAN_UP_AFTER_MERGE.createStep(job, config);
        final Segments<NDataSegment> mergingSegments = df.getMergingSegments(mergedSegment);
        cleanStep.setParam(NBatchConstants.P_SEGMENT_IDS,
                String.join(",", NSparkCubingUtil.toSegmentIds(mergingSegments)));
        // TODO SecondStorage
//        if (SecondStorageUtil.isModelEnable(df.getProject(), job.getTargetSubject())
//                && layouts.stream().anyMatch(SecondStorageUtil::isBaseTableIndex)) {
//            // can't merge segment when second storage do rebalanced
//            SecondStorageUtil.validateProjectLock(df.getProject(), Collections.singletonList(LockTypeEnum.LOAD.name()));
//            AbstractExecutable mergeStep = JobStepType.SECOND_STORAGE_MERGE.createStep(job, config);
//            mergeStep.setParam(SecondStorageConstants.P_MERGED_SEGMENT_ID, mergedSegment.getId());
//            mergeStep.setParam(NBatchConstants.P_SEGMENT_IDS,
//                    String.join(",", NSparkCubingUtil.toSegmentIds(mergingSegments)));
//        }
        JobStepType.UPDATE_METADATA.createStep(job, config);
        return job;
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

    public NSparkMergingStep getSparkMergingStep() {
        return getTask(NSparkMergingStep.class);
    }

    public NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    public NSparkCleanupAfterMergeStep getCleanUpAfterMergeStep() {
        return getTask(NSparkCleanupAfterMergeStep.class);
    }

    @Override
    public void cancelJob() {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkMergingStep().getDataflowId());
        List<NDataSegment> toRemovedSegments = new ArrayList<>();
        for (String id : getSparkMergingStep().getSegmentIds()) {
            NDataSegment segment = dataflow.getSegment(id);
            if (segment != null && SegmentStatusEnum.READY != segment.getStatus()
                    && SegmentStatusEnum.WARNING != segment.getStatus()) {
                toRemovedSegments.add(segment);
            }
        }
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(toRemovedSegments.toArray(new NDataSegment[0]));
        nDataflowManager.updateDataflow(nDataflowUpdate);
    }
}
