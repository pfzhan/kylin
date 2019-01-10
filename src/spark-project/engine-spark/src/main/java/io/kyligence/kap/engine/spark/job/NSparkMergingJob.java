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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;

public class NSparkMergingJob extends DefaultChainedExecutable {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(NSparkMergingJob.class);

    /**
     * Merge the segments that are contained in the given mergedSegment
     *
     * @param mergedSegment, new segment that expect to merge, which should contains a couple of ready segments.
     * @param layouts,       user is allowed to specify the cuboids to merge. By default, it is null and merge all
     *                       the ready cuboids in the segments.
     */
    public static NSparkMergingJob merge(NDataSegment mergedSegment, Set<LayoutEntity> layouts, String submitter, String jobId) {
        Preconditions.checkArgument(mergedSegment != null);
        Preconditions.checkArgument(submitter != null);

        if (layouts == null) {
            layouts = Sets.newHashSet(mergedSegment.getDataflow().getIndexPlan().getAllLayouts());
        }
        NSparkMergingJob job = new NSparkMergingJob();
        job.setName(JobTypeEnum.INDEX_MERGE.toString());
        job.setJobType(JobTypeEnum.INDEX_MERGE);
        job.setId(jobId);
        job.setDataRangeStart(Long.parseLong(mergedSegment.getSegRange().getStart().toString()));
        job.setDataRangeEnd(Long.parseLong(mergedSegment.getSegRange().getEnd().toString()));
        job.setTargetModel(mergedSegment.getModel().getUuid());
        job.setTargetSegments(Lists.newArrayList(String.valueOf(mergedSegment.getId())));
        job.setProject(mergedSegment.getProject());
        job.setSubmitter(submitter);
        job.addSparkMergingStep(mergedSegment, layouts);
        job.addCleanupStep(mergedSegment);

        return job;
    }

    public NSparkMergingStep getSparkCubingStep() {
        return (NSparkMergingStep) getTasks().get(0);
    }

    @Override
    public void cancelJob() throws IOException {
        NDataflowManager nDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflow dataflow = nDataflowManager.getDataflow(getSparkCubingStep().getDataflowId());
        List<NDataSegment> segments = new ArrayList<>();
        NDataSegment segment = dataflow.getSegment(getSparkCubingStep().getSegmentIds());
        if (segment != null && !segment.getStatus().equals(SegmentStatusEnum.READY)) {
            segments.add(segment);
        }
        NDataSegment[] segmentsArray = new NDataSegment[segments.size()];
        NDataSegment[] nDataSegments = segments.toArray(segmentsArray);
        NDataflowUpdate nDataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        nDataflowUpdate.setToRemoveSegs(nDataSegments);
        nDataflowManager.updateDataflow(nDataflowUpdate);
        NDefaultScheduler.stopThread(getId());
    }

    private void addSparkMergingStep(NDataSegment mergedSegment, Set<LayoutEntity> layouts) {
        NSparkMergingStep step = new NSparkMergingStep();
        NDataflow df = mergedSegment.getDataflow();
        KylinConfigExt config = df.getConfig();
        step.setSparkSubmitClassName(config.getSparkMergeClassName());
        step.setName(ExecutableConstants.STEP_NAME_MERGER_SPARK_SEGMENT);
        step.setTargetModel(mergedSegment.getModel().getUuid());
        step.setProject(getProject());
        step.setProjectParam();
        step.setDataflowId(df.getUuid());
        step.setSegmentId(mergedSegment.getId());
        step.setCuboidLayoutIds(NSparkCubingUtil.toCuboidLayoutIds(layouts));
        step.setJobId(getId());
        this.addTask(step);
        //after addTask, step's id is changed
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(step.getId()).toString());
    }

    private void addCleanupStep(NDataSegment mergedSegment) {
        NDataflow dataflow = mergedSegment.getDataflow();
        String name = dataflow.getUuid();
        Collection<String> ids = Collections2.transform(dataflow.getMergingSegments(mergedSegment),
                new Function<NDataSegment, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable NDataSegment input) {
                        return input.getId();
                    }
                });

        Set<String> segmentIds = new HashSet<>();
        segmentIds.addAll(ids);
        NSparkCleanupAfterMergeStep step = new NSparkCleanupAfterMergeStep();
        step.setName(ExecutableConstants.STEP_NAME_CLEANUP);
        step.setTargetModel(mergedSegment.getModel().getUuid());
        step.setProject(getProject());
        step.setDataflowId(name);
        step.setSegmentIds(segmentIds);
        this.addTask(step);
    }

}
