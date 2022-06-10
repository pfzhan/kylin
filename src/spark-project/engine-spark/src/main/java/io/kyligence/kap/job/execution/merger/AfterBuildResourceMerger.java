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

package io.kyligence.kap.job.execution.merger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.util.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegDetails;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.rest.delegate.ModelMetadataBaseInvoker;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AfterBuildResourceMerger extends SparkJobMetadataMerger {

    public AfterBuildResourceMerger(KylinConfig config, String project) {
        super(config, project);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentId, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType, Set<Long> partitions) {
        switch (jobType) {
        case INDEX_BUILD:
        case SUB_PARTITION_BUILD:
            return mergeAfterCatchup(dataflowId, segmentId, layoutIds, remoteResourceStore, partitions);
        case INC_BUILD:
            Preconditions.checkArgument(segmentId.size() == 1);
            return mergeAfterIncrement(dataflowId, segmentId.iterator().next(), layoutIds, remoteResourceStore);
        default:
            throw new UnsupportedOperationException("Error job type: " + jobType);
        }
    }

    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), abstractExecutable)) {
            val dataFlowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val segmentIds = ExecutableUtils.getSegmentIds(abstractExecutable);
            val layoutIds = ExecutableUtils.getLayoutIds(abstractExecutable);
            val partitionIds = ExecutableUtils.getPartitionIds(abstractExecutable);
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore,
                    abstractExecutable.getJobType(), partitionIds);
            NDataflow dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(dataFlowId);
            if (ExecutableUtils.needBuildSnapshots(abstractExecutable)) {
                mergeSnapshotMeta(dataflow, buildResourceStore);
            }
            mergeTableExtMeta(dataflow, buildResourceStore);
            recordDownJobStats(abstractExecutable, nDataLayouts);
            abstractExecutable.notifyUserIfNecessary(nDataLayouts);
        }
    }

    public NDataLayout[] mergeAfterIncrement(String flowName, String segmentId, Set<Long> layoutIds,
            ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dfUpdate = new NDataflowUpdate(flowName);
        val theSeg = remoteDataflow.getSegment(segmentId);

        if (theSeg.getModel().isMultiPartitionModel()) {
            final long lastBuildTime = System.currentTimeMillis();
            theSeg.getMultiPartitions().forEach(partition -> {
                partition.setStatus(PartitionStatusEnum.READY);
                partition.setLastBuildTime(lastBuildTime);
            });
            theSeg.setLastBuildTime(lastBuildTime);
        } else {
            theSeg.setLastBuildTime(theSeg.getSegDetails().getLastModified());
        }

        resetBreakpoints(theSeg);

        theSeg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(theSeg);
        dfUpdate.setToAddOrUpdateLayouts(theSeg.getSegDetails().getLayouts().toArray(new NDataLayout[0]));

        List<NDataSegDetails> segDetails = Collections.singletonList(theSeg.getSegDetails());
        List<Integer> layoutCounts = Collections.singletonList(dfUpdate.getToAddOrUpdateLayouts().length);

        ModelMetadataBaseInvoker.getInstance().updateDataflow(new DataFlowUpdateRequest(getProject(), dfUpdate,
                segDetails.toArray(new NDataSegDetails[0]), layoutCounts.toArray(new Integer[0])));
        updateIndexPlan(flowName, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    public NDataLayout[] mergeAfterCatchup(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteStore, Set<Long> partitionIds) {
        if (CollectionUtils.isNotEmpty(partitionIds)) {
            return mergeMultiPartitionModelAfterCatchUp(flowName, segmentIds, layoutIds, remoteStore, partitionIds);
        } else {
            return mergeNormalModelAfterCatchUp(flowName, segmentIds, layoutIds, remoteStore);
        }
    }

    public NDataLayout[] mergeNormalModelAfterCatchUp(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val dataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();

        val dfUpdate = new NDataflowUpdate(flowName);
        val addCuboids = Lists.<NDataLayout> newArrayList();
        val availableLayoutIds = getAvailableLayoutIds(dataflow, layoutIds);

        List<NDataSegment> segsToUpdate = Lists.newArrayList();
        List<NDataSegDetails> segDetails = Lists.newArrayList();
        List<Integer> layoutCounts = Lists.newArrayList();
        for (String segId : segmentIds) {
            val localSeg = dataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId);
            // ignore if local segment is not ready
            if (isUnavailableSegment(localSeg)) {
                continue;
            }
            remoteSeg.setLastBuildTime(remoteSeg.getSegDetails().getLastModified());
            for (long layoutId : availableLayoutIds) {
                NDataLayout dataCuboid = remoteSeg.getLayout(layoutId);
                Preconditions.checkNotNull(dataCuboid);
                addCuboids.add(dataCuboid);
            }

            resetBreakpoints(remoteSeg);

            segsToUpdate.add(remoteSeg);

            segDetails.add(remoteSeg.getSegDetails());
            layoutCounts.add(availableLayoutIds.size());
        }

        dfUpdate.setToUpdateSegs(segsToUpdate.toArray(new NDataSegment[0]));
        dfUpdate.setToAddOrUpdateLayouts(addCuboids.toArray(new NDataLayout[0]));

        ModelMetadataBaseInvoker.getInstance().updateDataflow(new DataFlowUpdateRequest(getProject(), dfUpdate,
                segDetails.toArray(new NDataSegDetails[0]), layoutCounts.toArray(new Integer[0])));
        updateIndexPlan(flowName, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    private boolean isUnavailableSegment(NDataSegment localSeg) {
        if (localSeg == null) {
            return true;
        }
        return localSeg.getStatus() != SegmentStatusEnum.READY && localSeg.getStatus() != SegmentStatusEnum.WARNING;
    }

    /**
     * MultiPartition model job:
     * New layoutIds mean index build job which should add new layouts.
     * Old layoutIds mean partition build job which should update partitions in layouts.
     */
    public NDataLayout[] mergeMultiPartitionModelAfterCatchUp(String flowName, Set<String> segmentIds,
            Set<Long> layoutIds, ResourceStore remoteStore, Set<Long> partitionIds) {

        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName).copy();
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName).copy();
        val dataflow = localDataflowManager.getDataflow(flowName);
        val dfUpdate = new NDataflowUpdate(flowName);
        val upsertCuboids = Lists.<NDataLayout> newArrayList();
        val availableLayoutIds = getAvailableLayoutIds(dataflow, layoutIds);
        List<NDataSegment> segsToUpdate = Lists.newArrayList();
        List<NDataSegDetails> segDetails = Lists.newArrayList();
        List<Integer> layoutCounts = Lists.newArrayList();

        for (String segId : segmentIds) {
            val localSeg = localDataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId);
            // ignore if local segment is not ready
            if (isUnavailableSegment(localSeg)) {
                continue;
            }
            NDataSegment updateSegment = upsertSegmentPartition(localSeg, remoteSeg, partitionIds);

            for (long layoutId : availableLayoutIds) {
                NDataLayout remoteLayout = remoteSeg.getLayout(layoutId);
                NDataLayout localLayout = localSeg.getLayout(layoutId);
                NDataLayout upsertLayout = upsertLayoutPartition(localLayout, remoteLayout, partitionIds);
                if (upsertLayout == null) {
                    log.warn("Layout {} is null in segment {}. Segment have layouts {} ", layoutId, segId,
                            remoteSeg.getLayoutIds());
                }
                upsertCuboids.add(upsertLayout);
            }
            segsToUpdate.add(updateSegment);

            segDetails.add(remoteSeg.getSegDetails());
            layoutCounts.add(availableLayoutIds.size());
        }
        dfUpdate.setToUpdateSegs(segsToUpdate.toArray(new NDataSegment[0]));
        dfUpdate.setToAddOrUpdateLayouts(upsertCuboids.toArray(new NDataLayout[0]));

        ModelMetadataBaseInvoker.getInstance().updateDataflow(new DataFlowUpdateRequest(getProject(), dfUpdate,
                segDetails.toArray(new NDataSegDetails[0]), layoutCounts.toArray(new Integer[0])));
        updateIndexPlan(flowName, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    private void resetBreakpoints(NDataSegment dataSegment) {
        // Reset breakpoints.
        dataSegment.setFactViewReady(false);
        dataSegment.setDictReady(false);
        if (!getConfig().isPersistFlatTableEnabled()) {
            dataSegment.setFlatTableReady(false);
        }

        // Multi level partition FLAT-TABLE is not reusable.
        if (Objects.nonNull(dataSegment.getModel()) //
                && Objects.nonNull(dataSegment.getModel().getMultiPartitionDesc())) {
            dataSegment.setFlatTableReady(false);
            // By design, multi level partition shouldn't refresh snapshots frequently.
        } else {
            dataSegment.setSnapshotReady(false);
        }
    }
}
