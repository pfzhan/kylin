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

package io.kyligence.kap.engine.spark.merger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.clearspring.analytics.util.Lists;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import lombok.val;

public class AfterMergeOrRefreshResourceMerger extends SparkJobMetadataMerger {

    public AfterMergeOrRefreshResourceMerger(KylinConfig config, String project) {
        super(config, project);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType) {

        NDataflowManager mgr = NDataflowManager.getInstance(getConfig(), getProject());
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);

        NDataflowManager distMgr = NDataflowManager.getInstance(remoteResourceStore.getConfig(), getProject());
        NDataflow distDataflow = distMgr.getDataflow(update.getDataflowId()).copy(); // avoid changing cached objects

        List<NDataSegment> toUpdateSegments = Lists.newArrayList();
        List<NDataLayout> toUpdateCuboids = Lists.newArrayList();
        NDataSegment mergedSegment = distDataflow.getSegment(segmentIds.iterator().next());

        if (mergedSegment.getStatus() == SegmentStatusEnum.NEW)
            mergedSegment.setStatus(SegmentStatusEnum.READY);

        toUpdateSegments.add(mergedSegment);
        if (JobTypeEnum.INDEX_REFRESH.equals(jobType)) {
            mergeSnapshotMeta(distDataflow, remoteResourceStore);
        }

        // only add layouts which still in segments, others maybe deleted by user
        List<NDataSegment> toRemoveSegments = distMgr.getToRemoveSegs(distDataflow, mergedSegment);
        if (JobTypeEnum.INDEX_MERGE.equals(jobType)) {
            Optional<Long> reduce = toRemoveSegments.stream().map(NDataSegment::getSourceBytesSize)
                    .filter(size -> size != -1).reduce(Long::sum);
            if (reduce.isPresent()) {
                long totalSourceSize = reduce.get();
                mergedSegment.setSourceBytesSize(totalSourceSize);
                mergedSegment.setLastBuildTime(System.currentTimeMillis());
            }

            if (toRemoveSegments.stream().anyMatch(seg -> seg.getStatus() == SegmentStatusEnum.WARNING)) {
                mergedSegment.setStatus(SegmentStatusEnum.WARNING);
            }
        }
        toUpdateCuboids.addAll(new ArrayList<>(mergedSegment.getSegDetails().getLayouts()));

        update.setToAddOrUpdateLayouts(toUpdateCuboids.toArray(new NDataLayout[0]));
        update.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[0]));
        update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));

        mgr.updateDataflow(update);

        IndexPlan remoteIndexPlan = distMgr.getDataflow(dataflowId).getIndexPlan();
        IndexPlan indexPlan = mgr.getDataflow(dataflowId).getIndexPlan();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.setLayoutBucketNumMapping(remoteIndexPlan.getLayoutBucketNumMapping());
        });
        return update.getToAddOrUpdateLayouts();
    }


    @Override
    public void merge(AbstractExecutable abstractExecutable) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), abstractExecutable)) {
            val dataFlowId = ExecutableUtils.getDataflowId(abstractExecutable);
            val segmentIds = ExecutableUtils.getSegmentIds(abstractExecutable);
            val layoutIds = ExecutableUtils.getLayoutIds(abstractExecutable);
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore,
                    abstractExecutable.getJobType());
            recordDownJobStats(abstractExecutable, nDataLayouts);
            abstractExecutable.notifyUserIfNecessary(nDataLayouts);
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (config.isUTEnv()) {
                EventBusFactory.getInstance().postAsync(new SourceUsageUpdateNotifier());
            } else {
                UnitOfWork.get()
                        .doAfterUnit(() -> EventBusFactory.getInstance().postAsync(new SourceUsageUpdateNotifier()));
            }
        }
    }

}
