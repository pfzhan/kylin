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

package io.kyligence.kap.rest.response;

import java.util.ArrayList;
import java.util.List;

import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.FusionModelManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.util.ModelUtils;

@Setter
@Getter
public class FusionModelResponse extends NDataModelResponse {

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("streaming_indexes")
    private long streamingIndexes;

    @EqualsAndHashCode.Include
    @JsonProperty("batch_partition_desc")
    private PartitionDesc batchPartitionDesc;

    @JsonProperty("batch_segments")
    private List<NDataSegmentResponse> batchSegments = new ArrayList<>();

    @JsonProperty("batch_segment_holes")
    private List<SegmentRange> batchSegmentHoles;

    public FusionModelResponse(NDataModel dataModel) {
        super(dataModel);
    }

    @Override
    protected void computedDisplayInfo(NDataModel modelDesc) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        NDataflow streamingDataflow = dfManager.getDataflow(modelDesc.getUuid());
        FusionModel fusionModel = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getFusionModel(modelDesc.getFusionId());
        val batchModel = fusionModel.getBatchModel();
        if (batchModel.isBroken() || modelDesc.isBroken()) {
            this.setStatus(ModelStatusToDisplayEnum.BROKEN);
        }
        this.setBatchId(batchModel.getUuid());
        this.setFusionId(modelDesc.getFusionId());
        this.setBatchId(fusionModel.getBatchModel().getUuid());
        NDataflow batchDataflow = dfManager.getDataflow(batchId);
        this.setLastBuildTime(getMaxLastBuildTime(batchDataflow, streamingDataflow));
        this.setStorage(getTotalStorage(batchDataflow, streamingDataflow));
        this.setSource(getTotalSource(batchDataflow, streamingDataflow));
        this.setBatchSegmentHoles(calculateTotalSegHoles(batchDataflow));
        this.setSegmentHoles(calculateTotalSegHoles(streamingDataflow));
        this.setExpansionrate(ModelUtils.computeExpansionRate(this.getStorage(), this.getSource()));
        this.setUsage(getTotalUsage(streamingDataflow));
        this.setInconsistentSegmentCount(getTotalInconsistentSegmentCount(batchDataflow, streamingDataflow));
        if (!modelDesc.isBroken() && !batchModel.isBroken()) {
            this.setHasSegments(CollectionUtils.isNotEmpty(streamingDataflow.getSegments())
                    || CollectionUtils.isNotEmpty(batchDataflow.getSegments()));
            this.setBatchPartitionDesc(fusionModel.getBatchModel().getPartitionDesc());
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    this.getProject());
            IndexPlan batchIndex = indexPlanManager.getIndexPlan(batchId);
            IndexPlan streamingIndex = indexPlanManager.getIndexPlan(modelDesc.getUuid());
            this.setAvailableIndexesCount(getTotalAvailableIndexesCount(batchIndex, streamingIndex));
            this.setTotalIndexes(getIndexesCount(batchIndex));
            this.setStreamingIndexes(getIndexesCount(streamingIndex));
            this.setEmptyIndexesCount(this.getTotalIndexes() - this.getAvailableIndexesCount());
            this.setHasBaseAggIndex(streamingIndex.containBaseAggLayout());
            this.setHasBaseTableIndex(streamingIndex.containBaseTableLayout());
        }
    }

    private long getMaxLastBuildTime(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return Math.max(batchDataflow.getLastBuildTime(), streamingDataflow.getLastBuildTime());
    }

    private long getTotalStorage(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return batchDataflow.getStorageBytesSize() + streamingDataflow.getStorageBytesSize();
    }

    private long getTotalSource(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return batchDataflow.getSourceBytesSize() + streamingDataflow.getSourceBytesSize();
    }

    private List<SegmentRange> calculateTotalSegHoles(NDataflow batchDataflow) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), this.getProject());
        return dfManager.calculateSegHoles(batchDataflow.getUuid());
    }

    private long getTotalUsage(NDataflow streamingDataflow) {
        return streamingDataflow.getQueryHitCount();
    }

    private long getTotalInconsistentSegmentCount(NDataflow batchDataflow, NDataflow streamingDataflow) {
        return (long) batchDataflow.getSegments(SegmentStatusEnum.WARNING).size()
                + (long) streamingDataflow.getSegments(SegmentStatusEnum.WARNING).size();
    }

    private long getTotalAvailableIndexesCount(IndexPlan batchIndex, IndexPlan streamingIndex) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                this.getProject());
        return indexPlanManager.getAvailableIndexesCount(getProject(), batchIndex.getId())
                + indexPlanManager.getAvailableIndexesCount(getProject(), streamingIndex.getId());
    }

    private long getIndexesCount(IndexPlan indexPlan) {
        return indexPlan.getAllLayouts().size();
    }
}
