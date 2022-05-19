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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.handler.ExecutableHandler;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.cube.model.SegmentPartition;
import io.kyligence.kap.metadata.model.ManagementType;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

public class DefaultChainedExecutableOnModel extends DefaultChainedExecutable {

    @Getter
    @Setter
    private ExecutableHandler handler;

    public DefaultChainedExecutableOnModel() {
        super();
    }

    public DefaultChainedExecutableOnModel(Object notSetId) {
        super(notSetId);
    }

    private String getTargetModel() {
        return getTargetSubject();
    }

    @Override
    public void onExecuteErrorHook(String jobId) {
        markDFLagBehindIfNecessary(jobId);
    }

    private void markDFLagBehindIfNecessary(String jobId) {
        if (JobTypeEnum.INC_BUILD != this.getJobType()) {
            return;
        }
        val dataflow = getDataflow(jobId);
        if (dataflow == null || RealizationStatusEnum.LAG_BEHIND == dataflow.getStatus()) {
            return;
        }
        val model = dataflow.getModel();
        if (ManagementType.MODEL_BASED == model.getManagementType()) {
            return;
        }

        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        dfManager.updateDataflowStatus(dataflow.getId(), RealizationStatusEnum.LAG_BEHIND);
    }

    private NDataflow getDataflow(String jobId) {
        val execManager = getExecutableManager(getProject());
        val executable = (DefaultChainedExecutableOnModel) execManager.getJob(jobId);
        val modelId = executable.getTargetModel();
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        return dfManager.getDataflow(modelId);
    }

    @Override
    public boolean checkSuicide() {
        try {
            return !checkAnyTargetSegmentAndPartitionExists() || !checkAnyLayoutExists();
        } catch (Exception e) {
            return true;
        }
    }

    public boolean checkAnyLayoutExists() {
        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isEmpty(layouts)) {
            return true;
        }
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        val indexPlan = indexPlanManager.getIndexPlan(getTargetModel());
        val allLayoutIds = indexPlan.getAllLayouts().stream().map(l -> l.getId() + "").collect(Collectors.toSet());
        return Stream.of(StringUtil.splitAndTrim(layouts, ",")).anyMatch(allLayoutIds::contains);
    }

    private boolean checkTargetSegmentAndPartitionExists(String segmentId) {
        NDataflow dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(getTargetModel());
        if (dataflow == null || dataflow.checkBrokenWithRelatedInfo()) {
            return false;
        }
        NDataSegment segment = dataflow.getSegment(segmentId);
        // segment is deleted or model multi partition
        if (segment == null) {
            return false;
        }
        if (dataflow.getModel().isMultiPartitionModel()) {
            Set<Long> partitionIds = segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                    .collect(Collectors.toSet());
            Set<Long> partitionInSegment = getPartitionsBySegment().get(segmentId);
            if (partitionInSegment == null) {
                logger.warn("Segment {} doesn't contain any partition in this job", segmentId);
                return true;
            }
            for (long partition : partitionInSegment) {
                if (!partitionIds.contains(partition)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkAnyTargetSegmentAndPartitionExists() {
        List<String> topJobTargetSegments = getTargetSegments();
        AbstractExecutable parent = getParent();
        if (parent != null) {
            topJobTargetSegments = parent.getTargetSegments();
        }

        Preconditions.checkState(!topJobTargetSegments.isEmpty());
        return topJobTargetSegments.stream().anyMatch(this::checkTargetSegmentAndPartitionExists);
    }

    public boolean checkCuttingInJobByModel() {
        AbstractExecutable parent = getParent();
        if (parent == null) {
            parent = this;
        }
        if (!JobParam.isBuildIndexJob(parent.getJobType())) {
            return false;
        }
        val model = ((DefaultChainedExecutableOnModel) parent).getTargetModel();
        return ExecutableManager.getInstance(getConfig(), getProject()).countCuttingInJobByModel(model, parent) > 0;
    }

    @Override
    public void onExecuteDiscardHook(String jobId) {
        if (handler != null) {
            handler.handleDiscardOrSuicidal();
        }
    }

    @Override
    protected void onExecuteSuicidalHook(String jobId) {
        if (handler != null) {
            handler.handleDiscardOrSuicidal();
        }
    }
}
