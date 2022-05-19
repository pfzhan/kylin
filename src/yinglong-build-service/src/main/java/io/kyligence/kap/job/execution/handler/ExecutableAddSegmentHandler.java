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

package io.kyligence.kap.job.execution.handler;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.job.execution.DefaultChainedExecutableOnModel;
import io.kyligence.kap.job.execution.NSparkExecutable;
import io.kyligence.kap.job.execution.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.val;

public class ExecutableAddSegmentHandler extends ExecutableHandler {

    public ExecutableAddSegmentHandler(String project, String modelId, String owner, String segmentId, String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        String project = getProject();
        val executable = getExecutable();
        val jobId = executable.getId();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterBuildResourceMerger(kylinConfig, project);
        executable.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        markDFStatus(dfMgr);
    }

    @Override
    public void handleDiscardOrSuicidal() {
        if (((DefaultChainedExecutableOnModel) getExecutable()).checkAnyLayoutExists()) {
            return;
        }
        makeSegmentReady();
    }

    private void makeSegmentReady() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val segmentId = getSegmentId();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, getProject());
        NDataflow df = dfMgr.getDataflow(getModelId());

        //update target seg's status
        val dfUpdate = new NDataflowUpdate(getModelId());
        val seg = df.copy().getSegment(segmentId);
        seg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(seg);
        dfMgr.updateDataflow(dfUpdate);
        markDFStatus(dfMgr);
    }

    private void markDFStatus(NDataflowManager dfManager) {
        super.markDFStatus();
        val df = dfManager.getDataflow(getModelId());
        RealizationStatusEnum status = df.getStatus();
        if (RealizationStatusEnum.LAG_BEHIND == status) {
            val model = df.getModel();
            Preconditions.checkState(ManagementType.TABLE_ORIENTED == model.getManagementType());
            if (checkOnline(model) && !df.getIndexPlan().isOfflineManually()) {
                dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);
            }
        }
    }

    private boolean checkOnline(NDataModel model) {
        // 1. check the job status of the model
        val executableManager = getExecutableManager(model.getProject(), KylinConfig.getInstanceFromEnv());
        val count = executableManager
                .listExecByModelAndStatus(model.getId(), ExecutableState::isNotProgressing, JobTypeEnum.INC_BUILD)
                .size();
        if (count > 0) {
            return false;
        }
        // 2. check the model aligned with data loading range
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val df = dfManager.getDataflow(model.getId());
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(model.getRootFactTableName());
        // In theory, dataLoadingRange can not be null, because full load table related model will build with INDEX_BUILD job or INDEX_REFRESH job.
        Preconditions.checkState(dataLoadingRange != null);
        val querableSegmentRange = dataLoadingRangeManager.getQuerableSegmentRange(dataLoadingRange);
        Preconditions.checkState(querableSegmentRange != null);
        val segments = SegmentUtil
                .getSegmentsExcludeRefreshingAndMerging(df.getSegments().getSegmentsByRange(querableSegmentRange));
        for (NDataSegment segment : segments) {
            if (SegmentStatusEnum.NEW == segment.getStatus()) {
                return false;
            }
        }
        return true;
    }

}
