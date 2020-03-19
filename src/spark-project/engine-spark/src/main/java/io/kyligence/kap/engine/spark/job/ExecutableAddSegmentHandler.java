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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.google.common.base.Preconditions;

import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.engine.spark.merger.AfterBuildResourceMerger;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NSegmentConfigHelper;
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
        if (!checkSubjectExists(project, getModelId(), getSegmentId())) {
            rollFQBackToInitialStatus(SUBJECT_NOT_EXIST_COMMENT);
            return;
        }
        val buildTask = executable.getTask(NSparkCubingStep.class);
        val dataflowId = ExecutableUtils.getDataflowId(buildTask);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterBuildResourceMerger(kylinConfig, project);
        executable.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        markDFOnlineIfNecessary(dfMgr.getDataflow(dataflowId));
        handleRetention(project, getModelId());
        //TODO: take care of this
        autoMergeSegments(project, getModelId(), getOwner());
        handleFavoriteQuery();
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

        markDFOnlineIfNecessary(dfMgr.getDataflow(df.getId()));

        //TODO: take care of this
        handleRetention(getProject(), getModelId());
        autoMergeSegments(getProject(), getModelId(), getOwner());
    }

    private void autoMergeSegments(String project, String modelId, String owner) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        Segments segments = df.getSegments();
        SegmentRange rangeToMerge = null;
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        Preconditions.checkState(segmentConfig != null);
        rangeToMerge = segments.autoMergeSegments(segmentConfig);
        if (rangeToMerge == null) {
            return;
        } else {
            NDataSegment mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .mergeSegments(df, rangeToMerge, true);
            postEvent(MERGE_SEGMENT_EVENT_CLASS, mergeSeg.getId());
        }

    }

    private void markDFOnlineIfNecessary(NDataflow dataflow) {
        if (!RealizationStatusEnum.LAG_BEHIND.equals(dataflow.getStatus())) {
            return;
        }
        val model = dataflow.getModel();
        Preconditions.checkState(ManagementType.TABLE_ORIENTED.equals(model.getManagementType()));

        if (checkOnline(model)) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            dfManager.updateDataflow(dataflow.getId(),
                    copyForWrite -> copyForWrite.setStatus(RealizationStatusEnum.ONLINE));
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
        val segments = df.getSegments().getSegmentsByRange(querableSegmentRange)
                .getSegmentsExcludeRefreshingAndMerging();
        for (NDataSegment segment : segments) {
            if (SegmentStatusEnum.NEW.equals(segment.getStatus())) {
                return false;
            }
        }
        return true;
    }

    private void handleRetention(String project, String modelId) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        dfManager.handleRetention(df);
    }
}
