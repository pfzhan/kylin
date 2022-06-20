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

package org.apache.kylin.job.handler;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.ServerErrorCode.BASE_TABLE_INDEX_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_ADD_JOB_FAILED;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_JOB_FACTORY;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.factory.JobFactory;
import io.kyligence.kap.job.handler.AbstractJobHandler;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;

public class SecondStorageSegmentLoadJobHandler extends AbstractJobHandler {

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        List<String> hasBaseIndexSegmentIds = jobParam.getTargetSegments().stream().map(dataflow::getSegment)
                .filter(segment -> segment.getLayoutsMap().values()
                        .stream().map(NDataLayout::getLayout).anyMatch(SecondStorageUtil::isBaseTableIndex))
                .map(NDataSegment::getId)
                .collect(Collectors.toList());

        if (hasBaseIndexSegmentIds.isEmpty()) {
            throw new KylinException(BASE_TABLE_INDEX_NOT_AVAILABLE,
                    MsgPicker.getMsg().getSecondStorageSegmentWithoutBaseIndex());
        }

        return JobFactory.createJob(STORAGE_JOB_FACTORY,
                new JobFactory.JobBuildParams(
                        jobParam.getTargetSegments().stream().map(dataflow::getSegment).collect(Collectors.toSet()),
                        jobParam.getProcessLayouts(),
                        jobParam.getOwner(),
                        JobTypeEnum.EXPORT_TO_SECOND_STORAGE,
                        jobParam.getJobId(),
                        null,
                        jobParam.getIgnoredSnapshotTables(),
                        null,
                        null));
    }

    @Override
    protected boolean needComputeJobBucket() {
        return false;
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        String model = jobParam.getModel();
        String project = jobParam.getProject();
        checkNotNull(project);
        checkNotNull(model);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExecutableManager execManager = ExecutableManager.getInstance(kylinConfig, project);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(model, ExecutableState::isRunning, JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        NDataflow dataflow = dataflowManager.getDataflow(jobParam.getModel());

        Set<String> targetSegs = new HashSet<>(jobParam.getTargetSegments());
        List<String> failedSegs = executables.stream().flatMap(executable -> executable.getTargetSegments().stream())
                .filter(targetSegs::contains).collect(Collectors.toList());

        // segment doesn't have base table index
        List<String> noBaseIndexSegs = targetSegs.stream().filter(seg -> {
            NDataSegment segment = dataflow.getSegment(seg);
            return segment.getIndexPlan().getAllLayouts().stream().anyMatch(SecondStorageUtil::isBaseTableIndex);
        }).collect(Collectors.toList());
        if (failedSegs.isEmpty()) {
            return;
        }
        JobSubmissionException jobSubmissionException = new JobSubmissionException(
                MsgPicker.getMsg().getAddJobCheckFail());
        for (String failedSeg : failedSegs) {
            jobSubmissionException.addJobFailInfo(failedSeg,
                    new KylinException(SECOND_STORAGE_ADD_JOB_FAILED, MsgPicker.getMsg().getAddExportJobFail()));
        }
        noBaseIndexSegs.forEach(seg -> jobSubmissionException.addJobFailInfo(seg,
                new KylinException(FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getAddJobCheckFailWithoutBaseIndex(), seg))));
        throw jobSubmissionException;
    }
}
