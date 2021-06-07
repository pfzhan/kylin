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

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;
import org.msgpack.core.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_JOB_FACTORY;

public class SecondStorageSegmentLoadJobHandler extends AbstractJobHandler {

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        List<String> segIds = jobParam.getTargetSegments().stream().map(dataflow::getSegment)
                .filter(segment -> segment.getLayoutsMap().values()
                        .stream().map(NDataLayout::getLayout).noneMatch(index -> index.isBaseIndex() && IndexEntity.isTableIndex(index.getId())))
                .map(NDataSegment::getId)
                .collect(Collectors.toList());
        Preconditions.checkState(segIds.isEmpty(), "segments " + segIds + " don't have base index. Please build base table index firstly");
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
    protected void checkBeforeHandle(JobParam jobParam) {
        String model = jobParam.getModel();
        String project = jobParam.getProject();
        checkNotNull(project);
        checkNotNull(model);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NExecutableManager execManager = NExecutableManager.getInstance(kylinConfig, project);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(model, ExecutableState::isRunning, JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        NDataflow dataflow = dataflowManager.getDataflow(jobParam.getModel());

        Set<String> targetSegs = new HashSet<>(jobParam.getTargetSegments());
        List<String> failedSegs = executables.stream().flatMap(executable -> executable.getTargetSegments().stream())
                .filter(targetSegs::contains).collect(Collectors.toList());
        List<String> noBaseIndexSegs = targetSegs.stream().filter(seg -> {
            NDataSegment segment = dataflow.getSegment(seg);
            return segment.getIndexPlan().getAllLayouts().stream().anyMatch(SecondStorageUtil::isBaseIndex);
        }).collect(Collectors.toList());
        if (failedSegs.isEmpty()) {
            return;
        }
        JobSubmissionException jobSubmissionException = new JobSubmissionException(
                MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        for (String failedSeg : failedSegs) {
            jobSubmissionException.addJobFailInfo(failedSeg,
                    new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_CHECK_FAIL()));
        }
        noBaseIndexSegs.forEach(seg -> jobSubmissionException.addJobFailInfo(seg,
                new KylinException(FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getADD_JOB_CHECK_FAIL_WITHOUT_BASE_INDEX()), seg)));
        throw jobSubmissionException;
    }
}
