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
package io.kyligence.kap.job.handler;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_FAIL;
import static org.apache.kylin.job.execution.AbstractExecutable.DEPENDENT_FILES;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.model.JobParam;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public abstract class AbstractJobHandler {

    public final void handle(JobParam jobParam) {

        checkBeforeHandle(jobParam);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            checkBeforeHandle(jobParam);
            doHandle(jobParam);
            return null;
        }, jobParam.getProject(), 1);
    }

    protected boolean needComputeJobBucket() {
        return true;
    }

    protected final void doHandle(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (needComputeJobBucket()) {
            ExecutableUtil.computeJobBucket(jobParam);
        }
        AbstractExecutable job = createJob(jobParam);
        if (job == null) {
            log.info("Job {} no need to create job ", jobParam);
            jobParam.setJobId(null);
            return;
        }
        job.setSparkYarnQueueIfEnabled(jobParam.getProject(), jobParam.getYarnQueue());
        job.setPriority(jobParam.getPriority());
        job.setTag(jobParam.getTag());
        log.info("Job {} creates job {}", jobParam, job);
        String project = jobParam.getProject();
        val po = NExecutableManager.toPO(job, project);
        NExecutableManager executableManager = getExecutableManager(project, kylinConfig);
        executableManager.addJob(po);

        if (job instanceof ChainedExecutable) {
            val deps = ((ChainedExecutable) job).getTasks().stream()
                    .flatMap(j -> j.getDependencies(kylinConfig).stream()).collect(Collectors.toSet());
            Map<String, String> info = Maps.newHashMap();
            info.put(DEPENDENT_FILES, StringUtils.join(deps, ","));
            executableManager.updateJobOutput(po.getId(), null, info, null, null);
            JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig, project);
            long startOfDay = TimeUtil.getDayStart(System.currentTimeMillis());
            jobStatisticsManager.updateStatistics(startOfDay, jobParam.getModel(), 0, 0, 1);
        }
    }

    protected abstract AbstractExecutable createJob(JobParam jobParam);

    protected void checkBeforeHandle(JobParam jobParam) {
        String model = jobParam.getModel();
        String project = jobParam.getProject();
        checkNotNull(project);
        checkNotNull(model);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val dataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(model);
        val execManager = NExecutableManager.getInstance(kylinConfig, project);
        List<AbstractExecutable> executables;
        if (jobParam.isMultiPartitionJob()) {
            executables = execManager.listMultiPartitionModelExec(model, ExecutableState::isRunning,
                    jobParam.getJobTypeEnum(), jobParam.getTargetPartitions(), null);
        } else {
            executables = execManager.listExecByModelAndStatus(model, ExecutableState::isRunning, null);
        }

        List<String> failedSegs = new LinkedList<>();
        if (JobParam.isBuildIndexJob(jobParam.getJobTypeEnum())) {
            for (String segmentId : jobParam.getTargetSegments()) {
                if (isOverlapWithJob(executables, segmentId, jobParam, dataflow)) {
                    failedSegs.add(segmentId);
                }
            }
        } else {
            if (isOverlapWithJob(executables, jobParam.getSegment(), jobParam, dataflow)) {
                failedSegs.add(jobParam.getSegment());
            }
        }

        if (failedSegs.isEmpty()) {
            return;
        }

        JobSubmissionException jobSubmissionException = new JobSubmissionException(JOB_CREATE_CHECK_FAIL);
        for (String failedSeg : failedSegs) {
            jobSubmissionException.addJobFailInfo(failedSeg, new KylinException(JOB_CREATE_CHECK_FAIL));
        }
        throw jobSubmissionException;
    }

    public boolean isOverlapWithJob(List<AbstractExecutable> executables, String segmentId, JobParam jobParam,
            NDataflow dataflow) {
        val dealSegment = dataflow.getSegment(segmentId);
        HashMap<String, NDataSegment> relatedSegment = new HashMap<>();
        dataflow.getSegments().forEach(segment -> relatedSegment.put(segment.getId(), segment));

        for (AbstractExecutable job : executables) {
            val targetSegments = job.getTargetSegments();
            for (String segId : targetSegments) {
                if (relatedSegment.get(segId) != null
                        && dealSegment.getSegRange().overlaps(relatedSegment.get(segId).getSegRange())) {
                    log.debug("JobParam {} segment range  conflicts with running job {}", jobParam, job);
                    return true;
                }
            }
        }
        return false;
    }

    protected NExecutableManager getExecutableManager(String project, KylinConfig config) {
        return NExecutableManager.getInstance(config, project);
    }
}
