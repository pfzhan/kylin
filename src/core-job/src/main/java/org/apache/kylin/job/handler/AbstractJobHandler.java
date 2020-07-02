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
import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_ADD_JOB_CHECK;
import static org.apache.kylin.job.execution.AbstractExecutable.DEPENDENT_FILES;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.model.JobParam;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 *
 **/
@Slf4j
public abstract class AbstractJobHandler {

    public final void handle(JobParam jobParam) {

        if (!checkBeforeHandle(jobParam)) {
            throw new KylinException(FAILED_ADD_JOB_CHECK, MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if (!checkBeforeHandle(jobParam)) {
                throw new KylinException(FAILED_ADD_JOB_CHECK, MsgPicker.getMsg().getADD_JOB_CHECK_FAIL());
            }
            doHandle(jobParam);
            return null;
        }, jobParam.getProject(), 1);
    }

    protected final void doHandle(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        AbstractExecutable job = createJob(jobParam);
        if (job == null) {
            log.info("Event {} no need to create job ", jobParam);
            jobParam.setJobId(null);
            return;
        }
        log.info("Event {} creates job {}", jobParam, job);
        val po = NExecutableManager.toPO(job, jobParam.getProject());

        NExecutableManager executableManager = getExecutableManager(jobParam.getProject(), kylinConfig);
        executableManager.addJob(po);

        if (job instanceof ChainedExecutable) {
            val deps = ((ChainedExecutable) job).getTasks().stream()
                    .flatMap(j -> j.getDependencies(kylinConfig).stream()).collect(Collectors.toSet());
            Map<String, String> info = Maps.newHashMap();
            info.put(DEPENDENT_FILES, StringUtils.join(deps, ","));
            executableManager.updateJobOutput(po.getId(), null, info, null, null);
        }
    }

    protected abstract AbstractExecutable createJob(JobParam jobParam);

    protected boolean checkBeforeHandle(JobParam jobParam) {
        String model = jobParam.getModel();
        String project = jobParam.getProject();
        checkNotNull(project);
        checkNotNull(model);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val dataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(model);
        val execManager = NExecutableManager.getInstance(kylinConfig, project);
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(model, ExecutableState::isRunning, null);

        if (jobParam.getJobTypeEnum().equals(JobTypeEnum.INDEX_BUILD)) {
            for (String segmentId : jobParam.getTargetSegments()) {
                if (isOverlapWithJob(executables, segmentId, jobParam, dataflow)) {
                    return false;
                }
            }
            return true;
        }
        return !isOverlapWithJob(executables, jobParam.getSegment(), jobParam, dataflow);
    }

    public boolean isOverlapWithJob(List<AbstractExecutable> executables, String segmentId, JobParam jobParam, NDataflow dataflow) {
        val dealSegment = dataflow.getSegment(segmentId);
        HashMap<String, NDataSegment> relatedSegment = new HashMap<>();
        dataflow.getSegments().forEach(segment -> relatedSegment.put(segment.getId(), segment));

        for (AbstractExecutable job : executables) {
            val targetSegments = job.getTargetSegments();
            for (String segId : targetSegments) {
                if (relatedSegment.get(segId) != null &&
                        dealSegment.getSegRange().overlaps(relatedSegment.get(segId).getSegRange())) {
                    log.trace("JobParam {} segment range  conflicts with running job {}", jobParam, job);
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
