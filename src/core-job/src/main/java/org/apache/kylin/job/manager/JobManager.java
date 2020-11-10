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

package org.apache.kylin.job.manager;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;

import java.util.HashSet;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.AddIndexHandler;
import org.apache.kylin.job.handler.AddSegmentHandler;
import org.apache.kylin.job.handler.MergeSegmentHandler;
import org.apache.kylin.job.handler.RefreshSegmentHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.SegmentUtils;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class JobManager {

    private KylinConfig config;

    private String project;

    public static JobManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, JobManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static JobManager newInstance(KylinConfig conf, String project) {
        checkNotNull(project);
        return new JobManager(conf, project);
    }

    public String addSnapshotJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.SNAPSHOT_BUILD);
        return addJob(jobParam);
    }

    public String addSegmentJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.INC_BUILD);
        return addJob(jobParam);
    }

    /**
     * Recommend to use checkAndAddCuboidJob
     */
    public String addFullIndexJob(JobParam jobParam) {
        return checkAndAddIndexJob(jobParam);
    }

    /**
     * This is recommended because one cubboid cannot be built at the same time
     */
    public String checkAndAddIndexJob(JobParam jobParam) {
        val relatedSegments = getValidSegments(jobParam.getModel(), project).stream().map(NDataSegment::getId)
                .collect(Collectors.toList());
        jobParam.setTargetSegments(new HashSet<>(relatedSegments));
        return addRelatedIndexJob(jobParam);
    }

    public String addRelatedIndexJob(JobParam jobParam) {
        boolean noNeed = (jobParam.getTargetSegments() == null
                && getValidSegments(jobParam.getModel(), project).isEmpty())
                || (jobParam.getTargetSegments() != null && jobParam.getTargetSegments().isEmpty());
        if (noNeed) {
            log.debug("No need to add index build job due to there is no valid segment in {}.", jobParam.getModel());
            return null;
        }
        jobParam.setJobTypeEnum(JobTypeEnum.INDEX_BUILD);
        return addJob(jobParam);
    }

    public String mergeSegmentJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.INDEX_MERGE);
        return addJob(jobParam);
    }

    public String refreshSegmentJob(JobParam jobParam) {
        return refreshSegmentJob(jobParam, false);
    }

    public String refreshSegmentJob(JobParam jobParam, boolean refreshAllLayouts) {
        jobParam.getCondition().put(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS, refreshAllLayouts);
        jobParam.setJobTypeEnum(JobTypeEnum.INDEX_REFRESH);
        return addJob(jobParam);
    }

    public String addJob(JobParam jobParam) {
        if (!config.isJobNode() && !config.isUTEnv()) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getADD_JOB_ABANDON());
        }
        checkNotNull(project);
        jobParam.setProject(project);
        ExecutableUtil.computLayouts(jobParam);
        AbstractJobHandler handler;
        switch (jobParam.getJobTypeEnum()) {
        case INC_BUILD:
            handler = new AddSegmentHandler();
            break;
        case INDEX_MERGE:
            handler = new MergeSegmentHandler();
            break;
        case INDEX_BUILD:
            handler = new AddIndexHandler();
            break;
        case INDEX_REFRESH:
            handler = new RefreshSegmentHandler();
            break;
        default:
            log.error("jobParam doesn't have matched job: {}", jobParam.getJobTypeEnum());
            return null;
        }
        handler.handle(jobParam);
        return jobParam.getJobId();
    }

    public JobManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    /**
     * Valid segment：
     * 1. SegmentStatusEnum is READY.
     * 2. Time doesn't overlap with running segments.
     */
    public static Segments<NDataSegment> getValidSegments(String modelId, String project) {
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        val executables = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listExecByModelAndStatus(modelId, ExecutableState::isRunning, null);
        val runningSegs = new Segments<NDataSegment>();
        executables.stream().filter(e -> e.getTargetSegments() != null).flatMap(e -> e.getTargetSegments().stream())
                .distinct().filter(segId -> df.getSegment(segId) != null)
                .forEach(segId -> runningSegs.add(df.getSegment(segId)));
        return SegmentUtils.filterSegmentsByTime(df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING),
                runningSegs);
    }

}
