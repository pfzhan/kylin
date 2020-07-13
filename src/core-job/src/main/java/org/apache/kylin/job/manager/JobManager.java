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
import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_ADD_JOB_ABANDON;

import java.util.HashSet;
import java.util.Set;
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

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.SegmentUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

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

    public String addSegmentJob(NDataSegment newSegment, String modelId, String userName) {
        return addSegmentJob(newSegment, modelId, userName, null);
    }

    public String addSegmentJob(NDataSegment newSegment, String modelId, String userName, Set<Long> targetLayouts) {
        HashSet<String> targetSegments = Sets.newHashSet();
        if (newSegment != null) {
            targetSegments.add(newSegment.getId());
        }
        return addJob(modelId, userName, targetSegments, targetLayouts, JobTypeEnum.INC_BUILD);
    }

    /**
     * Recommend to use checkAndAddCuboidJob
     */
    public String addFullIndexJob(String modelId, String userName) {
        return checkAndAddIndexJob(modelId, userName);
    }

    /**
     * This is recommended because one cubboid cannot be built at the same time
     */
    public String checkAndAddIndexJob(String modelId, String userName) {
        val relatedSegments = getValidSegments(modelId, project).stream().map(NDataSegment::getId).collect(Collectors.toList());
        return addRelatedIndexJob(modelId, userName, new HashSet<>(relatedSegments), null);
    }

    public String addRelatedIndexJob(String modelId, String userName, Set<String> relatedSegments, Set<Long> targetLayouts) {
        boolean noNeed = (relatedSegments == null && getValidSegments(modelId, project).isEmpty()) ||
                (relatedSegments != null && relatedSegments.isEmpty());
        if (noNeed) {
            log.debug("No need to add index build job due to there is no valid segment in {}.", modelId);
            return null;
        }
        return addJob(modelId, userName, relatedSegments, targetLayouts, JobTypeEnum.INDEX_BUILD);
    }

    public String mergeSegmentJob(NDataSegment newSegment, String modelId, String userName) {
        return addJob(newSegment, modelId, userName, JobTypeEnum.INDEX_MERGE);
    }

    public String refreshSegmentJob(NDataSegment newSegment, String modelId, String userName) {
        return addJob(newSegment, modelId, userName, JobTypeEnum.INDEX_REFRESH);
    }

    public String addJob(NDataSegment newSegment, String modelId, String userName, JobTypeEnum jobTypeEnum) {
        HashSet<String> targetSegments = Sets.newHashSet();
        if (newSegment != null) {
            targetSegments.add(newSegment.getId());
        }
        return addJob(modelId, userName, targetSegments, null, jobTypeEnum);
    }

    public String addJob(String modelId, String userName, Set<String> targetSegments, Set<Long> targetLayouts,
                         JobTypeEnum jobTypeEnum) {
        if (!config.isJobNode() && !config.isUTEnv()) {
            throw new KylinException(FAILED_ADD_JOB_ABANDON, MsgPicker.getMsg().getADD_JOB_ABANDON());
        }
        checkNotNull(project);
        val jobParam = new JobParam(targetSegments, targetLayouts, userName, modelId, project, jobTypeEnum);
        ExecutableUtil.computLayouts(jobParam);
        AbstractJobHandler handler;
        switch (jobTypeEnum) {
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
                log.error("jobParam doesn't have matched job: {}", jobTypeEnum);
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
        executables.stream()
                .filter(e -> e.getTargetSegments() != null)
                .flatMap(e -> e.getTargetSegments().stream())
                .distinct()
                .filter(segId -> df.getSegment(segId) != null)
                .forEach(segId -> runningSegs.add(df.getSegment(segId)));
        return SegmentUtils.filterSegmentsByTime(df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING), runningSegs);
    }

}
