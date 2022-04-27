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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_STORAGE_QUOTA_LIMIT;

import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.handler.AbstractJobHandler;
import org.apache.kylin.job.handler.AddIndexHandler;
import org.apache.kylin.job.handler.AddSegmentHandler;
import org.apache.kylin.job.handler.MergeSegmentHandler;
import org.apache.kylin.job.handler.RefreshSegmentHandler;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.job.model.JobParam;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
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

    public String addSegmentJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.INC_BUILD);
        return addJob(jobParam);
    }

    public String addIndexJob(JobParam jobParam) {
        val relatedSegments = SegmentUtil.getValidSegments(jobParam.getModel(), project).stream()
                .map(NDataSegment::getId).collect(Collectors.toSet());
        jobParam.withTargetSegments(relatedSegments);
        return addRelatedIndexJob(jobParam);
    }

    public String addRelatedIndexJob(JobParam jobParam) {
        boolean noNeed = (jobParam.getTargetSegments() == null
                && SegmentUtil.getValidSegments(jobParam.getModel(), project).isEmpty())
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
        NDataModel model = NDataModelManager.getInstance(config, project).getDataModelDesc(jobParam.getModel());
        if (model.isMultiPartitionModel() && CollectionUtils.isNotEmpty(jobParam.getTargetPartitions())) {
            jobParam.setJobTypeEnum(JobTypeEnum.SUB_PARTITION_REFRESH);
        } else {
            jobParam.setJobTypeEnum(JobTypeEnum.INDEX_REFRESH);
        }
        return addJob(jobParam);
    }

    public String buildPartitionJob(JobParam jobParam) {
        jobParam.setJobTypeEnum(JobTypeEnum.SUB_PARTITION_BUILD);
        return addJob(jobParam);
    }

    public String addJob(JobParam jobParam) {
        return addJob(jobParam, null);
    }

    public String addJob(JobParam jobParam, AbstractJobHandler handler) {
        if (!config.isJobNode() && !config.isUTEnv()) {
            throw new KylinException(JOB_CREATE_ABANDON);
        }
        checkNotNull(project);
        checkStorageQuota(project);
        jobParam.setProject(project);
        ExecutableUtil.computeParams(jobParam);

        AbstractJobHandler localHandler = handler != null ? handler : createJobHandler(jobParam);
        if (localHandler == null)
            return null;

        localHandler.handle(jobParam);
        return jobParam.getJobId();
    }

    private AbstractJobHandler createJobHandler(JobParam jobParam) {
        AbstractJobHandler handler;
        switch (jobParam.getJobTypeEnum()) {
        case INC_BUILD:
            handler = new AddSegmentHandler();
            break;
        case INDEX_MERGE:
            handler = new MergeSegmentHandler();
            break;
        case INDEX_BUILD:
        case SUB_PARTITION_BUILD:
            handler = new AddIndexHandler();
            break;
        case INDEX_REFRESH:
        case SUB_PARTITION_REFRESH:
            handler = new RefreshSegmentHandler();
            break;
        case EXPORT_TO_SECOND_STORAGE:
            throw new UnsupportedOperationException();
        default:
            log.error("jobParam doesn't have matched job: {}", jobParam.getJobTypeEnum());
            return null;
        }
        return handler;
    }

    public JobManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
    }

    public static void checkStorageQuota(String project) {
        val scheduler = NDefaultScheduler.getInstance(project);
        if (scheduler.hasStarted() && scheduler.getContext().isReachQuotaLimit()) {
            log.error("Add job failed due to no available storage quota in project {}", project);
            throw new KylinException(JOB_STORAGE_QUOTA_LIMIT);
        }
    }
}
